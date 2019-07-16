// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/plan-root-sink.h"

#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "service/query-result-set.h"
#include "util/pretty-printer.h"

#include <memory>
#include <boost/thread/mutex.hpp>

using namespace std;
using boost::unique_lock;
using boost::mutex;

namespace impala {

PlanRootSink::PlanRootSink(
    TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state,
    const RowDescriptor* output_row_desc)
  : DataSink(sink_id, row_desc, "PLAN_ROOT_SINK", state),
    num_rows_produced_limit_(state->query_options().num_rows_produced_limit),
    output_row_desc_(output_row_desc) {}

namespace {

/// Validates that all collection-typed slots in the given batch are set to NULL.
/// See SubplanNode for details on when collection-typed slots are set to NULL.
/// TODO: This validation will become obsolete when we can return collection values.
/// We will then need a different mechanism to assert the correct behavior of the
/// SubplanNode with respect to setting collection-slots to NULL.
void ValidateCollectionSlots(const RowDescriptor& row_desc, RowBatch* batch) {
#ifndef NDEBUG
  if (!row_desc.HasVarlenSlots()) return;
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    for (int j = 0; j < row_desc.tuple_descriptors().size(); ++j) {
      const TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[j];
      if (tuple_desc->collection_slots().empty()) continue;
      for (int k = 0; k < tuple_desc->collection_slots().size(); ++k) {
        const SlotDescriptor* slot_desc = tuple_desc->collection_slots()[k];
        int tuple_idx = row_desc.GetTupleIdx(slot_desc->parent()->id());
        const Tuple* tuple = row->GetTuple(tuple_idx);
        if (tuple == NULL) continue;
        DCHECK(tuple->IsNull(slot_desc->null_indicator_offset()));
      }
    }
  }
#endif
}
} // namespace

Status PlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  // If the batch is empty, we have nothing to do so just return Status::OK()
  if (batch->num_rows() == 0) {
    return Status::OK();
  }
  ValidateCollectionSlots(*row_desc_, batch);

  // Check to ensure that the number of rows produced by query execution does not exceed
  // rows_returned_limit_.
  if (num_rows_produced_limit_ > 0 && num_rows_produced_ + batch->num_rows() > num_rows_produced_limit_) {
    Status err = Status::Expected(TErrorCode::ROWS_PRODUCED_LIMIT_EXCEEDED,
        PrintId(state->query_id()),
        PrettyPrinter::Print(num_rows_produced_limit_, TUnit::NONE));
    VLOG_QUERY << err.msg().msg();
    return err;
  }

  // TODO consider having two locks - one for the BTS and one for the memory resources; this should allow the Producer
  // thread to materialize row batches and the Consumer thread to read from the BTS, in parallel
  // Must acquire the lock before using any memory resources in case the query is cancelled the Close is called
  boost::unique_lock<boost::mutex> l(lock_);
  // In case Close was called before the lock was acquired, returned
  RETURN_IF_CANCELLED(state);
  if (is_prs_closed_) {
    return Status::CANCELLED;
  }

  // The output_batch we write materialized expressions to
  std::unique_ptr<RowBatch> output_batch = std::make_unique<RowBatch>(output_row_desc_, state->batch_size(), mem_tracker()); // TODO I think there is a bug here, the query state mem_tracker might be closed before the query is cancelled?
  // or maybe there is something else with cancel going on that is weird
  // TODO this could happen if you releases resources first and then call cancel, which is maybe more likely
  // TODO could be a race condition where the cancel thread starts to cancel the query, the coordinator thread is alerted
  // and then calls ReleaseResources, and then the cancel thread marks the runtime_state as cancelled
  // so the check above should solve the issue for now, but needs to be cleaned up

  // Iterate over each TupleRow in the RowBatch
  FOREACH_ROW(batch, 0, batch_itr) {
    TupleRow *row = batch_itr.Get();
    TupleRow* dst_row = output_batch->GetRow(output_batch->AddRow());

    // Iterate over each Tuple in the TupleRow, create a new Tuple and call MaterializeExprs on it
    for (int i = 0; i < output_row_desc_->tuple_descriptors().size(); i++) {
      auto *tup_desc = output_row_desc_->tuple_descriptors()[i];
      Tuple *insert_tuple = nullptr;
      insert_tuple = Tuple::Create(tup_desc->byte_size(), output_batch->tuple_data_pool());
      insert_tuple->MaterializeExprs<false, false>(row, *tup_desc, output_expr_evals_, output_batch->tuple_data_pool());
      dst_row->SetTuple(i, insert_tuple);
    }
    output_batch->CommitLastRow();
  }
  num_rows_produced_ += output_batch->num_rows();
  while (query_results_.size() >= max_row_batches) {
    sender_cv_.Wait(l);
    RETURN_IF_CANCELLED(state);
    if (is_prs_closed_) {
      return Status::CANCELLED;
    }
  }
  query_results_.emplace_back(move(output_batch));
  rows_available_.NotifyOne();
  expr_results_pool_->Clear(); // Necessary to clear any intermediate allocations made in MaterializeExprs
  return Status::OK();
}

Status PlanRootSink::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  boost::unique_lock<boost::mutex> l(lock_);
  sender_state_ = SenderState::EOS;
  rows_available_.NotifyOne();
  return Status::OK();
}

void PlanRootSink::Close(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  boost::unique_lock<boost::mutex> l(lock_);
  // FlushFinal() won't have been called when the fragment instance encounters an error
  // before sending all rows.
  if (sender_state_ == SenderState::ROWS_PENDING) {
    sender_state_ = SenderState::CLOSED_NOT_EOS;
  }
}

void PlanRootSink::Cancel(RuntimeState* state) {
  DCHECK(state->is_cancelled());
  rows_available_.NotifyOne();
  sender_cv_.NotifyAll();
  VLOG_QUERY << "Calling cancel on PRS " << this << " stack " << GetStackTrace();
  // TODO Need to wake up any sleeping thread and probably set the SenderState? could we release ReceiverResources here as well?
}

Status PlanRootSink::GetNext(
    RuntimeState* state, QueryResultSet* results, int num_results, bool* eos) {
  boost::unique_lock<boost::mutex> l(lock_);
  // Edge Cases:
  // Rows are never sent
  // TODO Not enough rows are sent to fulfill the GetNext call - should still return as many rows as available

  // Call Patterns:
  // Send is called, GetNext is called, Send is called, FlushFinal is called, GetNext is called
  // Send is called, GetNext is called, Send is called, GetNext is called, FlushFinal is called, GetNext is called
  // Send is called, Send is called, FlushFinal is called, GetNext is called, GetNext is called
  // Send is called, Send is called, GetNext is called, FlushFinal is called, GetNext is called
  // Send is called, Send is called, GetNext is called, GetNext is called, FlushFinal is called
  // GetNext is called, Send is called, Send is called, FlushFinal is called, GetNext is called
  // GetNext is called, Send is called, Send is called, GetNext is called, FlushFinal is called
  // GetNext is called, Send is called, GetNext is called, Send is called, FlushFinal is called

  // Wait until rows are available for consumption unless the SenderState is set to EOS, in which case this method
  // should not wait because no more rows will be produced
  if (num_rows_produced_ <= num_rows_read_ && sender_state_ != SenderState::EOS && sender_state_ != SenderState::CLOSED_NOT_EOS) {
    rows_available_.Wait(l);
    // Check if you are cancelled or closed?
    RETURN_IF_CANCELLED(state);
    if (is_prs_closed_) {
      return Status::CANCELLED;
    }
  }

  // rows_available_ might have been woken up by FlushFinal, so double check if there are any more rows to read
  if (!state->is_cancelled() && sender_state_ != SenderState::CLOSED_NOT_EOS && num_rows_produced_ > num_rows_read_) {
    if (intermediate_read_batch_) {
      if (intermediate_read_batch_index_ + num_results >= intermediate_read_batch_->num_rows()) {
        // Read until the end of the batch, for now, we return the rest of the batch rather than reading into the next batch
        RETURN_IF_ERROR(results->AddRows(output_expr_evals_, intermediate_read_batch_.get(), intermediate_read_batch_index_, intermediate_read_batch_->num_rows() - intermediate_read_batch_index_));
        num_rows_read_ += num_results;
        intermediate_read_batch_index_ = 0;
        intermediate_read_batch_->Reset();
        intermediate_read_batch_.reset();
      } else {
        RETURN_IF_ERROR(results->AddRows(output_expr_evals_, intermediate_read_batch_.get(), intermediate_read_batch_index_, num_results));
        intermediate_read_batch_index_ += num_results;
        num_rows_read_ += num_results;
      }
    } else {
      std::unique_ptr<RowBatch> batch = move(query_results_.front());
      query_results_.pop_front();
      sender_cv_.NotifyAll();
      // If num_results is 0 or a negative value, then return all rows in the RowBatch
      if (num_results > 0 && batch->num_rows() > num_results) {
        intermediate_read_batch_ = std::move(batch);
        intermediate_read_batch_index_ = num_results;

        RETURN_IF_ERROR(results->AddRows(output_expr_evals_, intermediate_read_batch_.get(), 0, num_results));
        num_rows_read_ += num_results;
      } else {
        RETURN_IF_ERROR(results->AddRows(output_expr_evals_, batch.get(), 0, batch->num_rows()));
        num_rows_read_ += batch->num_rows();
        batch->Reset();
        batch.reset();
      }
    }
  }
  *eos = sender_state_ == SenderState::EOS && num_rows_produced_ == num_rows_read_;
  return state->GetQueryStatus();
}

void PlanRootSink::ReleaseReceiverResources(RuntimeState* state) {
  // TODO for now we just acquire the lock here as well to avoid race conditions when a Cancel call invokes this method
  // while the FragmentInstanceState is calling Send
  boost::unique_lock<boost::mutex> l(lock_);
  if (is_prs_closed_) return; // TODO needed to guard against a race condition where the cancellation thread and the client thread both end up calling this method
  if (intermediate_read_batch_ != nullptr) {
    intermediate_read_batch_->Reset();
    intermediate_read_batch_.reset();
  }
  query_results_.clear();
  DataSink::Close(state);
  is_prs_closed_ = true;
}
}
