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

#include "exec/buffered-plan-root-sink.h"
#include "service/query-result-set.h"

#include "common/names.h"

namespace impala {

const int BufferedPlanRootSink::MAX_FETCH_SIZE;

BufferedPlanRootSink::BufferedPlanRootSink(TDataSinkId sink_id,
    const RowDescriptor* row_desc, RuntimeState* state,
    const TBackendResourceProfile& resource_profile, const TDebugOptions& debug_options)
  : PlanRootSink(sink_id, row_desc, state),
    resource_profile_(resource_profile),
    debug_options_(debug_options) {}

Status BufferedPlanRootSink::Prepare(
    RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  row_batches_send_wait_timer_ = ADD_TIMER(profile(), "RowBatchSendWaitTime");
  row_batches_get_wait_timer_ = ADD_TIMER(profile(), "RowBatchGetWaitTime");
  return Status::OK();
}

Status BufferedPlanRootSink::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));
  batch_queue_.reset(new SpillableRowBatchQueue(name_,
      state->query_options().max_unpinned_result_spooling_memory, state, mem_tracker(),
      profile(), row_desc_, resource_profile_, debug_options_));
  RETURN_IF_ERROR(batch_queue_->Open());
  return Status::OK();
}

Status BufferedPlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  // If the batch is empty, we have nothing to do so just return Status::OK().
  if (batch->num_rows() == 0) return Status::OK();

  // Close should only be called by the producer thread, no RowBatches should be sent
  // after the sink is closed.
  DCHECK(!closed_);
  DCHECK(batch_queue_->IsOpen());
  PlanRootSink::ValidateCollectionSlots(*row_desc_, batch);
  RETURN_IF_ERROR(PlanRootSink::UpdateAndCheckRowsProducedLimit(state, batch));

  {
    // Add the copied batch to the RowBatch queue and wake up the consumer thread if it is
    // waiting for rows to process.
    unique_lock<mutex> l(lock_);

    // If the queue is full, wait for the producer thread to read batches from it.
    while (!state->is_cancelled() && batch_queue_->IsFull()) {
      SCOPED_TIMER(profile()->inactive_timer());
      SCOPED_TIMER(row_batches_send_wait_timer_);
      batch_queue_has_capacity_.Wait(l);
    }
    RETURN_IF_CANCELLED(state);

    // Add the batch to the queue and then notify the consumer that rows are available.
    RETURN_IF_ERROR(batch_queue_->AddBatch(batch));
  }
  // Release the lock before calling notify so the consumer thread can immediately acquire
  // the lock.
  rows_available_.NotifyOne();
  return Status::OK();
}

Status BufferedPlanRootSink::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  DCHECK(!closed_);
  unique_lock<mutex> l(lock_);
  sender_state_ = SenderState::EOS;
  // If no batches are ever added, wake up the consumer thread so it can check the
  // SenderState and return appropriately.
  rows_available_.NotifyAll();
  // Wait until the consumer has read all rows from the batch_queue_.
  {
    SCOPED_TIMER(profile()->inactive_timer());
    consumer_eos_.Wait(l);
  }
  RETURN_IF_CANCELLED(state);
  return Status::OK();
}

void BufferedPlanRootSink::Close(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  unique_lock<mutex> l(lock_);
  // FlushFinal() won't have been called when the fragment instance encounters an error
  // before sending all rows.
  if (sender_state_ == SenderState::ROWS_PENDING) {
    sender_state_ = SenderState::CLOSED_NOT_EOS;
  }
  if (current_batch_ != nullptr) {
    current_batch_->Reset();
    current_batch_.reset();
  }
  if (batch_queue_ != nullptr) batch_queue_->Close();
  // While it should be safe to call NotifyOne() here, prefer to use NotifyAll() to
  // ensure that all sleeping threads are awoken. The call to NotifyAll() is not on the
  // fast path so any overhead from calling it should be negligible.
  rows_available_.NotifyAll();
  DataSink::Close(state);
}

void BufferedPlanRootSink::Cancel(RuntimeState* state) {
  DCHECK(state->is_cancelled());
  // Wake up all sleeping threads so they can check the cancellation state.
  // While it should be safe to call NotifyOne() here, prefer to use NotifyAll() to
  // ensure that all sleeping threads are awoken. The calls to NotifyAll() are not on the
  // fast path so any overhead from calling it should be negligible.
  rows_available_.NotifyAll();
  consumer_eos_.NotifyAll();
  batch_queue_has_capacity_.NotifyAll();
}

Status BufferedPlanRootSink::GetNext(
    RuntimeState* state, QueryResultSet* results, int num_results, bool* eos) {
  {
    unique_lock<mutex> l(lock_);
    // Cap the maximum number of results fetched by this call to GetNext so that the
    // resulting QueryResultSet does not consume excessive amounts of memory.
    num_results = min(num_results, MAX_FETCH_SIZE);

    // Track the number of rows read from the queue and the number of rows to read.
    int num_read = 0;
    // If 'num_results' <= 0 then by default fetch BATCH_SIZE rows.
    int num_to_read = num_results <= 0 ? state->batch_size() : num_results;

    // Read from the queue until all requested rows have been read, or eos is hit.
    while (!*eos && num_read < num_to_read) {
      // Wait for the queue to have rows in it.
      while (IsQueueEmpty() && sender_state_ == SenderState::ROWS_PENDING
          && !state->is_cancelled()) {
        SCOPED_TIMER(row_batches_get_wait_timer_);
        rows_available_.Wait(l);
      }

      // If the query was cancelled while the sink was waiting for rows to become
      // available, or if the query was cancelled before the current call to GetNext, set
      // eos and then return. The queue could be empty if the sink was closed while
      // waiting for rows to become available, or if the sink was closed before the
      // current call to GetNext.
      if (!state->is_cancelled() && !IsQueueEmpty()) {
        // If current_batch_ is nullptr, then read directly from the queue.
        if (current_batch_ == nullptr) {
          DCHECK_EQ(current_batch_row_, 0);
          current_batch_ =
              make_unique<RowBatch>(row_desc_, state->batch_size(), mem_tracker());
          RETURN_IF_ERROR(batch_queue_->GetBatch(current_batch_.get()));
        }

        // Set the number of rows to be fetched from 'current_batch_'. Either read all
        // remaining rows in the batch, or read up to the 'num_to_read' limit is hit.
        int num_to_fetch =
            min(current_batch_->num_rows() - current_batch_row_, num_to_read - num_read);
        // if (num_results > 0) num_to_fetch = min(num_to_fetch, num_results);
        // num_to_fetch = min(num_to_fetch, num_to_read - num_read);

        // Read rows from 'current_batch_' and add them to 'results'.
        RETURN_IF_ERROR(results->AddRows(
            output_expr_evals_, current_batch_.get(), current_batch_row_, num_to_fetch));
        num_read += num_to_fetch;
        current_batch_row_ += num_to_fetch;

        // If all rows have been read from 'current_batch_' then reset the batch and its
        // index.
        DCHECK_LE(current_batch_row_, current_batch_->num_rows());
        if (current_batch_row_ == current_batch_->num_rows()) {
          current_batch_row_ = 0;
          current_batch_->Reset();
          current_batch_.reset();
        }

        // Prevent expr result allocations from accumulating.
        expr_results_pool_->Clear();
      }
      *eos = IsQueueEmpty() && sender_state_ == SenderState::EOS;
    }
    if (*eos) consumer_eos_.NotifyOne();
  }
  // Release the lock before calling notify so the consumer thread can immediately
  // acquire the lock. It is safe to call notify batch_queue_has_capacity_ regardless of
  // whether a RowBatch is read. Either (1) a RowBatch is read and the queue is no longer
  // full, so notify the consumer thread or (2) a Rowbatch was not read, which means
  // either FlushFinal was called or the query was cancelled. If FlushFinal was called
  // then the consumer thread has completed. If the query is cancelled, then we wake up
  // the consumer thread so it can check the cancellation status and return. Releasing
  // the lock is safe because the consumer always loops until the queue actually has
  // space.
  batch_queue_has_capacity_.NotifyOne();
  return state->GetQueryStatus();
}
} // namespace impala
