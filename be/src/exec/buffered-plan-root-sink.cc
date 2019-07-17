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

BufferedPlanRootSink::BufferedPlanRootSink(TDataSinkId sink_id,
    const RowDescriptor* row_desc, RuntimeState* state, RowBatchQueue* batch_queue)
  : PlanRootSink(sink_id, row_desc, state), batch_queue_(batch_queue) {}

Status BufferedPlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  // Close should only be called by the producer thread, no RowBatches should be sent
  // after the sink is closed.
  DCHECK(!closed_);
  RETURN_IF_ERROR(PlanRootSink::Send(state, batch));
  // Make a copy of the given RowBatch and place it on the queue.
  unique_ptr<RowBatch> output_batch =
      make_unique<RowBatch>(batch->row_desc(), batch->capacity(), mem_tracker());
  output_batch->AcquireState(batch);

  // Add the copied batch to the RowBatch queue and wake up the consumer thread if it is
  // waiting for rows to process.
  unique_lock<mutex> l(lock_);
  DCHECK(batch_queue_->IsOpen());
  if (!batch_queue_->AddBatch(move(output_batch))) {
    is_full_.Wait(l);
    RETURN_IF_CANCELLED(state);
  }
  rows_available_.NotifyAll();
  return Status::OK();
}

Status BufferedPlanRootSink::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  DCHECK(!closed_);
  unique_lock<mutex> l(lock_);
  sender_state_ = SenderState::EOS;
  rows_available_.NotifyAll();
  // Wait until the consumer has read all rows from the batch_queue_.
  consumer_eos_.Wait(l);
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
  batch_queue_->Close();
  rows_available_.NotifyAll();
  DataSink::Close(state);
}

void BufferedPlanRootSink::Cancel(RuntimeState* state) {
  DCHECK(state->is_cancelled());
  rows_available_.NotifyAll();
  consumer_eos_.NotifyAll();
  is_full_.NotifyAll();
}

Status BufferedPlanRootSink::GetNext(
    RuntimeState* state, QueryResultSet* results, int num_results, bool* eos) {
  unique_lock<mutex> l(lock_);
  while (batch_queue_->IsEmpty() && sender_state_ == SenderState::ROWS_PENDING
      && !state->is_cancelled()) {
    rows_available_.Wait(l);
  }

  // If the query was cancelled while the sink was waiting for rows to become available,
  // or if the query was cancelled before the current call to GetNext, set eos and then
  // return. The queue could be empty if the sink was closed while waiting for rows to
  // become available, or if the sink was closed before the current call to GetNext.
  if (!state->is_cancelled() && !batch_queue_->IsEmpty()) {
    unique_ptr<RowBatch> batch = batch_queue_->GetBatch();
    is_full_.NotifyAll();
    // For now, if num_results < batch->num_rows(), we terminate returning results early.
    if (num_results > 0 && num_results < batch->num_rows()) {
      *eos = true;
      return state->GetQueryStatus();
    }
    RETURN_IF_ERROR(
        results->AddRows(output_expr_evals_, batch.get(), 0, batch->num_rows()));
    batch->Reset();
  }
  *eos = batch_queue_->IsEmpty() && sender_state_ == SenderState::EOS;
  if (*eos) consumer_eos_.NotifyAll();
  return state->GetQueryStatus();
}
}
