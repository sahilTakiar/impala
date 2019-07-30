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

#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/spillable-row-batch-queue.h"
#include "runtime/query-state.h"

#include "common/names.h"

namespace impala {

SpillableRowBatchQueue::SpillableRowBatchQueue(const string &name,
    int64_t max_unpinned_bytes, RuntimeState* state, MemTracker* mem_tracker,
    RuntimeProfile* profile, const RowDescriptor* row_desc,
    const TBackendResourceProfile& resource_profile, const TDebugOptions& debug_options)
  : name_(name),
    state_(state),
    mem_tracker_(mem_tracker),
    profile_(profile),
    row_desc_(row_desc),
    resource_profile_(resource_profile),
    debug_options_(debug_options),
    max_unpinned_bytes_(max_unpinned_bytes) {}

SpillableRowBatchQueue::~SpillableRowBatchQueue() {
  DCHECK(closed_);
}

Status SpillableRowBatchQueue::Open() {
  reservation_manager_.Init(name_, profile_, state_->instance_buffer_reservation(),
      mem_tracker_, resource_profile_, debug_options_);

  RETURN_IF_ERROR(reservation_manager_.ClaimBufferReservation(state_));

  batch_queue_ = new BufferedTupleStream(state_, row_desc_, reservation_manager_.buffer_pool_client(),
          resource_profile_.spillable_buffer_size, resource_profile_.max_row_buffer_size);
  RETURN_IF_ERROR(batch_queue_->Init(name_, true));
  bool got_reservation = false;
  RETURN_IF_ERROR(batch_queue_->PrepareForReadWrite(true, &got_reservation));
  DCHECK(got_reservation) << "Failed to get reservation using buffer poof client: "
                          << reservation_manager_.buffer_pool_client()->DebugString();
  return Status::OK();
}

Status SpillableRowBatchQueue::AddBatch(RowBatch* batch) {
  Status status;
  FOREACH_ROW(batch, 0, batch_itr) {
    if (UNLIKELY(!batch_queue_->AddRow(batch_itr.Get(), &status))) {
      RETURN_IF_ERROR(status);
      RETURN_IF_ERROR(state_->StartSpilling(mem_tracker_));
      DCHECK_EQ(batch_queue_->BytesUnpinned(), 0);
      batch_queue_->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
      if (!batch_queue_->AddRow(batch_itr.Get(), &status)) {
        RETURN_IF_ERROR(status);
        DCHECK(false) << "Rows should be added in unpinned mode unless an error occured";
      }
    }
  }
  return Status::OK();
}

Status SpillableRowBatchQueue::GetBatch(RowBatch* batch) {
  bool eos = false;
  RETURN_IF_ERROR(batch_queue_->GetNext(batch, &eos));
  DCHECK((eos && IsEmpty()) || (!eos && !IsEmpty()));
  return Status::OK();
}

bool SpillableRowBatchQueue::IsFull() const {
  return batch_queue_->BytesUnpinned() >= max_unpinned_bytes_;
}

bool SpillableRowBatchQueue::IsEmpty() const {
  return batch_queue_->num_rows() == batch_queue_->rows_returned();
}

bool SpillableRowBatchQueue::IsOpen() const {
  return !closed_;
}

void SpillableRowBatchQueue::Close() {
  if (closed_) return;
  if (batch_queue_ != nullptr) batch_queue_->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  reservation_manager_.Close(state_);
  closed_ = true;
}
}
