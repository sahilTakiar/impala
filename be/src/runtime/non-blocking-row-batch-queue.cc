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

#include "runtime/non-blocking-row-batch-queue.h"
#include "runtime/row-batch.h"

#include "common/names.h"

namespace impala {

NonBlockingRowBatchQueue::NonBlockingRowBatchQueue(int max_batches)
  : max_batches_(max_batches) {}

NonBlockingRowBatchQueue::~NonBlockingRowBatchQueue() {
  DCHECK(closed_);
}

Status NonBlockingRowBatchQueue::Prepare(RuntimeProfile* profile) {
  return Status::OK();
}

bool NonBlockingRowBatchQueue::AddBatch(unique_ptr<RowBatch> batch) {
  if (closed_ || IsFull()) return false;
  batch_queue_.push(move(batch));
  return true;
}

unique_ptr<RowBatch> NonBlockingRowBatchQueue::GetBatch() {
  if (closed_) return unique_ptr<RowBatch>();
  unique_ptr<RowBatch> result = move(batch_queue_.front());
  batch_queue_.pop();
  return result;
}

bool NonBlockingRowBatchQueue::IsFull() const {
  return batch_queue_.size() == max_batches_;
}

bool NonBlockingRowBatchQueue::IsEmpty() const {
  return batch_queue_.empty();
}

bool NonBlockingRowBatchQueue::IsOpen() const {
  return !closed_;
}

void NonBlockingRowBatchQueue::Close() {
  if (closed_) return;
  while (!batch_queue_.empty()) {
    unique_ptr<RowBatch> result = GetBatch();
    result->Reset();
  }
  queue<unique_ptr<RowBatch>> empty;
  swap(batch_queue_, empty);
  closed_ = true;
}
}
