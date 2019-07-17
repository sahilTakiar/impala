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

#pragma once

#include <queue>

#include "runtime/row-batch-queue.h"

namespace impala {

class RowBatch;

/// A RowBatchQueue that stores batches in a std::queue. None of the methods block, this
/// class is not thread safe. Most methods directly correspond to a method in std::queue.
/// The size of the queue can be capped by the 'max_batches' parameter. Calls to AddBatch
/// after the capacity has been reached will return false.
class NonBlockingRowBatchQueue : public RowBatchQueue {
 public:
  NonBlockingRowBatchQueue(int max_batches);
  ~NonBlockingRowBatchQueue();

  /// TODO consider adding metrics to the RuntimeProfile about queue usage.
  virtual Status Prepare(RuntimeProfile* profile) override;

  /// Adds the given RowBatch to the queue.
  virtual bool AddBatch(std::unique_ptr<RowBatch> batch) override;

  /// Returns and removes the RowBatch at the head of the queue.
  virtual std::unique_ptr<RowBatch> GetBatch() override;

  /// Returns true if the queue limit has been reached, false otherwise.
  virtual bool IsFull() const override;

  /// Returns true if the queue is empty, false otherwise.
  virtual bool IsEmpty() const override;

  /// Returns false if Close() has been called, true otherwise.
  virtual bool IsOpen() const override;

  /// Resets and remanining RowBatches in the queue and releases the queue memory.
  virtual void Close() override;

 private:
  /// Queue of row batches.
  std::queue<std::unique_ptr<RowBatch>> batch_queue_;

  /// True if the queue has been closed, false otherwise.
  bool closed_ = false;

  /// The max size of the queue.
  int max_batches_;
};
}
