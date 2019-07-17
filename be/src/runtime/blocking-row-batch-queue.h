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

#include <list>
#include <memory>

#include "runtime/blocking-row-batch-queue.h"
#include "runtime/row-batch-queue.h"
#include "util/blocking-queue.h"
#include "util/spinlock.h"

namespace impala {

class RowBatch;

/// Functor that returns the bytes in MemPool chunks for a row batch.
/// Note that we don't include attached BufferPool::BufferHandle objects because this
/// queue is only used in scan nodes that don't attach buffers.
struct RowBatchBytesFn {
  int64_t operator()(const std::unique_ptr<RowBatch>& batch) {
    return batch->tuple_data_pool()->total_reserved_bytes();
  }
};

/// Extends blocking queue for row batches. Row batches have a property that
/// they must be processed in the order they were produced, even in cancellation
/// paths. Preceding row batches can contain ptrs to memory in subsequent row batches
/// and we need to make sure those ptrs stay valid.
/// Row batches that are added after Shutdown() are queued in a separate "cleanup"
/// queue, which can be cleaned up during Close().
///
/// The queue supports limiting the capacity in terms of bytes enqueued.
///
/// All functions are thread safe.
///
/// TODO only extend RowBatchQueue (prefer composition over inheritance)
class BlockingRowBatchQueue
    : public BlockingQueue<std::unique_ptr<RowBatch>, RowBatchBytesFn>,
      public RowBatchQueue {
 public:
  /// 'max_batches' is the maximum number of row batches that can be queued.
  /// 'max_bytes' is the maximum number of bytes of row batches that can be queued (-1
  /// means no limit).
  /// When the queue is full, producers will block.
  BlockingRowBatchQueue(int max_batches, int64_t max_bytes);
  ~BlockingRowBatchQueue();

  /// TODO re-factor row_batches_put_timer_ and row_batches_get_timer_ into this method
  virtual Status Prepare(RuntimeProfile* profile) override;

  /// Adds a batch to the queue. This is blocking if the queue is full.
  virtual bool AddBatch(std::unique_ptr<RowBatch> batch) override;

  /// Gets a row batch from the queue. Returns NULL if there are no more.
  /// This function blocks.
  /// Returns NULL after Shutdown().
  virtual std::unique_ptr<RowBatch> GetBatch() override;

  /// Returns true if the underlying BlockingQueue is full, false otherwise. Does not
  /// account of the current size of the cleanup queue.
  virtual bool IsFull() const override;

  /// Returns true if the underlying BlockingQueue is full, false otherwise. Does not
  /// account for the contents of the cleanup queue.
  virtual bool IsEmpty() const override;

  /// Returns false if the queue has been shutdown, true otherwise.
  virtual bool IsOpen() const override;

  /// Deletes all row batches in cleanup_queue_. Not valid to call AddBatch()
  /// after this is called.
  /// TODO consider re-factoring Cleanup() into RowBatchQueue
  void Cleanup();

  /// Shutdowns the underlying BlockingQueue. Future calls to AddBatch will put the
  /// RowBatch on the cleanup queue. Future calls to GetBatch will continue to return
  /// RowBatches from the BlockingQueue.
  virtual void Close() override;

 private:
  /// Lock protecting cleanup_queue_
  SpinLock lock_;

  /// Queue of orphaned row batches
  std::list<std::unique_ptr<RowBatch>> cleanup_queue_;
};
}
