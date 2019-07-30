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

#include "runtime/buffered-tuple-stream.h"
#include "runtime/reservation-manager.h"
#include "runtime/row-batch.h"

namespace impala {

class RowBatch;

/// A RowBatchQueue that provides non-blocking queue semantics. RowBatches are stored
/// inside a std::deque. None of the methods block, this class is not thread safe. The
/// size of the queue can be capped by the 'max_batches' parameter. Calls to AddBatch
/// after the capacity has been reached will return false. Calls to GetBatch on an empty
/// queue will return null.
class SpillableRowBatchQueue {
 public:
  SpillableRowBatchQueue(const std::string &name, int64_t max_unpinned_bytes,
      RuntimeState* state, MemTracker* mem_tracker, RuntimeProfile* profile,
      const RowDescriptor* row_desc, const TBackendResourceProfile& resource_profile,
      const TDebugOptions& debug_options);
  ~SpillableRowBatchQueue();

  Status Open();

  /// Adds the given RowBatch to the queue. Returns true if the batch was successfully
  /// added, returns false if the queue is full or has already been closed. The ownership
  /// of the given batch is transferred from the 'batch' to the queue.
  Status AddBatch(RowBatch* batch);

  /// Returns and removes the RowBatch at the head of the queue. Returns a nullptr if the
  /// queue is already closed or the queue is empty. The ownership of the returned batch
  /// is transferred from the queue to the returned unique_ptr.
  Status GetBatch(RowBatch* batch);

  /// Returns true if the queue limit has been reached, false otherwise.
  bool IsFull() const;

  /// Returns true if the queue is empty, false otherwise.
  bool IsEmpty() const;

  /// Returns false if Close() has been called, true otherwise.
  bool IsOpen() const;

  /// Resets the remaining RowBatches in the queue and releases the queue memory.
  void Close();

 private:
  const std::string &name_;

  /// Queue of row batches.
  BufferedTupleStream* batch_queue_;

  ReservationManager reservation_manager_;

  RuntimeState* state_;

  MemTracker* mem_tracker_;

  RuntimeProfile* profile_;

  const RowDescriptor* row_desc_;

  const TBackendResourceProfile& resource_profile_;

  const TDebugOptions& debug_options_;

  const int64_t max_unpinned_bytes_;

  /// True if the queue has been closed, false otherwise.
  bool closed_ = false;
};
}
