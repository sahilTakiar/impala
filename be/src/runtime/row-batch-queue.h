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

#include "runtime/row-batch.h"

namespace impala {

class RowBatch;

/// An interface for buffering RowBatches in a queue. This interface makes no
/// thread-safety guarantees and different implementations might have different
/// thread-safety semantics. The class references RowBatches using unique pointers in
/// order to enforce precise object ownership of RowBatches.
///
/// Prepare must be the first method called on a RowBatchQueue. Once a queue is closed,
/// GetBatch will return nullptr, and AddBatch will return false. IsEmpty must return true
/// on a closed queue.
class RowBatchQueue {
 public:
  virtual ~RowBatchQueue() {}

  /// Prepares the queue so that batches can be added.
  virtual Status Prepare(RuntimeProfile* profile) = 0;

  /// Adds the given RowBatch to the queue. Returns true if the batch was successfully
  /// added, returns false if the queue is full.
  virtual bool AddBatch(std::unique_ptr<RowBatch> batch) = 0;

  /// Gets the RowBatch from the head of the queue. Returns null if a batch could not
  /// successfully be retrieved.
  virtual std::unique_ptr<RowBatch> GetBatch() = 0;

  // Returns true if the queue is empty, false otherwise.
  virtual bool IsEmpty() const = 0;

  /// Returns true if the queue is full, false otherwise.
  virtual bool IsFull() const = 0;

  /// Returns false if the queue has been closed, true otherwise.
  virtual bool IsOpen() const = 0;

  /// Closes the queue and releases any resources held by the queue.
  virtual void Close() = 0;
};
}
