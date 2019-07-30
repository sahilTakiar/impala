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

#include "exec/plan-root-sink.h"
#include "runtime/spillable-row-batch-queue.h"
#include "util/condition-variable.h"

namespace impala {

/// PlanRootSink that buffers RowBatches from the 'sender' (fragment) thread. RowBatches
/// are buffered in memory until a max number of RowBatches are queued. Any subsequent
/// calls to Send will block until the 'consumer' (coordinator) thread has read enough
/// RowBatches to free up sufficient space in the queue. The blocking behavior follows
/// the same semantics as BlockingPlanRootSink.
///
/// FlushFinal() blocks until the consumer has read all RowBatches from the queue or
/// until the sink is eiher closed or cancelled. This ensures that the coordinator
/// fragment stays alive until the client fetches all results, but allows all other
/// fragments to complete and release their resources.
///
/// The sink assumes a non-thread safe RowBatchQueue is injected and uses a single lock to
/// synchronize access to the queue.
class BufferedPlanRootSink : public PlanRootSink {
 public:
  BufferedPlanRootSink(TDataSinkId sink_id, const RowDescriptor* row_desc,
      RuntimeState* state, const TBackendResourceProfile& resource_profile,
      const TDebugOptions& debug_options);

  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  virtual Status Open(RuntimeState* state) override;

  /// Creates a copy of the given RowBatch and adds it to the queue. The copy is
  /// necessary as the ownership of 'batch' remains with the sender.
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Notifies the consumer of producer eos and blocks until the consumer has read all
  /// batches from the queue, or until the sink is either closed or cancelled.
  virtual Status FlushFinal(RuntimeState* state) override;

  /// Release resources and unblocks consumer.
  virtual void Close(RuntimeState* state) override;

  /// Blocks until rows are available for consumption
  virtual Status GetNext(
      RuntimeState* state, QueryResultSet* result_set, int num_rows, bool* eos) override;

  /// Notifies both consumer and producer threads so they can check the cancellation
  /// status.
  virtual void Cancel(RuntimeState* state) override;

 private:
  /// Protects the RowBatchQueue and all ConditionVariables.
  boost::mutex lock_;

  /// Waited on by the consumer inside GetNext() until rows are available for consumption.
  /// Signalled when the producer adds a RowBatch to the queue. Also signalled by
  /// FlushFinal(), Close() and Cancel() to unblock the sender.
  ConditionVariable rows_available_;

  /// Waited on by the producer inside FlushFinal() until the consumer has hit eos.
  /// Signalled when the consumer reads all RowBatches from the queue. Also signalled in
  /// Cancel() to unblock the producer.
  ConditionVariable consumer_eos_;

  /// Waited on by the producer inside Send() if the RowBatchQueue is full. Signalled
  /// when the consumer reads a batch from the RowBatchQueue. Also signalled in Cancel()
  /// to unblock the producer.
  ConditionVariable is_full_;

  /// A SpillableRowBatchQueue that buffers RowBatches from the sender for consumption by
  /// the consumer. The queue is not thread safe and access is protected by 'lock_'.
  std::unique_ptr<SpillableRowBatchQueue> batch_queue_;

  const TBackendResourceProfile& resource_profile_;

  const TDebugOptions& debug_options_;

  RuntimeProfile::Counter* row_batches_send_wait_timer_ = nullptr;
  RuntimeProfile::Counter* row_batches_get_wait_timer_ = nullptr;
};
}
