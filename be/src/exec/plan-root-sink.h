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

#ifndef IMPALA_EXEC_PLAN_ROOT_SINK_H
#define IMPALA_EXEC_PLAN_ROOT_SINK_H

#include "exec/data-sink.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/reservation-manager.h"
#include "util/condition-variable.h"

namespace impala {

class TupleRow;
class RowBatch;
class QueryResultSet;
class ScalarExprEvaluator;

/// Sink which manages the handoff between a 'sender' (a fragment instance) that produces
/// batches by calling Send(), and a 'consumer' (e.g. the coordinator) which consumes rows
/// formed by computing a set of output expressions over the input batches, by calling
/// GetNext(). Send() and GetNext() are called concurrently.
///
/// The consumer calls GetNext() with a QueryResultSet and a requested fetch
/// size. GetNext() shares these fields with Send(), and then signals Send() to begin
/// populating the result set. GetNext() returns when a) the sender has sent all of its
/// rows b) the requested fetch size has been satisfied or c) the sender fragment
/// instance was cancelled.
///
/// The sender uses Send() to fill in as many rows as are requested from the current
/// batch. When the batch is exhausted - which may take several calls to GetNext() -
/// Send() returns so that the fragment instance can produce another row batch.
///
/// FlushFinal() should be called by the sender to signal it has finished calling
/// Send() for all rows. Close() should be called by the sender to release resources.
///
/// When the fragment instance is cancelled, Cancel() is called to unblock both the
/// sender and consumer. Cancel() may be called concurrently with Send(), GetNext() and
/// Close().
///
/// The sink is thread safe up to a single sender and single consumer.
///
/// Lifetime: The sink is owned by the QueryState and has the same lifetime as
/// QueryState. The QueryState references from the fragment instance and the Coordinator
/// ensures that this outlives any calls to Send() and GetNext(), respectively.
///
/// TODO: The consumer drives the sender in lock-step with GetNext() calls, forcing a
/// context-switch on every invocation. Measure the impact of this, and consider moving to
/// a fully asynchronous implementation with a queue to manage buffering between sender
/// and consumer. See IMPALA-4268.
class PlanRootSink : public DataSink {
 public:
  PlanRootSink(TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state,
          const TBackendResourceProfile& resource_profile, const TDebugOptions& debug_options, QueryState* query_state,
          const RowDescriptor* output_row_desc);

  virtual Status Open(RuntimeState* state);

  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Sends a new batch. Ownership of 'batch' remains with the sender. Blocks until the
  /// consumer has consumed 'batch' by calling GetNext().
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// Indicates eos and notifies consumer.
  virtual Status FlushFinal(RuntimeState* state);

  virtual void ReleaseReceiverResources(RuntimeState* state);

  /// To be called by sender only. Release resources and unblocks consumer.
  virtual void Close(RuntimeState* state);

  /// To be called by the consumer only. 'result_set' with up to 'num_rows' rows
  /// produced by the fragment instance that calls Send(). *eos is set to 'true' when
  /// there are no more rows to consume. If Cancel() or Close() are called concurrently,
  /// GetNext() will return and may not populate 'result_set'. All subsequent calls
  /// after Cancel() or Close() are no-ops.
  Status GetNext(
      RuntimeState* state, QueryResultSet* result_set, int num_rows, bool* eos);

  /// Unblocks both the consumer and sender so they can check the cancellation flag in
  /// the RuntimeState. The cancellation flag should be set prior to calling this.
  void Cancel(RuntimeState* state);

  static const std::string NAME;

 private:
  /// Protects all members, including the condition variables.
  boost::mutex lock_;

  std::unique_ptr<BufferedTupleStream> query_results_;

  /// State of the sender:
  /// - ROWS_PENDING: the sender is still producing rows; the only non-terminal state
  /// - EOS: the sender has passed all rows to Send()
  /// - CLOSED_NOT_EOS: the sender (i.e. sink) was closed before all rows were passed to
  ///   Send()
  enum class SenderState { ROWS_PENDING, EOS, CLOSED_NOT_EOS };
  SenderState sender_state_ = SenderState::ROWS_PENDING;

  /// Set by GetNext() to indicate to Send() how many rows it should write to results_.
  int num_rows_read_ = 0;

  /// Updated by Send() to indicate the total number of rows produced by query execution.
  int64_t num_rows_produced_ = 0;

  /// Limit on the number of rows produced by this query, initialized by the constructor.
  const int64_t num_rows_produced_limit_;

  const TBackendResourceProfile& resource_profile_;

  ReservationManager reservation_manager_;

  const TDebugOptions& debug_options_;

  QueryState* query_state_;

  const RowDescriptor* output_row_desc_;

  std::unique_ptr<RowBatch> intermediate_read_batch_ = nullptr;

  int intermediate_read_batch_index_ = 0;

  ConditionVariable rows_available_;

  bool is_prs_closed_ = false;
};
}

#endif
