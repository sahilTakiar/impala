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

#include "runtime/query-driver.h"
#include "runtime/exec-env.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "service/retry-work.h"
#include "util/debug-util.h"

#include "common/names.h"

DEFINE_int32(retry_thread_pool_size, 5,
    "(Advanced) Size of the thread-pool processing query retries");

namespace impala {

const uint32_t MAX_RETRY_QUEUE_SIZE = 65536;

QueryDriver::QueryDriver(ExecEnv* exec_env, ImpalaServer* parent_server,
    shared_ptr<ClientRequestState> client_request_state)
  : exec_env_(exec_env),
    parent_server_(parent_server),
    client_request_state_(client_request_state) {
  retry_thread_pool_.reset(new ThreadPool<RetryWork>("query-driver", "retry-worker",
      FLAGS_retry_thread_pool_size, MAX_RETRY_QUEUE_SIZE,
      bind<void>(&QueryDriver::RetryQueryFromThreadPool, this, _1, _2)));
  ABORT_IF_ERROR(retry_thread_pool_->Init());
}

shared_ptr<ClientRequestState> QueryDriver::GetClientRequestState() {
  lock_guard<SpinLock> l(client_request_state_lock_);
  if (retried_client_request_state_ != nullptr) return retried_client_request_state_;
  DCHECK(client_request_state_ != nullptr);
  return client_request_state_;
}

shared_ptr<ClientRequestState> QueryDriver::GetClientRequestState(
    const TUniqueId& query_id) {
  lock_guard<SpinLock> l(client_request_state_lock_);
  if (retried_client_request_state_ != nullptr
      && retried_client_request_state_->query_id() == query_id) {
    return retried_client_request_state_;
  }
  DCHECK(client_request_state_ != nullptr);
  DCHECK(client_request_state_->query_id() == query_id);
  return client_request_state_;
}

void QueryDriver::RetryAsync(const TUniqueId& query_id, const Status& error) {
  retry_thread_pool_->Offer(RetryWork(query_id, error));
}

void QueryDriver::RetryQueryFromThreadPool(
    uint32_t thread_id, const RetryWork& retry_work) {
  // TODO add some jitter here to simulate delays in threadpool execution
  const TUniqueId& query_id = retry_work.query_id();
  string retry_failed_msg = Substitute("Failed to retry query $0", PrintId(query_id));
  VLOG_QUERY << Substitute("Retrying query $0 with error message $1", PrintId(query_id),
      retry_work.error().GetDetail());

  // There should be no retried client request state.
  shared_ptr<ClientRequestState>* request_state;
  {
    lock_guard<SpinLock> l(client_request_state_lock_);
    DCHECK(retried_client_request_state_ == nullptr);
    DCHECK(client_request_state_ != nullptr);
    (*request_state) = client_request_state_;
  }
  DCHECK((*request_state)->retry_state() == ClientRequestState::RetryState::RETRYING)
      << Substitute("query=$0 unexpected state $1", PrintId((*request_state)->query_id()),
          (*request_state)->ExecStateToString((*request_state)->exec_state()));

  // Cancel the query.
  Status status = (*request_state)->Cancel(true, nullptr);
  if (!status.ok()) {
    status.AddDetail(retry_failed_msg + " cancellation failed");
    discard_result((*request_state)->UpdateQueryStatus(status));
    return;
  }

  // Copy the TExecRequest from the original query and reset it.
  unique_ptr<TExecRequest> retry_exec_request =
      make_unique<TExecRequest>((*request_state)->exec_request());
  const TQueryCtx& query_ctx = retry_exec_request->query_exec_request.query_ctx;
  parent_server_->PrepareQueryContext(&retry_exec_request->query_exec_request.query_ctx);

  // Copy the TUniqueId query_id from the original query.
  unique_ptr<TUniqueId> original_query_id =
      make_unique<TUniqueId>((*request_state)->query_id());

  // Create the ClientRequestState for the new query.
  shared_ptr<ClientRequestState> retry_request_state;
  retry_request_state.reset(new ClientRequestState(query_ctx, exec_env_,
      exec_env_->frontend(), parent_server_, (*request_state)->session(),
      move(retry_exec_request), (*request_state)->parent_driver()));
  retry_request_state->set_retried_id(move(original_query_id));
  retry_request_state->set_user_profile_access(
      retry_request_state->exec_request().user_has_profile_access);
  if (retry_request_state->exec_request().__isset.result_set_metadata) {
    retry_request_state->set_result_metadata(
        retry_request_state->exec_request().result_set_metadata);
  }

  // The steps below mimic what is done when a query is first launched. See
  // ImpalaServer::ExecuteStatement.

  // Register the new query.
  parent_server_->MarkSessionActive((*request_state)->session());

  // '(*request_state)->parent_driver()' is equivalent to 'this', but in order to avoid
  // creating a new shared_ptr to 'this', use the ptr from the original
  // ClientRequestState.
  status = parent_server_->RegisterQuery((*request_state)->query_id(),
      (*request_state)->session(), (*request_state)->parent_driver());
  if (!status.ok()) {
    status.AddDetail(
        retry_failed_msg + Substitute(" registration for new query with id $0 failed",
                               PrintId(retry_request_state->query_id())));
    discard_result((*request_state)->UpdateQueryStatus(status));
    parent_server_->UnregisterQueryDiscardResult(
        retry_request_state->query_id(), false, &status);
    return;
  }

  // Run the new query.
  status = retry_request_state->Exec();
  if (!status.ok()) {
    status.AddDetail(
        retry_failed_msg + Substitute(" Exec for new query with id $0 failed",
                               PrintId(retry_request_state->query_id())));
    discard_result((*request_state)->UpdateQueryStatus(status));
    parent_server_->UnregisterQueryDiscardResult(
        retry_request_state->query_id(), false, &status);
    return;
  }

  status = retry_request_state->WaitAsync();
  if (!status.ok()) {
    status.AddDetail(retry_failed_msg
        + Substitute(" WaitAsync for new query with id $0 failed with error $0",
                         PrintId(retry_request_state->query_id())));
    discard_result((*request_state)->UpdateQueryStatus(status));
    parent_server_->UnregisterQueryDiscardResult(
        retry_request_state->query_id(), false, &status);
    return;
  }

  // Mark the new query as "in flight".
  status =
      parent_server_->SetQueryInflight((*request_state)->session(), retry_request_state);
  if (!status.ok()) {
    status.AddDetail(
        retry_failed_msg + Substitute(" SetQueryInFlight for new query with id $0 failed",
                               PrintId(retry_request_state->query_id())));
    discard_result((*request_state)->UpdateQueryStatus(status));
    parent_server_->UnregisterQueryDiscardResult(
        retry_request_state->query_id(), false, &status);
    return;
  }

  // 'client_request_state_' points to the original query and
  // 'retried_client_request_state_' points to the retried query.
  {
    boost::lock_guard<SpinLock> l(client_request_state_lock_);
    retried_client_request_state_ = retry_request_state;
  }

  // Mark the original query as successfully retried.
  (*request_state)->MarkAsRetried(retry_request_state->query_id());
  VLOG_QUERY << Substitute("Retried query $0 with new query id $0", PrintId(query_id),
      PrintId(retry_request_state->query_id()));

  // Close the old query.
  parent_server_->CloseClientRequestState((*request_state));
  parent_server_->MarkSessionInactive((*request_state)->session());
}
}
