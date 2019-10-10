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

#include <unordered_map>

#include "common/status.h"
#include "service/impala-server.h"
#include "util/thread-pool.h"

#include "common/names.h"

namespace impala {

class ClientRequestState;
class ExecEnv;
class TExecRequest;

/// References to a ClientRequestState should be done via a QueryHandle. A
/// ClientRequestState requires a QueryDriver to be in scope, since the QueryDriver
/// owns the ClientRequestState. The QueryDriver exposes methods that return a naked
/// pointer to the ClientRequestState, and the pointer only references valid memory as
/// long as its corresponding QueryDriver is alive.
struct QueryHandle {
  std::shared_ptr<QueryDriver> query_driver;
  ClientRequestState* request_state = nullptr;
};

/// The QueryDriver owns the ClientRequestStates for a query. A single query can map
/// to multiple ClientRequestStates if the query is retried multiple times. The
/// QueryDriver sits between the ImpalaServer and ClientRequestState. Currently, it
/// mainly handles query retries.
///
/// ImpalaServer vs. QueryDriver vs. ClientRequestState vs. Coordinator:
///
/// All of these classes work together to run a client-submitted query end to end. A
/// client submits a request to the ImpalaServer to run a query. The ImpalaServer
/// creates a QueryDriver and ClientRequestState to run the query. The
/// ClientRequestState creates a Coordinator that is responsbile for coordinating the
/// execution of all query fragments and for tracking the lifecyle of the fragments. A
/// fetch results request flows from the ImpalaServer to the ClientRequestState and then
/// to the Coordinator, which fetches the results from the fragment running on the
/// coordinator process.
///
/// Query Retries:
///
/// Query retries are driven by the 'TryRetryQuery' and 'RetryAsync' methods. Each query
/// retry creates a new ClientRequestState. In other words, a single ClientRequestState
/// corresponds to a single attempt of a query. Thus, a QueryDriver can own multiple
/// ClientRequestStates, one for each query attempt.
///
/// Retries are done asynchronously in a separate thread. If the query cannot be retried
/// for any reason, the original query is unregistered and moved to the ERROR state. The
/// steps required to launch a retry of a query are very similar to the steps necessary
/// to start the original attempt of the query, except parsing, planning, optimizing,
/// etc. are all skipped. This is done by cacheing the TExecRequest from the original
/// query and re-using it for all query retries.
///
/// At a high level, retrying a query requires performing the following steps:
///   * Cancelling the original query
///   * Creating a new ClientRequestState with a unique query id and a copy of the
///     TExecRequest from the original query
///   * Registering the new query (ImpalaServer::RegisterQuery)
///   * Launching the new query (ClientRequestState::Exec)
///   * Closing and unregistering the original query
///
/// *Transparent* Query Retries:
/// A key feature of query retries is that they should be "transparent" from the client
/// perspective. No client code modifications should be necessary to support query
/// retries. The QueryDriver makes query retries "transparent" by introducing the
/// concept of an "active" ClientRequestState. The "active" ClientRequestState refers
/// to the currently running attempt to run the query, and is accessible by the method
/// GetActiveClientRequestState(). Callers (e.g. ImpalaServer) can use this method to get
/// a reference to the most recent attempt of the query.
///
/// Thread Safety:
///
/// Only GetClientRequestState(query_id) and GetActiveClientRequestState() are thread
/// safe. They are protected by an internal SpinLock.
class QueryDriver {
 public:
  QueryDriver(ImpalaServer* parent_server);
  ~QueryDriver();

  /// Creates the initial ClientRequestState for the given TQueryCtx. Should only be
  /// called once by the ImpalaServer. Additional ClientRequestStates are created by
  /// CreateRetriedClientRequestState, although they are only created if the query
  /// is retried.
  void CreateClientRequestState(const TQueryCtx& query_ctx,
      std::shared_ptr<ImpalaServer::SessionState> session_state,
      QueryHandle* query_handle);

  /// Creates the TExecRequest for this query. The TExecRequest is owned by the
  /// QueryDriver. The TExecRequest is created by calling into Frontend.java,
  /// specifically, the Frontend#createExecRequest(PlanCtx) method. When creating the
  /// TExecRequest, the Frontend runs the parser, analyzer, authorization code, planner,
  /// optimizer, etc.
  Status RunFrontendPlanner(const TQueryCtx& query_ctx) WARN_UNUSED_RESULT;

  /// Returns the ClientRequestState corresponding to the given query id.
  ClientRequestState* GetClientRequestState(const TUniqueId& query_id);

  /// Returns the active ClientRequestState for the query. If a query is retried, this
  /// method returns the most recent attempt of the query.
  ClientRequestState* GetActiveClientRequestState();

  /// Retry the query if query retries are enabled (they are enabled / disabled using the
  /// query option 'retry_failed_queries') and if the query can be retried. Queries can
  /// only be retried if (1) no rows have already been fetched for the query, (2) the
  /// query has not already been retried (when a query fails, only a single retry of that
  /// query attempt should be run), and (3) the max number of retries has not been
  /// exceeded (currently the limit is just one retry). Queries should only be retried if
  /// there has been a cluster membership change. So either a node is blacklisted or a
  /// statestore update removes a node from the cluster membership. The retry is done
  /// asynchronously. Returns an error status if the retry can not be started. 'status'
  /// is the reason why the query failed. If the attempt to retry the query failed,
  /// additional details might be added to the status. If 'was_retried' is not nullptr it
  /// is set to true if the query was actually retried, false otherwise. This method is
  /// idempotent, it can safely be called multiple times, however, only the first call
  /// to the method will trigger a retry.
  Status TryQueryRetry(ClientRequestState* client_request_state, Status* status,
      bool* was_retried = nullptr) WARN_UNUSED_RESULT;

  /// Called before starting the unregistration process. This indicates that the client
  /// should no longer be considered registered from the client's point of view. Returns
  /// an INVALID_QUERY_HANDLE error if unregistration already started.
  Status StartUnregister();

  /// Delete this query from the given QueryDriverMap.
  Status Unregister(QueryDriverMap* query_driver_map) WARN_UNUSED_RESULT;

  /// True if StartUnregister() was called.
  bool started_unregister() const { return started_unregister_.Load(); }

 private:
  /// Schedule a retry of the query with the given failure reason. The retry will be done
  /// asynchronously by a dedicated thread. Returns an error if the thread could not be
  /// started.
  Status RetryAsync(const Status& error) WARN_UNUSED_RESULT;

  /// Helper method to process query retries, called by the 'retry_query_thread_'.
  /// 'error' is the reason why the query failed. The failed query is cancelled, and then
  /// a new ClientRequestState is created for the retried query. The new
  /// ClientRequestState copies the TExecRequest from the failed query in order to avoid
  /// query compilation and planning again. Once the new query is registered and launched,
  /// the failed query is unregistered. 'query_driver' is a shared_ptr to 'this'
  /// QueryDriver. The pointer is necessary to ensure that 'this' QueryDriver is not
  /// deleted while the thread is running.
  void RetryQueryFromThread(
      const Status& error, std::shared_ptr<QueryDriver> query_driver);

  /// Helper method for RetryQueryFromThread. Creates the retry client request state (the
  /// new attempt of the query) based on the original request state. Uses the TExecRequest
  /// from the original request state to create the retry request state. Creates a new
  /// query id for the retry request state.
  void CreateRetriedClientRequestState(ClientRequestState* request_state,
      std::unique_ptr<ClientRequestState>* retry_request_state);

  /// Helper method for handling failures when retrying a query. 'status' is the reason
  /// why the retry failed and is expected to be in the error state. Additional details
  /// are added to the 'status'. Once the 'status' has been updated, it is set as the
  /// 'query status' of the given 'request_state'. Finally, the 'request_state' is
  /// unregistered from 'parent_server_', since the retry failed.
  void HandleRetryFailure(Status* status, string* error_msg,
      ClientRequestState* request_state, const TUniqueId& retry_query_id);

  /// ImpalaServer that owns this QueryDriver.
  ImpalaServer* parent_server_;

  /// Protects 'client_request_state_' and 'retried_client_request_state_'.
  SpinLock client_request_state_lock_;

  /// The ClientRequestState for the query. Set in 'CreateClientRequestState'. Owned by
  /// the QueryDriver. A shared_ptr is used to allow asynchronous deletion. A client may
  /// still be using the ClientRequestState after the QueryDriver is deleted.
  std::unique_ptr<ClientRequestState> client_request_state_;

  /// The ClientRequestState for the retried query. Set in 'RetryAsync'. Only set if the
  /// query is retried. Owned by the QueryDriver. A shared_ptr is used to allow
  /// asynchronous deletion.
  std::unique_ptr<ClientRequestState> retried_client_request_state_;

  /// The TExecRequest for the query. Created in 'CreateClientRequestState' and loaded in
  /// 'RunFrontendPlanner'.
  std::unique_ptr<TExecRequest> exec_request_;

  /// The TExecRequest for the retried query. Only set if the query is actually retried.
  /// It is initialized by copying 'exec_request_'.
  std::unique_ptr<TExecRequest> retry_exec_request_;

  /// Thread to process query retry requests. Done in a separate thread to avoid blocking
  /// control service RPC threads.
  std::unique_ptr<Thread> retry_query_thread_;

  /// True if a thread has called StartUnregister(). Threads calling StartUnregister()
  /// do a compare-and-swap on this so that only one thread can proceed.
  AtomicBool started_unregister_{false};
};
}
