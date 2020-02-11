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
#include "service/retry-work.h"
#include "util/thread-pool.h"

#include "common/names.h"

namespace impala {

class ClientRequestState;
class ExecEnv;
class ImpalaServer;
class SessionState;

class QueryDriver {
 public:
  QueryDriver(ExecEnv* exec_env_, ImpalaServer* parent_server,
      std::shared_ptr<ClientRequestState> client_request_state);

  std::shared_ptr<ClientRequestState> GetClientRequestState(const TUniqueId& query_id);

  std::shared_ptr<ClientRequestState> GetClientRequestState();

  /// Schedule a retry of the query with the given query_id. The retry will be done
  /// asynchronously by a dedicated threadpool.
  void RetryAsync(const TUniqueId& query_id, const Status& error);

 private:
  /// Helper method to process query retries, called from the query retry thread pool. The
  /// RetryWork contains the query id to retry and the reason the query failed. The failed
  /// query is cancelled, and then a new ClientRequestState is created for the retried
  /// query. The new ClientRequestState copies the TExecRequest from the failed query in
  /// order to avoid query compilation and planning again. Once the new query is
  /// registered and launched, the failed query is unregistered.
  void RetryQueryFromThreadPool(uint32_t thread_id, const RetryWork& retry_work);

  ExecEnv* exec_env_;
  ImpalaServer* parent_server_;

  SpinLock client_request_state_lock_;
  std::shared_ptr<ClientRequestState> client_request_state_;
  std::shared_ptr<ClientRequestState> retried_client_request_state_;

  /// Thread pool to process query retry requests that come from query state updates to
  /// avoid blocking control service RPC threads.
  boost::scoped_ptr<ThreadPool<RetryWork>> retry_thread_pool_;
};
}
