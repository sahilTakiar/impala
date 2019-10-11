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

#include "common/status.h"
#include "gen-cpp/Types_types.h"

namespace impala {

/// Work item for ImpalaServer::retry_thread_pool_.
/// This class needs to support move construction and assignment for use in ThreadPool.
class RetryWork {
 public:
  // Empty constructor needed to make ThreadPool happy.
  RetryWork() {}

  RetryWork(const TUniqueId& query_id, const Status& error) 
    : query_id_(query_id), error_(error) {};

  const TUniqueId& query_id() const { return query_id_; }
  const Status& error() const { return error_; }

 private:
  // ID of query to be cancelled.
  TUniqueId query_id_;

  Status error_;
};
} // namespace impala
