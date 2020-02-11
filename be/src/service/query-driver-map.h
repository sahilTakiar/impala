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

#include "gutil/strings/substitute.h"
#include "util/sharded-query-map-util.h"

namespace impala {

class QueryDriver;

/// A ShardedQueryMap for QueryDrivers. Maps a query_id to its corresponding
/// QueryDriver. Provides helper methods to easily add and delete
/// QueryDrivers from a ShardedQueryMap. The QueryDrivers are non-const, so
/// users of this class need to synchronize access to the QueryDrivers either by
/// creating a ScopedShardedMapRef or by locking on the QueryDriver::lock().
class QueryDriverMap : public ShardedQueryMap<std::shared_ptr<QueryDriver>> {
 public:
  /// Adds the given (query_id, query_driver) pair to the map. Returns an error Status
  /// if the query id already exists in the map.
  Status AddQueryDriver(
      const TUniqueId& query_id, std::shared_ptr<QueryDriver> query_driver) {
    ScopedShardedMapRef<std::shared_ptr<QueryDriver>> map_ref(query_id, this);
    DCHECK(map_ref.get() != nullptr);

    auto entry = map_ref->find(query_id);
    if (entry != map_ref->end()) {
      // There shouldn't be an active query with that same id.
      // (query_id is globally unique)
      return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
          strings::Substitute("query id $0 already exists", PrintId(query_id))));
    }
    map_ref->insert(make_pair(query_id, query_driver));
    return Status::OK();
  }

  /// Deletes the specified (query_id, query_driver) pair from the map and sets the given
  /// query_driver pointer to the QueryDriver associated with the given query_id.
  /// If query_driver == nullptr, it is not set. Returns an error Status if the query_id
  /// cannot be found in the map.
  Status DeleteQueryDriver(
      const TUniqueId& query_id, std::shared_ptr<QueryDriver>* query_driver = nullptr) {
    ScopedShardedMapRef<std::shared_ptr<QueryDriver>> map_ref(query_id, this);
    DCHECK(map_ref.get() != nullptr);
    auto entry = map_ref->find(query_id);
    if (entry == map_ref->end()) {
      string error_msg =
          strings::Substitute("Invalid or unknown query handle $0", PrintId(query_id));
      VLOG(1) << error_msg;
      return Status::Expected(error_msg);
    } else if (query_driver != nullptr) {
      *query_driver = entry->second;
    }
    map_ref->erase(entry);
    return Status::OK();
  }
};
}
