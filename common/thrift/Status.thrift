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

namespace cpp impala
namespace java org.apache.impala.thrift

include "ErrorCodes.thrift"

// NOTE: The definitions in this file are part of the binary format of the Impala query
// profiles. They should preserve backwards compatibility and as such some rules apply
// when making changes. Please see RuntimeProfile.thrift for more details.

// A struct of optional properties associated with a TStatus object. ErrorCodes specify
// the actual error encapsulated by a TStatus object, whereas properties are additional
// metadata used to describe properties of the TStatus object. For example, if the error
// is retryable or not. TStatusProperties are only used internally, and are not directly
// exposed to the client.
struct TStatusProperties {
  // True if the Status is retryable, false otherwise (false by default). Retryable errors
  // cause the Coordinator to transparently retry the query (IMPALA-9124).
  1: optional bool is_retryable = 0
  // True if the Status is recoverable, false otherwise (false by default). Recoverable
  // errors indicate client recoverable errors (e.g. the error is not fatal and the client
  // can recover from the error).
  2: optional bool is_recoverable = 0
}

struct TStatus {
  1: required ErrorCodes.TErrorCode status_code
  2: list<string> error_msgs
  3: optional TStatusProperties status_properties
}
