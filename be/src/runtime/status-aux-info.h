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

#include <boost/thread/lock_guard.hpp>

#include "common/names.h"
#include "gen-cpp/control_service.pb.h"
#include "util/network-util.h"
#include "util/spinlock.h"

namespace impala {

/// Mirror of StatusAuxInfoPB with some extra utilities for setting and getting members.
/// All methods are thread safe, and are guarded by a single SpinLock.
class StatusAuxInfo {
 public:
  /// Wrapper around a TRPCErrorMessage that provides a convenience constructor for
  /// specifying the parameters of TRPCErrorMessage (currently just the TNetworkAddress
  /// of the destination node of a failed RPC).
  class RPCErrorMessage {
    friend class StatusAuxInfo;

   public:
    RPCErrorMessage(TNetworkAddress dest_node) {
      NetworkAddressPB* network_addr = new NetworkAddressPB();
      network_addr->set_hostname(dest_node.hostname);
      network_addr->set_port(dest_node.port);
      rpc_error_msg_pb_.set_allocated_dest_node(network_addr);
    }

   private:
    RPCErrorMessagePB rpc_error_msg_pb_;
  };

  /// Sets the RPCErrorMessage for this StatusAuxInfo. Only sets the RPCErrorMessage once,
  /// all subsequent attempts to set the RPCErrorMessage are ignored (only the first
  /// RPCErrorMessage set is included in the StatusAuxInfo).
  void SetRPCErrorMessage(std::unique_ptr<RPCErrorMessage> rpc_error_msg) {
    boost::lock_guard<SpinLock> l(lock_);
    if (rpc_error_msg_ == nullptr) rpc_error_msg_ = move(rpc_error_msg);
  }

  /// Creates and returns a StatusAuxInfoPB equivalent of this class with the
  /// RPCErrorMessagePB set if SetRPCErrorMessage was called.
  StatusAuxInfoPB* CreateStatusAuxInfoPB() {
    StatusAuxInfoPB* status_info = new StatusAuxInfoPB();
    boost::lock_guard<SpinLock> l(lock_);
    if (rpc_error_msg_ != nullptr) {
      *status_info->mutable_rpc_error_msg() = rpc_error_msg_->rpc_error_msg_pb_;
    }
    return status_info;
  }

 private:
  /// Protects all members below.
  SpinLock lock_;
  std::unique_ptr<RPCErrorMessage> rpc_error_msg_;
};
}
