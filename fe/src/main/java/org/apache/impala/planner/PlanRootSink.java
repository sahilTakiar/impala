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

package org.apache.impala.planner;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanRootSink;
import org.apache.impala.thrift.TQueryOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Sink for the root of a query plan that produces result rows. Allows coordination
 * between the sender which produces those rows, and the consumer which sends them to the
 * client, despite both executing concurrently.
 */
public class PlanRootSink extends DataSink {

  private final TupleDescriptor tupleDescriptor_;

  public PlanRootSink(TupleDescriptor tupleDescriptor) {
    super();
    tupleDescriptor_ = tupleDescriptor;
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(String.format("%sPLAN-ROOT SINK\n", prefix));
  }

  @Override
  protected String getLabel() {
    return "ROOT";
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    // TODO should we use the spillable buffer size configs or create new ones
    // DEFAULT_SPILLABLE_BUFFER_SIZE (default = 2 mb)
    // MIN_SPILLABLE_BUFFER_SIZE (default = 64 kb)
    // MAX_ROW_SIZE (default = 512 kb)
    long bufferSize = queryOptions.getDefault_spillable_buffer_size();
    long maxRowBufferSize = PlanNode.computeMaxSpillableBufferSize(bufferSize,
            queryOptions.getMax_row_size());
    resourceProfile_ = new ResourceProfileBuilder()
        // TODO probably a better way to do this based on stats
	.setMemEstimateBytes(0)
        // TODO how to set this? changing to "maxRowBufferSize * 2" triggered a DCHECK
	// Since the BTS need one page for reading and one page for writing, this should
	// at least by 2 * default_page_size
	.setMinMemReservationBytes(2 * bufferSize)
        .setMaxRowBufferBytes(maxRowBufferSize)
        .setSpillableBufferBytes(bufferSize)
        .build();
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TPlanRootSink planRootSink = new TPlanRootSink();
    planRootSink.row_tuples = new ArrayList<>();
    planRootSink.nullable_tuples = new ArrayList<>();
    planRootSink.row_tuples.add(tupleDescriptor_.getId().asInt());
    planRootSink.nullable_tuples.add(false); // TODO not sure if this is correct
    planRootSink.resource_profile = resourceProfile_.toThrift();
    tsink.plan_root_sink = planRootSink;
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.PLAN_ROOT_SINK;
  }
}
