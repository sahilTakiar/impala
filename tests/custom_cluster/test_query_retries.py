# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest

from random import randint
from time import sleep

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestQueryRetries(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_query_retry(self, cursor):
    query = None
    with open(
        'testdata/workloads/tpcds/queries/raw/tpcds-query4.sql', 'r') as tpcds_query4:
      query = tpcds_query4.read()
    assert self.execute_query("use tpcds_parquet").success
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle, self.client.QUERY_STATES['RUNNING'], 60)
    sleep(randint(0, 10))
    self.cluster.impalads[2].kill()
    results = self.client.fetch(query, handle)
    assert results.success
    assert len(results.data) == 8
    self.client.close_query(handle)

  @pytest.mark.execute_serially
  def test_query_retry_multi(self, cursor):
    query = None
    with open(
        'testdata/workloads/tpcds/queries/raw/tpcds-query4.sql', 'r') as tpcds_query4:
      query = tpcds_query4.read()
    assert self.execute_query("use tpcds_parquet").success
    handle1 = self.execute_query_async(query,
            query_options={'retry_failed_queries': 'true'})
    handle2 = self.execute_query_async(query,
            query_options={'retry_failed_queries': 'true'})
    handle3 = self.execute_query_async(query,
            query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle1, self.client.QUERY_STATES['RUNNING'], 60)
    self.wait_for_state(handle2, self.client.QUERY_STATES['RUNNING'], 60)
    self.wait_for_state(handle3, self.client.QUERY_STATES['RUNNING'], 60)
    sleep(randint(0, 10))
    self.cluster.impalads[2].kill()
    results1 = self.client.fetch(query, handle1)
    assert results1.success
    print str(results1.runtime_profile)
    assert len(results1.data) == 8
    results2 = self.client.fetch(query, handle2)
    assert results2.success
    print str(results2.runtime_profile)
    assert len(results2.data) == 8
    results3 = self.client.fetch(query, handle3)
    assert results3.success
    print str(results3.runtime_profile)
    assert len(results3.data) == 8
    self.client.close_query(handle1)
    self.client.close_query(handle2)
    self.client.close_query(handle3)
