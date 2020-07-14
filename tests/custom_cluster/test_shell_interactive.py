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

from multiprocessing.pool import ThreadPool
from random import randint

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_vector import ImpalaTestVector
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.shell.util import (get_shell_cmd, get_impalad_port, run_impala_shell_cmd,
                              spawn_shell, wait_for_query_state)


class TestShellInteractive(CustomClusterTestSuite):

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-default_pool_max_requests 1")
  def test_admission_status(self):
    """Test whether the admission status gets printed if a query gets queued when
    either live_summary or live_progress is set to true"""
    expected_admission_status = "Query queued. Latest queuing reason: " \
                                "number of running queries 1 is at or over limit 1"
    # Start a long running query so that the next one gets queued.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)

    # Iterate over test vector within test function to avoid restarting cluster.
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      proc = spawn_shell(get_shell_cmd(vector))
      # Check with only live_summary set to true.
      proc.expect("{0}] default>".format(get_impalad_port(vector)))
      proc.sendline("set live_summary=true;")
      proc.sendline("select 1;")
      proc.expect(expected_admission_status)
      proc.sendcontrol('c')
      proc.expect("Cancelling Query")
      # Check with only live_progress set to true.
      proc.sendline("set live_summary=false;")
      proc.sendline("set live_progress=true;")
      proc.sendline("select 1;")
      proc.expect(expected_admission_status)

  @pytest.mark.execute_serially
  def test_query_retries(self):
    """Tests transparent query retries via impala-shell. Validats that the output of the
    'profile;' command in impala-shell prints out both the original and retried runtime
    profiles."""
    query_options = "set retry_failed_queries=true;"
    query = "select count(*) from functional.alltypes where bool_col = sleep(50)"
    vector = ImpalaTestVector([ImpalaTestVector.Value("protocol", "hs2")])
    pool = ThreadPool(processes=1)
    result_async = pool.apply_async(lambda: run_impala_shell_cmd(
        vector, ['-q', query_options + query + "; profile", '-B']))
    wait_for_query_state(vector, query, "RUNNING")
    self.cluster.impalads[
        randint(1, ImpalaTestSuite.get_impalad_cluster_size() - 1)].kill()

    result = result_async.get().stdout.strip()
    result_rows = result.split('\n')
    assert len(result_rows) > 1
    assert result_rows[0] == "3650"

    assert "Query Runtime Profile:" in result, result
    assert "Retry Status: RETRIED" in result, result
    assert "Query State: FINISHED" in result, result
    assert "Query State: EXCEPTION" in result, result
    assert "Failed Query Runtime Profile(s):" in result, result
