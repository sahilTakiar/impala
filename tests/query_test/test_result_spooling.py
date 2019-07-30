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

import re
import time
import threading

from time import sleep
from tests.common.errors import Timeout
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension
from tests.util.cancel_util import cancel_query_and_validate_state

# Queries to execute, use the TPC-H dataset because tables are large so queries take some
# time to execute.
CANCELLATION_QUERIES = ['select l_returnflag from lineitem',
                        'select * from lineitem limit 50',
                        'select * from lineitem order by l_orderkey']

# Time to sleep between issuing query and canceling.
CANCEL_DELAY_IN_SECONDS = [0, 0.01, 0.1, 1, 4]


class TestResultSpooling(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpooling, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_result_spooling(self, vector):
    self.run_test_case('QueryTest/result-spooling', vector)

  def test_multi_batches(self, vector):
    """Validates that reading multiple row batches works when result spooling is
    enabled."""
    vector.get_value('exec_option')['batch_size'] = 10
    self.validate_query("select id from functional.alltypes order by id limit 100",
        vector.get_value('exec_option'))

  def test_spilling(self, vector):
    """Tests that query results which don't fully fit into memory are spilled to disk.
    The test runs a query asynchronously and wait for the PeakUnpinnedBytes counter in
    the PLAN_ROOT_SINK section of the runtime profile to reach a non-zero value. Then
    it fetches all the results and validates them."""
    query = "select * from functional.alltypes order by id limit 1500"
    exec_options = vector.get_value('exec_option')

    # Set lower values for spill-to-disk configs to force the above query to spill
    # spooled results.
    exec_options['min_spillable_buffer_size'] = 8 * 1024
    exec_options['default_spillable_buffer_size'] = 8 * 1024
    exec_options['max_pinned_result_spooling_memory'] = 32 * 1024

    # Execute the query without result spooling and save the results for later validation
    base_result = self.execute_query(query, exec_options)
    assert base_result.success, "Failed to run {0} when result spooling is disabled" \
                                .format(query)

    # Enable result spooling and run the query, fetch the runtime profile every 0.5
    # seconds until either a timout of 10 seconds is hit, or PeakUnpinnedBytes shows
    # up in the profile.
    exec_options['spool_query_results'] = 'true'
    # Amount of time to wait for the PeakUnpinnedBytes counter in the PLAN_ROOT_SINK
    # section of the profile to reach a non-zero value.
    timeout = 10
    # Regex to look for in the runtime profiles. PeakUnpinnedBytes can show up in exec
    # nodes as well, so we only look for the PeakUnpinnedBytes metrics in the
    # PLAN_ROOT_SINK section of the profile.
    unpinned_bytes_regex = "PLAN_ROOT_SINK[\s\S]*?PeakUnpinnedBytes.*\([1-9][0-9]*\)"

    start_time = time.time()
    handle = self.execute_query_async(query, exec_options)
    try:
      while re.search(unpinned_bytes_regex, self.client.get_runtime_profile(handle)) \
          is None and time.time() - start_time < timeout:
        time.sleep(0.5)
      if re.search(unpinned_bytes_regex, self.client.get_runtime_profile(handle)) is None:
        raise Timeout("Query {0} did not spill spooled results within the timeout {1}"
                      .format(query, timeout))
      result = self.client.fetch(query, handle)
      assert result.data == base_result.data
    finally:
      self.client.close_query(handle)

  def test_full_queue(self, vector):
    """Tests that RowBatchSendWaitTime is updated properly for queries where Impala has
    to block because too much data has been buffered, and the client hasn't fetched
    any results."""
    exec_options = vector.get_value('exec_option')

    # Set lower values for spill-to-disk and result spooling configs so that the queue
    # gets full when selecting a small number of rows.
    exec_options['min_spillable_buffer_size'] = 8 * 1024
    exec_options['default_spillable_buffer_size'] = 8 * 1024
    exec_options['max_pinned_result_spooling_memory'] = 32 * 1024
    exec_options['max_unpinned_result_spooling_memory'] = 32 * 1024
    exec_options['spool_query_results'] = 'true'

    # Regex to look for in the runtime profile.
    send_wait_time_regex = "RowBatchSendWaitTime: [1-9]"
    query = "select * from functional.alltypes order by id limit 1500"

    # Execute the query asynchronously, wait a bit for the result spooling queue to fill
    # up, and then start fetching results.
    handle = self.execute_query_async(query, exec_options)
    try:
      self.wait_for_state(handle, self.client.QUERY_STATES['RUNNING'], 10)
      time.sleep(5)
      self.client.fetch(query, handle)
      assert re.search(send_wait_time_regex, self.client.get_runtime_profile(handle)) \
          is not None
    finally:
      self.client.close_query(handle)

  def test_get_wait_time(self, vector):
    """Tests that RowBatchGetWaitTime is updated properly for queries where the client
    has to block because Impala has not produced rows quickly enough."""
    query = "select id from functional.alltypes order by id limit 10"
    vector.get_value('exec_option')['spool_query_results'] = 'true'
    vector.get_value('exec_option')['debug_action'] = '2:GETNEXT:DELAY'
    get_wait_time_regex = "RowBatchGetWaitTime: [1-9]"
    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    try:
      thread = threading.Thread(target=lambda:
          self.create_impala_client().fetch(query, handle))
      thread.start()
      self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 10)
      assert re.search(get_wait_time_regex, self.client.get_runtime_profile(handle)) \
          is not None
    finally:
      self.client.close_query(handle)

  def validate_query(self, query, exec_options):
    """Compares the results of the given query with and without result spooling
    enabled. Returns the runtime profile of the query executed with spooling enabled."""
    result = self.execute_query(query, exec_options)
    assert result.success, "Failed to run %s when result spooling is disabled" % query
    base_data = result.data
    exec_options['spool_query_results'] = 'true'
    result = self.execute_query(query, exec_options)
    assert result.success, "Failed to run %s when result spooling is enabled" % query
    assert len(result.data) == len(base_data), "%s returned a different number of " \
                                               "results when result spooling was " \
                                               "enabled" % query
    assert result.data == base_data, "%s returned different results when result " \
                                     "spooling was enabled" % query
    return result.runtime_profile


class TestResultSpoolingCancellation(ImpalaTestSuite):
  """Test cancellation of queries when result spooling is enabled. This class heavily
  borrows from the cancellation tests in test_cancellation.py. It uses the following test
  dimensions: 'query' and 'cancel_delay'. 'query' is a list of queries to run
  asynchronously and then cancel. 'cancel_delay' controls how long a query should run
  before being cancelled.
  """

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpoolingCancellation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('query',
        *CANCELLATION_QUERIES))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('cancel_delay',
        *CANCEL_DELAY_IN_SECONDS))

    # Result spooling should be independent of file format, so only testing for
    # table_format=parquet/none in order to avoid a test dimension explosion.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  def test_cancellation(self, vector):
    vector.get_value('exec_option')['spool_query_results'] = 'true'
    cancel_query_and_validate_state(self.client, vector.get_value('query'),
        vector.get_value('exec_option'), vector.get_value('table_format'),
        vector.get_value('cancel_delay'))

  def test_cancel_no_fetch(self, vector):
    """Test cancelling a query before any results are fetched. Unlike the
    test_cancellation test, the query is cancelled before results are
    fetched (there is no fetch thread)."""
    vector.get_value('exec_option')['spool_query_results'] = 'true'
    handle = None
    try:
      handle = self.execute_query_async(vector.get_value('query'),
          vector.get_value('exec_option'))
      sleep(vector.get_value('cancel_delay'))
      cancel_result = self.client.cancel(handle)
      assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: %s' % cancel_result
    finally:
      if handle: self.client.close_query(handle)
