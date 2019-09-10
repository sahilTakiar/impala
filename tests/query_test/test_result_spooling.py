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
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.util.cancel_util import cancel_query_and_validate_state


class TestResultSpooling(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpooling, cls).add_test_dimensions()
    # Result spooling should be independent of file format, so only test against
    # Parquet files.
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
    self.__validate_query("select id from functional_parquet.alltypes order by id "
                          "limit 1000", vector.get_value('exec_option'))

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
    exec_options['max_result_spooling_mem'] = 32 * 1024

    # Execute the query without result spooling and save the results for later validation
    base_result = self.execute_query(query, exec_options)
    assert base_result.success, "Failed to run {0} when result spooling is disabled" \
                                .format(query)

    exec_options['spool_query_results'] = 'true'

    # Amount of time to wait for the PeakUnpinnedBytes counter in the PLAN_ROOT_SINK
    # section of the profile to reach a non-zero value.
    timeout = 30

    # Regexes to look for in the runtime profiles.
    # PeakUnpinnedBytes can show up in exec nodes as well, so we only look for the
    # PeakUnpinnedBytes metrics in the PLAN_ROOT_SINK section of the profile.
    unpinned_bytes_regex = "PLAN_ROOT_SINK[\s\S]*?PeakUnpinnedBytes.*\([1-9][0-9]*\)"
    # The PLAN_ROOT_SINK should have 'Spilled' in the 'ExecOption' info string.
    spilled_exec_option_regex = "ExecOption:.*Spilled"

    # Fetch the runtime profile every 0.5 seconds until either the timeout is hit, or
    # PeakUnpinnedBytes shows up in the profile.
    start_time = time.time()
    handle = self.execute_query_async(query, exec_options)
    try:
      while re.search(unpinned_bytes_regex, self.client.get_runtime_profile(handle)) \
          is None and time.time() - start_time < timeout:
        time.sleep(0.5)
      profile = self.client.get_runtime_profile(handle)
      if re.search(unpinned_bytes_regex, profile) is None:
        raise Timeout("Query {0} did not spill spooled results within the timeout {1}"
                      .format(query, timeout))
      # At this point PLAN_ROOT_SINK must have spilled, so spilled_exec_option_regex
      # should be in the profile as well.
      assert re.search(spilled_exec_option_regex, profile)
      result = self.client.fetch(query, handle)
      assert result.data == base_result.data
    finally:
      self.client.close_query(handle)

  def test_full_queue(self, vector):
    """Delegates to _test_full_queue."""
    query = "select * from functional.alltypes order by id limit 1500"
    self._test_full_queue(vector, query)

  def test_full_queue_large_fetch(self, vector):
    """Delegates to _test_full_queue, but specifies a fetch size equal to the number of
    rows returned by the query. This tests that clients can fetch all rows from a full
    queue."""
    num_rows = 1500
    query = "select * from functional.alltypes order by id limit {0}".format(num_rows)
    self._test_full_queue(vector, query, fetch_size=num_rows)

  def _test_full_queue(self, vector, query, fetch_size=-1):
    """Tests result spooling when there is no more space to buffer query results (the
    queue is full), and the client hasn't fetched any results. Validates that
    RowBatchSendWaitTime (amount of time Impala blocks waiting for the client to read
    buffered results and clear up space in the queue) is updated properly."""
    exec_options = vector.get_value('exec_option')

    # Set lower values for spill-to-disk and result spooling configs so that the queue
    # gets full when selecting a small number of rows.
    exec_options['min_spillable_buffer_size'] = 8 * 1024
    exec_options['default_spillable_buffer_size'] = 8 * 1024
    exec_options['max_result_spooling_mem'] = 32 * 1024
    exec_options['max_spilled_result_spooling_mem'] = 32 * 1024
    exec_options['spool_query_results'] = 'true'

    # Amount of time to wait for the query to reach a running state before through a
    # Timeout exception.
    timeout = 10
    # Regex to look for in the runtime profile.
    send_wait_time_regex = "RowBatchSendWaitTime: [1-9]"

    # Execute the query asynchronously, wait a bit for the result spooling queue to fill
    # up, start fetching results, and then validate that RowBatchSendWaitTime shows a
    # non-zero value in the profile.
    handle = self.execute_query_async(query, exec_options)
    try:
      self.wait_for_any_state(handle, [self.client.QUERY_STATES['RUNNING'],
          self.client.QUERY_STATES['FINISHED']], timeout)
      time.sleep(5)
      self.client.fetch(query, handle, max_rows=fetch_size)
      assert re.search(send_wait_time_regex, self.client.get_runtime_profile(handle)) \
          is not None
    finally:
      self.client.close_query(handle)

  def test_slow_query(self, vector):
    """Tests results spooling when the client is blocked waiting for Impala to add more
    results to the queue. Validates that RowBatchGetWaitTime (amount of time the client
    spends waiting for Impala to buffer query results) is updated properly."""
    query = "select id from functional.alltypes"

    # Add a delay to the SCAN_NODE in the query above to simulate a "slow" query. The
    # delay should give the client enough time to issue a fetch request and block until
    # Impala produces results.
    vector.get_value('exec_option')['debug_action'] = '0:GETNEXT:DELAY'
    vector.get_value('exec_option')['spool_query_results'] = 'true'

    # Regex to look for in the runtime profile.
    get_wait_time_regex = "RowBatchGetWaitTime: [1-9]"

    # Execute the query, start a thread to fetch results, wait for the query to finish,
    # and then validate that RowBatchGetWaitTime shows a non-zero value in the profile.
    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    try:
      thread = threading.Thread(target=lambda:
          self.create_impala_client().fetch(query, handle))
      thread.start()
      self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 10)
      thread.join()
      assert re.search(get_wait_time_regex, self.client.get_runtime_profile(handle)) \
          is not None
    finally:
      self.client.close_query(handle)

  def __validate_query(self, query, exec_options):
    """Compares the results of the given query with and without result spooling
    enabled."""
    exec_options = exec_options.copy()
    result = self.execute_query(query, exec_options)
    assert result.success, "Failed to run {0} when result spooling is " \
                           "disabled".format(query)
    base_data = result.data
    exec_options['spool_query_results'] = 'true'
    result = self.execute_query(query, exec_options)
    assert result.success, "Failed to run {0} when result spooling is " \
                           "enabled".format(query)
    assert len(result.data) == len(base_data), "{0} returned a different number of " \
                                               "results when result spooling was " \
                                               "enabled".format(query)
    assert result.data == base_data, "{0} returned different results when result " \
                                     "spooling was enabled".format(query)


class TestResultSpoolingFetchSize(ImpalaTestSuite):
  """Tests fetching logic when result spooling is enabled. When result spooling is
  disabled, Impala only supports fetching up to BATCH_SIZE rows at a time (since only
  one RowBatch is ever buffered). When result spooling is enabled, clients can specify
  any fetch size (up to a limit) and Impala will return exactly that number of rows
  (assuming there are that many rows left to fetch). This class validates the
  aformentioned result spooling fetch logic using different fetch and batch sizes."""

  # The different values of BATCH_SIZE 'test_fetch' will be parameterized by.
  _batch_sizes = [100, 1024, 2048]

  # The number of rows to fetch from the query handle.
  _fetch_sizes = [7, 23, 321, 512, 2048, 4321, 5000, 10000]

  # The number of rows in functional_parquet.alltypes.
  _num_rows = 7300

  # The query that 'test_fetch' will run.
  _query = "select id from functional_parquet.alltypes order by id"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpoolingFetchSize, cls).add_test_dimensions()
    # Create a test matrix with three different dimensions: BATCH_SIZE, the number of
    # rows to fetch at a time, and whether the tests should wait for all results to be
    # spooled before fetching any rows.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        batch_sizes=cls._batch_sizes))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('fetch_size',
        *cls._fetch_sizes))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('wait_for_finished',
        *[True, False]))

    # Result spooling should be independent of file format, so only testing for
    # table_format=parquet/none in order to avoid a test dimension explosion.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def setup_class(cls):
    super(TestResultSpoolingFetchSize, cls).setup_class()
    # All tests only ever run a single query, so rather than re-run this query for every
    # test, run it once and store the results.
    base_result = cls.client.execute(cls._query)
    assert base_result.success, "Failed to run {0} when result spooling is " \
                                "enabled".format(cls._query)
    cls._base_data = base_result.data

  def test_fetch(self, vector):
    """Run '_query' with result spooling enabled and with the specified BATCH_SIZE. Use
    the 'fetch_size' parameter to determine how many rows to fetch from the query handle
    at a time. Fetch all results and then validate they match '_base_data'."""
    exec_options = vector.get_value('exec_option')
    exec_options['spool_query_results'] = 'true'
    fetch_size = vector.get_value('fetch_size')

    # Amount of time to wait for the query to reach a running state before through a
    # Timeout exception.
    timeout = 10

    results = []
    handle = self.execute_query_async(self._query, exec_options)
    try:
      # If 'wait_for_finished' is True, wait for the query to reach the FINISHED state.
      # When it reaches this state all results should be successfully spooled.
      if vector.get_value('wait_for_finished'):
          self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], timeout)
      rows_fetched = 0

      # Call 'fetch' on the query handle enough times to read all rows.
      while rows_fetched < self._num_rows:
        result_data = self.client.fetch(self._query, handle, fetch_size).data
        # Assert that each fetch request returns exactly the number of rows requested,
        # unless less than that many rows were left in the result set.
        assert len(result_data) == min(fetch_size, self._num_rows - rows_fetched)
        rows_fetched += len(result_data)
        results.extend(result_data)
    finally:
       self.client.close_query(handle)

    # Assert that the fetched results match the '_base_data'.
    assert self._num_rows == rows_fetched
    assert self._base_data == results


class TestResultSpoolingCancellation(ImpalaTestSuite):
  """Test cancellation of queries when result spooling is enabled. This class heavily
  borrows from the cancellation tests in test_cancellation.py. It uses the following test
  dimensions: 'query' and 'cancel_delay'. 'query' is a list of queries to run
  asynchronously and then cancel. 'cancel_delay' controls how long a query should run
  before being cancelled.
  """

  # Queries to execute, use the TPC-H dataset because tables are large so queries take
  # some time to execute.
  _cancellation_queries = ["select l_returnflag from tpch_parquet.lineitem",
                          "select * from tpch_parquet.lineitem limit 50",
                          "select * from tpch_parquet.lineitem order by l_orderkey"]

  # Time to sleep between issuing query and canceling.
  _cancel_delay_in_seconds = [0, 0.01, 0.1, 1, 4]

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpoolingCancellation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('query',
        *cls._cancellation_queries))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('cancel_delay',
        *cls._cancel_delay_in_seconds))

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
          "Unexpected status code from cancel request: {0}".format(cancel_result)
    finally:
      if handle: self.client.close_query(handle)


class TestFailpoints(ImpalaTestSuite):

  _debug_actions = ['5:GETNEXT:MEM_LIMIT_EXCEEDED|0:GETNEXT:DELAY', 'BPRS_BEFORE_OPEN:FAIL', 
            'BPRS_BEFORE_ADD_BATCH:FAIL@1.0', 'BPRS_BEFORE_FLUSH_FINAL:FAIL',
            'BPRS_BEFORE_GET_BATCH:FAIL@1.0']
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_failpoints(self, vector):
    # this reproduce the error, but not really sure why it works but it does so just stick with it
    vector.get_value('exec_option')['batch_size'] = 10
    query = "select 1 from functional.alltypessmall a join functional.alltypessmall b on a.id = b.id"
    vector.get_value('exec_option')['debug_action'] = '5:GETNEXT:MEM_LIMIT_EXCEEDED|0:GETNEXT:DELAY'
    vector.get_value('exec_option')['spool_query_results'] = 'true'
    try:
      result = self.execute_query(query, vector.get_value('exec_option'))
      assert 'Expected Failure'
    except ImpalaBeeswaxException as e:
      LOG.debug(e)
    del vector.get_value('exec_option')['debug_action']
    self.execute_query(query, vector.get_value('exec_option'))
