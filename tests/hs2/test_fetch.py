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
#

import pytest
import re

from time import sleep
from time import time
from tests.common.errors import Timeout
from tests.hs2.hs2_test_suite import (HS2TestSuite, needs_session,
    create_op_handle_without_secret)
from TCLIService import TCLIService, constants
from TCLIService.ttypes import TTypeId


# Simple test to make sure all the HS2 types are supported for both the row and
# column-oriented versions of the HS2 protocol.
class TestFetch(HS2TestSuite):
  def __verify_primitive_type(self, expected_type, hs2_type):
    assert hs2_type.typeDesc.types[0].primitiveEntry.type == expected_type

  def __verify_char_max_len(self, t, max_len):
    l = t.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers\
      [constants.CHARACTER_MAXIMUM_LENGTH]
    assert l.i32Value == max_len

  def __verify_decimal_precision_scale(self, hs2_type, precision, scale):
    p = hs2_type.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers\
      [constants.PRECISION]
    s = hs2_type.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers\
      [constants.SCALE]
    assert p.i32Value == precision
    assert s.i32Value == scale

  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  def test_result_metadata_v1(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle

    # Verify all primitive types in the alltypes table.
    execute_statement_req.statement =\
        "SELECT * FROM functional.alltypessmall ORDER BY id LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch_at_most(execute_statement_resp.operationHandle,
                                 TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    column_types = metadata_resp.schema.columns
    assert len(column_types) == 13
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[0])
    self.__verify_primitive_type(TTypeId.BOOLEAN_TYPE, column_types[1])
    self.__verify_primitive_type(TTypeId.TINYINT_TYPE, column_types[2])
    self.__verify_primitive_type(TTypeId.SMALLINT_TYPE, column_types[3])
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[4])
    self.__verify_primitive_type(TTypeId.BIGINT_TYPE, column_types[5])
    self.__verify_primitive_type(TTypeId.FLOAT_TYPE, column_types[6])
    self.__verify_primitive_type(TTypeId.DOUBLE_TYPE, column_types[7])
    self.__verify_primitive_type(TTypeId.STRING_TYPE, column_types[8])
    self.__verify_primitive_type(TTypeId.STRING_TYPE, column_types[9])
    self.__verify_primitive_type(TTypeId.TIMESTAMP_TYPE, column_types[10])
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[11])
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[12])
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the DECIMAL type.
    execute_statement_req.statement =\
        "SELECT d1,d5 FROM functional.decimal_tbl ORDER BY d1 LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch_at_most(execute_statement_resp.operationHandle,
                                 TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    # Verify the result schema is what we expect. The result has 2 columns, the
    # first is decimal(9,0) and the second is decimal(10,5)
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    column_types = metadata_resp.schema.columns
    assert len(column_types) == 2
    self.__verify_primitive_type(TTypeId.DECIMAL_TYPE, column_types[0])
    self.__verify_decimal_precision_scale(column_types[0], 9, 0)
    self.__verify_primitive_type(TTypeId.DECIMAL_TYPE, column_types[1])
    self.__verify_decimal_precision_scale(column_types[1], 10, 5)
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the CHAR/VARCHAR types.
    execute_statement_req.statement =\
        "SELECT * FROM functional.chars_tiny ORDER BY cs LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch_at_most(execute_statement_resp.operationHandle,
                                 TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    column_types = metadata_resp.schema.columns
    assert len(column_types) == 3
    self.__verify_primitive_type(TTypeId.CHAR_TYPE, column_types[0])
    self.__verify_char_max_len(column_types[0], 5)
    self.__verify_primitive_type(TTypeId.CHAR_TYPE, column_types[1])
    self.__verify_char_max_len(column_types[1], 140)
    self.__verify_primitive_type(TTypeId.VARCHAR_TYPE, column_types[2])
    self.__verify_char_max_len(column_types[2], 32)
    self.close(execute_statement_resp.operationHandle)

    # Verify the result metadata for the DATE type.
    execute_statement_req.statement =\
        "SELECT * FROM functional.date_tbl ORDER BY date_col LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch_at_most(execute_statement_resp.operationHandle,
                                 TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    column_types = metadata_resp.schema.columns
    assert len(column_types) == 3
    self.__verify_primitive_type(TTypeId.INT_TYPE, column_types[0])
    self.__verify_primitive_type(TTypeId.DATE_TYPE, column_types[1])
    self.__verify_primitive_type(TTypeId.DATE_TYPE, column_types[2])
    self.close(execute_statement_resp.operationHandle)

  def __query_and_fetch(self, query):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = query
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    # Do the actual fetch with a valid request.
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1024
    fetch_results_resp = self.fetch(fetch_results_req)

    return fetch_results_resp

  @needs_session()
  def test_alltypes_v6(self):
    """Test that a simple select statement works for all types"""
    fetch_results_resp = self.__query_and_fetch(
      "SELECT *, NULL from functional.alltypes ORDER BY id LIMIT 1")

    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert num_rows == 1
    assert result == \
        "0, True, 0, 0, 0, 0, 0.0, 0.0, 01/01/09, 0, 2009-01-01 00:00:00, 2009, 1, NULL\n"

    # Decimals
    fetch_results_resp = self.__query_and_fetch(
      "SELECT * from functional.decimal_tbl LIMIT 1")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == ("1234, 2222, 1.2345678900, "
                      "0.12345678900000000000000000000000000000, 12345.78900, 1\n")

    # VARCHAR
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('str' AS VARCHAR(3))")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == "str\n"

    # CHAR not inlined
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('car' AS CHAR(140))")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == "car" + (" " * 137) + "\n"

    # CHAR inlined
    fetch_results_resp = self.__query_and_fetch("SELECT CAST('car' AS CHAR(5))")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == "car  \n"

    # Date
    fetch_results_resp = self.__query_and_fetch(
      "SELECT * from functional.date_tbl ORDER BY date_col LIMIT 1")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert result == ("0, 0001-01-01, 0001-01-01\n")

  @needs_session()
  def test_show_partitions(self):
    """Regression test for IMPALA-1330"""
    for query in ["SHOW PARTITIONS functional.alltypes",
                  "SHOW TABLE STATS functional.alltypes"]:
      fetch_results_resp = self.__query_and_fetch(query)
      num_rows, result = \
          self.column_results_to_string(fetch_results_resp.results.columns)
      assert num_rows == 25
      # Match whether stats are computed or not
      assert re.match(
        r"2009, 1, -?\d+, -?\d+, \d*\.?\d+KB, NOT CACHED, NOT CACHED, TEXT", result) is not None

  @needs_session()
  def test_show_column_stats(self):
    fetch_results_resp = self.__query_and_fetch("SHOW COLUMN STATS functional.alltypes")
    num_rows, result = self.column_results_to_string(fetch_results_resp.results.columns)
    assert num_rows == 13
    assert re.match(r"id, INT, -?\d+, -?\d+, -?\d+, 4.0", result) is not None

  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  def test_execute_select_v1(self):
    """Test that a simple select statement works in the row-oriented protocol"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.fetch(fetch_results_req)

    assert len(fetch_results_resp.results.rows) == 1
    assert fetch_results_resp.results.startRowOffset == 0

    try:
      assert not fetch_results_resp.hasMoreRows
    except AssertionError:
      pytest.xfail("IMPALA-558")

  @needs_session()
  def test_select_null(self):
    """Regression test for IMPALA-1370, where NULL literals would appear as strings where
    they should be booleans"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "select null"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    # Check that the expected type is boolean (for compatibility with Hive, see also
    # IMPALA-914)
    get_result_metadata_req = TCLIService.TGetResultSetMetadataReq()
    get_result_metadata_req.operationHandle = execute_statement_resp.operationHandle
    get_result_metadata_resp = \
        self.hs2_client.GetResultSetMetadata(get_result_metadata_req)
    col = get_result_metadata_resp.schema.columns[0]
    assert col.typeDesc.types[0].primitiveEntry.type == TTypeId.BOOLEAN_TYPE

    # Check that the actual type is boolean
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1
    fetch_results_resp = self.fetch(fetch_results_req)
    assert fetch_results_resp.results.columns[0].boolVal is not None

    assert self.column_results_to_string(
      fetch_results_resp.results.columns) == (1, "NULL\n")

  @needs_session()
  def test_compute_stats(self):
    """Exercise the child query path"""
    self.__query_and_fetch("compute stats functional.alltypes")

  @needs_session()
  def test_invalid_secret(self):
    """Test that the FetchResults, GetResultSetMetadata and CloseOperation APIs validate
    the session secret."""
    execute_req = TCLIService.TExecuteStatementReq(
        self.session_handle, "select 'something something'")
    execute_resp = self.hs2_client.ExecuteStatement(execute_req)
    HS2TestSuite.check_response(execute_resp)

    good_handle = execute_resp.operationHandle
    bad_handle = create_op_handle_without_secret(good_handle)

    # Fetching and closing operations with an invalid handle should be a no-op, i.e.
    # the later operations with the good handle should succeed.
    HS2TestSuite.check_invalid_query(self.hs2_client.FetchResults(
        TCLIService.TFetchResultsReq(operationHandle=bad_handle, maxRows=1024)),
        expect_legacy_err=True)
    HS2TestSuite.check_invalid_query(self.hs2_client.GetResultSetMetadata(
        TCLIService.TGetResultSetMetadataReq(operationHandle=bad_handle)),
        expect_legacy_err=True)
    HS2TestSuite.check_invalid_query(self.hs2_client.CloseOperation(
        TCLIService.TCloseOperationReq(operationHandle=bad_handle)),
        expect_legacy_err=True)

    # Ensure that the good handle remained valid.
    HS2TestSuite.check_response(self.hs2_client.FetchResults(
        TCLIService.TFetchResultsReq(operationHandle=good_handle, maxRows=1024)))
    HS2TestSuite.check_response(self.hs2_client.GetResultSetMetadata(
        TCLIService.TGetResultSetMetadataReq(operationHandle=good_handle)))
    HS2TestSuite.check_response(self.hs2_client.CloseOperation(
        TCLIService.TCloseOperationReq(operationHandle=good_handle)))

  @needs_session()
  def test_fetch_timeout(self):
    """Test FETCH_ROWS_TIMEOUT_MS with default configs."""
    #self.__test_fetch_timeout()
    self.__test_fetch_materialization_timeout()

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_result_spooling_timeout(self):
    """Test FETCH_ROWS_TIMEOUT_MS with result spooling enabled, and test that the timeout
    applies when reading multiple RowBatches."""
    #self.__test_fetch_timeout()
    self.__test_fetch_materialization_timeout()

    # Validate that the timeout applies when reading multiple RowBatches.
    num_rows = 500
    statement = "select id from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'batch_size': '10',
                      'debug_action': '0:GETNEXT:DELAY',
                      'fetch_rows_timeout_ms': '500'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Issue a fetch request to read all rows, and validate that only a subset of the rows
    # are returned.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=num_rows))
    HS2TestSuite.check_response(fetch_results_resp)
    num_rows_fetched = self.get_num_rows(fetch_results_resp.results)
    assert num_rows_fetched > 0 and num_rows_fetched < num_rows
    assert fetch_results_resp.hasMoreRows

    self.__fetch_remaining(execute_statement_resp.operationHandle,
        num_rows - num_rows_fetched, statement)

  @needs_session()
  def test_fetch_timeout_multiple_batches(self):
    pass

  # is it worth having another test for the first timeout getting hit, and then validating
  # the timeout on the second batch

  @needs_session()
  def test_fetch_first_batch_timeout(self):
    """Test the query option FETCH_ROWS_TIMEOUT_MS by running a query with a DELAY
    DEBUG_ACTION and a low value for the fetch timeout. Validates that when the timeout
    is hit, 0 rows are returned."""
    num_rows = 10
    statement = "select bool_col from functional.alltypes where bool_col = sleep(250) " \
                "limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'debug_action': '0:GETNEXT:DELAY', 'fetch_rows_timeout_ms': '1000'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Assert that the first fetch request returns 0 rows.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=1024))
    # this is really testing the timeout on the first batch, but there should be on the
    # second batch as well? - can use debug actions in add in delays between getting the
    # first and second batch
    HS2TestSuite.check_response(fetch_results_resp,
        expected_status_code=TCLIService.TStatusCode.STILL_EXECUTING_STATUS)
    assert fetch_results_resp.hasMoreRows
    assert not fetch_results_resp.results

    get_operation_status_resp = self.wait_for_operation_state(
            execute_statement_resp.operationHandle,
            TCLIService.TOperationState.FINISHED_STATE)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Assert that all remaining rows can be fetched.
    self.__fetch_remaining(execute_statement_resp.operationHandle, num_rows, statement)

  def __test_fetch_materialization_timeout(self):
    """Test the query option FETCH_ROWS_TIMEOUT_MS applies to the time taken to
    materialize rows. Runs a query with a sleep() which is evaluated during
    materialization and validates the timeout is applied appropriately."""
    num_rows = 2
    statement = "select sleep(5000) from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'batch_size': '1', 'fetch_rows_timeout_ms': '2500'})
    HS2TestSuite.check_response(execute_statement_resp)

    get_operation_status_resp = self.wait_for_operation_state(
            execute_statement_resp.operationHandle,
            TCLIService.TOperationState.FINISHED_STATE)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Only one row should be returned because the timeout should be hit after
    # materializing the first row, but before materializing the second one.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=2))
    HS2TestSuite.check_response(fetch_results_resp)
    assert self.get_num_rows(fetch_results_resp.results) == 1

    # Assert that all remaining rows can be fetched.
    self.__fetch_remaining(execute_statement_resp.operationHandle, num_rows - 1,
        statement)

  def __fetch_remaining(self, op_handle, num_rows, statement):
    """Fetch the remaining rows in the given op_handle and validate that the number of
    rows returned matches the expected number of rows. If the op_handle does not return
    the expected number of rows within a timeout, an error is thrown."""
    # The timeout to wait for fetch requests to fetch all rows.
    timeout = 30

    start_time = time()
    num_fetched = 0

    # Fetch results until either the timeout is hit or all rows have been fetched.
    while num_fetched != num_rows and time() - start_time < timeout:
      sleep(0.5)
      fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
          operationHandle=op_handle, maxRows=1024))
      HS2TestSuite.check_response(fetch_results_resp)
      num_fetched += self.get_num_rows(fetch_results_resp.results)
    if num_fetched != num_rows:
      raise Timeout("Query {0} did not fetch all results within the timeout {1}"
                    .format(statement, timeout))
