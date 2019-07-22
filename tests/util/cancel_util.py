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

import threading
from time import sleep
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite


def cancel_query_validate_state(client, query, exec_option, table_format, cancel_delay,
    join_before_close=False):
  if exec_option: client.set_configuration(exec_option)
  if table_format: ImpalaTestSuite.change_database(client, table_format)
  handle = client.execute_async(query)

  def fetch_results():
    threading.current_thread().fetch_results_error = None
    threading.current_thread().query_profile = None
    try:
      new_client = ImpalaTestSuite.create_impala_client()
      new_client.fetch(query, handle)
    except ImpalaBeeswaxException as e:
      threading.current_thread().fetch_results_error = e

  thread = threading.Thread(target=fetch_results)
  thread.start()

  sleep(cancel_delay)
  assert client.get_state(handle) != client.QUERY_STATES['EXCEPTION']
  cancel_result = client.cancel(handle)
  assert cancel_result.status_code == 0,\
      'Unexpected status code from cancel request: %s' % cancel_result

  if join_before_close:
    thread.join()

  close_error = None
  try:
    client.close_query(handle)
  except ImpalaBeeswaxException as e:
    close_error = e

  # Before accessing fetch_results_error we need to join the fetch thread
  thread.join()

  if thread.fetch_results_error is None:
    # If the fetch rpc didn't result in CANCELLED (and auto-close the query) then
    # the close rpc should have succeeded.
    assert close_error is None
  elif close_error is None:
    # If the close rpc succeeded, then the fetch rpc should have either succeeded,
    # failed with 'Cancelled' or failed with 'Invalid query handle' (if the close
    # rpc occured before the fetch rpc).
    if thread.fetch_results_error is not None:
      assert 'Cancelled' in str(thread.fetch_results_error) or \
        ('Invalid query handle' in str(thread.fetch_results_error)
         and not join_before_close)
  else:
    # If the close rpc encountered an exception, then it must be due to fetch
    # noticing the cancellation and doing the auto-close.
    assert 'Invalid or unknown query handle' in str(close_error)
    assert 'Cancelled' in str(thread.fetch_results_error)

  # TODO: Add some additional verification to check to make sure the query was
  # actually canceled
