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

import json
import sys
import time
from enum import Enum
from functools import wraps
from inspect import signature
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from requests import PreparedRequest, Session
from requests.auth import AuthBase
from requests.models import Response

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook
from airflow.typing_compat import TypedDict

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property



def fallback_to_default_account(func: Callable) -> Callable:
    """
    Decorator which provides a fallback value for ``account_id``. If the ``account_id`` is None or not passed
    to the decorated function, the value will be taken from the configured dbt Cloud Airflow Connection.
    """
    sig = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        bound_args = sig.bind(*args, **kwargs)

        # Check if ``account_id`` was not included in the function signature or, if it is, the value is not
        # provided.
        if bound_args.arguments.get("account_id") is None:
            self = args[0]
            default_account_id = self.connection.login
            if not default_account_id:
                raise AirflowException("Could not determine the dbt Cloud account.")

            bound_args.arguments["account_id"] = int(default_account_id)

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper



def _get_provider_info() -> Tuple[str, str]:
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    package_name = manager.hooks[DataopslyHook.conn_type].package_name  # type: ignore[union-attr]
    provider = manager.providers[package_name]

    return package_name, provider.version



class TokenAuth(AuthBase):
    """Helper class for Auth when executing requests."""

    def __init__(self, token: str) -> None:
        self.token = token


def __call__(self, request: PreparedRequest) -> PreparedRequest:
        package_name, provider_version = _get_provider_info()
        request.headers["User-Agent"] = f"{package_name}-v{provider_version}"
        request.headers["Content-Type"] = "application/json"
        request.headers["Authorization"] = f"Token {self.token}"

        return request







class DataopslyHook(HttpHook):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """


    conn_name_attr = "dataopsly_conn_id"


    default_conn_name = "dataopsly_conn_id"
    
    conn_type = "http"
    
    hook_name = "dataopsly"


#     @staticmethod

# [docs]    def get_ui_field_behaviour() -> Dict[str, Any]:
#         """Builds custom field behavior for the dbt Cloud connection form in the Airflow UI."""
#         return {
#             "hidden_fields": ["host", "port", "extra"],
#             "relabeling": {"login": "Account ID", "password": "API Token", "schema": "Tenant"},
#             "placeholders": {"schema": "Defaults to 'cloud'."},

#         }

    def __init__(self, dataopsly_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth)
        self.dataopsly_conn_id = dataopsly_conn_id
        # tenant = self.connection.schema if self.connection.schema else 'dataopsly'

        self.base_url = f"https://localohost/8000/"

    @cached_property
    def connection(self) -> Connection:
        _connection = self.get_connection(self.dataopsly_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dataoplsy.")

        return _connection
    

    def get_conn(self, *args, **kwargs) -> Session:
        session = Session()
        session.auth = self.auth_type(self.connection.password)

        return session

    def _run_and_get_response(
        self,
        method: str = "GET",
        endpoint: Optional[str] = None,
        payload: Union[str, Dict[str, Any], None] = None,
        paginate: bool = False,
    ) -> Any:
        self.method = method

        if paginate:
            if isinstance(payload, str):
                raise ValueError("Payload cannot be a string to paginate a response.")

            if endpoint:
                return self._paginate(endpoint=endpoint, payload=payload)
            else:
                raise ValueError("An endpoint is needed to paginate a response.")

        return self.run(endpoint=endpoint, data=payload)



    def trigger_job_run(
        self,
        dataopsly_conn_id: int,
        job_id: int,
        cause: str,
      
    ) -> Response:
        """
        Triggers a run of a dbt Cloud job.

        :param job_id: The ID of a dbt Cloud job.
        :param cause: Description of the reason to trigger the job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param steps_override: Optional. List of dbt commands to execute when triggering the job
            instead of those configured in dbt Cloud.
        :param schema_override: Optional. Override the destination schema in the configured target for this
            job.
        :param additional_run_config: Optional. Any additional parameters that should be included in the API
            request when triggering the job.
        :return: The request response.
        """
      

        return self._run_and_get_response(
            method="POST",
            endpoint=f"{dataopsly_conn_id}/jobs/{job_id}/run/",

        )

def test_connection(self) -> Tuple[bool, str]:
        """Test dbt Cloud connection."""
        try:
            self._run_and_get_response()
            return True, "Successfully connected to dbt Cloud."
        except Exception as e:
            return False, str(e)