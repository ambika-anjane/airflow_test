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
from __future__ import annotations
from flask import redirect
from enum import Enum



import json
import requests
import time
import warnings
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from dataopsly_hooks import DataopslyHook,Connection
from enum import Enum

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.context import Context


class DataopslyRunJobOperatorLink(BaseOperatorLink):
    """Allows users to monitor the triggered job run directly in dbt Cloud."""

    name = "Monitor Job Run"

    def get_link(self, operator: BaseOperator, *, ti_key=None):
        return XCom.get_value(key="job_run_url", ti_key=ti_key)


class DataopslyRunJobOperator(BaseOperator):
    """
    Executes a dbt Cloud job.

    :param dataopsly_conn_id: The connection ID for connecting to dbt Cloud.
    :param job_id: The ID of a dbt Cloud job.
    :param login_name: The login name for authentication.
    :param password: The password for authentication.
    :param trigger_reason: Optional. Description of the reason to trigger the job.
    :param wait_for_termination: Flag to wait on a job run's termination.
    :param timeout: Time in seconds to wait for a job run to reach a terminal status for non-asynchronous waits.
    :param check_interval: Time in seconds to check on a job run's status for non-asynchronous waits.
    :param reuse_existing_run: Flag to determine whether to reuse existing non terminal job run.
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields = (
        "dataopsly_conn_id",
        "job_id",
        
    )

    def __init__(
        self,
        *,
        dataopsly_conn_id: str,
        job_id: int,
        # run_id: int,
        trigger_reason: str = None,
        wait_for_termination: bool = True,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        reuse_existing_run: bool = False,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataopsly_conn_id = dataopsly_conn_id
        self.job_id = job_id
        # self.run_id = run_id
        self.trigger_reason = trigger_reason
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.reuse_existing_run = reuse_existing_run
        self.deferrable = deferrable

    def execute(self, context: Context):
        trigger_reason = (
            f"Triggered via Apache Airflow by task {self.task_id!r} in the {self.dag_id} DAG."
        )

          # Log the job ID after triggering the job
        # self.log.info(f"Job ID {self.job_id} has been triggered.")
        # connection_id = context["task_instance"].xcom_pull(task_ids="run_dataopsly")

        # # Log the connection ID
        # self.log.info(f"Connection ID used in Airflow: {connection_id}")
        self.log.info(f"Job ID {self.job_id} has been triggered.") 
        self.log.info(f"Connection ID used in Airflow: {self.dataopsly_conn_id}")
        
        # success_message = f"Dataopsly job triggered successfully! Redirecting to {self.redirect_url}"
        # self.log.info(success_message)

        # # Return a dictionary containing the success message and redirect URL
        # return {"success_message": success_message, "redirect_url": self.redirect_url}
    
    def connection(self) -> Connection:
        _connection = self.get_conn(self.dataopsly_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dataoplsy.")

        return _connection
    
    def get_conn(self, *args, **kwargs) -> Session:
        session = Session()
        session.auth = self.auth_type(self.connection.password)

        return session
        
        # Rest of the execution logic here
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


    # Other methods and properties omitted for brevity
    def trigger_job_run(self, dataopsly_conn_id, job_id, token):
        # Construct the API endpoint URL
        endpoint_url = f"{dataopsly_conn_id}"
        print("END_POINT_URL",endpoint_url)

        headers = {
        'Authorization': f'Token {token}',
        'Content-Type': 'application/json',
    }

        # Make a POST request to trigger the job run with run_id and cause as parameters
        response = requests.post(endpoint_url, headers=headers)

        if response.status_code == 200:
    # Extract the JSON data from the response
          response_data = response.json()
    
    # Access the 'id' field (assuming 'id' is the key for 'run_id' in the response)
          run_id = response_data.get("id")
          print("RUN_ID",run_id)

    # Check if 'run_id' is available
          if run_id:
        # Log the 'run_id'
           self.log.info(f"Run ID: {run_id}")
          else:
           self.log.warning("Run ID not found in the response.")
        else:
    # Log an error if the request was not successful
           self.log.error(f"Failed to retrieve run ID. Status code: {response.status_code}")


        # Log the response status and content
        self.log.info(f"Request URL: {endpoint_url}")
        self.log.info(f"Request Headers: {headers}")
        self.log.info(f"Response status code: {response.status_code}")
        self.log.info(f"Response content: {response.content}")

        # Return the response for logging or further processing
        return response
  
    def list_projects(self, dataopsly_conn_id, job_id, token):
        # Construct the API endpoint URL
        endpoint_url = f"{dataopsly_conn_id}"
        print("END_POINT_URL",endpoint_url)

        headers = {
        'Authorization': f'Token {token}',
        'Content-Type': 'application/json',
    }
        
        response = requests.post(endpoint_url, headers=headers)

        self.log.info(f"Request URL: {endpoint_url}")
        self.log.info(f"Request Headers: {headers}")
        self.log.info(f"Response status code: {response.status_code}")
        self.log.info(f"Response content: {response.content}")



        return response
    
    def list_job_runs(self, dataopsly_conn_id, job_id, token):
        # Construct the API endpoint URL
        endpoint_url = f"{dataopsly_conn_id}"
        print("END_POINT_URL",endpoint_url)

        headers = {
        'Authorization': f'Token {token}',
        'Content-Type': 'application/json',
    }
        
        response = requests.post(endpoint_url, headers=headers)

        self.log.info(f"Request URL: {endpoint_url}")
        self.log.info(f"Request Headers: {headers}")
        self.log.info(f"Response status code: {response.status_code}")
        self.log.info(f"Response content: {response.content}")

        response_content = self.log.info(f"Response content: {response.content}")
        if response.status_code == 200:
         response_content = response.content.decode('utf-8')
         self.log.info(f"Response content: {response_content}")
         json_content = json.loads(response_content)
         if "results" in json_content:
           results = json_content["results"]
           for result in results:
            job_id = result.get("job_id")
            run_id = result.get("run_id")
            job_status = result.get("status")
            status_code = result.get("status_code")
            print(f"Job ID: {job_id}, Run_id: {run_id}, Status: {job_status}, status_code: {status_code}")
         else:
            self.log.warning("No results found in the response.")
        else:
            self.log.error(f"Request failed with status code: {response.status_code}")

        # job_id = response_content.get("job_id") if response_content else None
# 
    # Log job ID and status
        self.log.info(f"Job ID: {job_id}")
        self.log.info(f"Run ID: {run_id}")

    # Return job ID, status code, and content
        return job_id, response.status_code, response.content


        return response
    
    def cancel_job(self, dataopsly_conn_id, job_id, run_id, token):
        # Construct the API endpoint URL
        endpoint_url = f"{dataopsly_conn_id}"
        print("END_POINT_URL",endpoint_url)
        print("RUN_ID",run_id)

        headers = {
        'Authorization': f'Token {token}',
        'Content-Type': 'application/json',
    }
        
        response = requests.post(endpoint_url, headers=headers)

        self.log.info(f"Request URL: {endpoint_url}")
        self.log.info(f"Request Headers: {headers}")
        self.log.info(f"Response status code: {response.status_code}")
        self.log.info(f"Response content: {response.content}")


        return response
    
    
    
    
class DataopslyJobRunStatus(Enum):
    """dbt Cloud Job statuses."""

    QUEUED = 1
 
    STARTING = 2

    RUNNING = 3

    SUCCESS = 10

    ERROR = 20

    CANCELLED = 30

    TERMINAL_STATUSES = (SUCCESS, ERROR, CANCELLED)

    def get_job_run(self, dataopsly_conn_id, run_id):
        """
        Retrieves metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{dataopsly_conn_id}"

        )
    
    def get_job_run_status(self, dataopsly_conn_id, job_id, run_id, token):
        """
        Retrieves the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The status of a dbt Cloud job run.
        """
        endpoint_url = f"{dataopsly_conn_id}"
        print("END_POINT_URL",endpoint_url)
        # self.log.info("Getting the status of job run %s.", str(run_id))

        job_run = self.get_job_run(dataopsly_conn_id=dataopsly_conn_id, run_id=run_id)
        job_run_status = job_run.json()["status"]

        # self.log.info(
        #     "Current status of job run %s: %s", str(run_id), DataopslyJobRunStatus(job_run_status).name
        # )

        return job_run_status