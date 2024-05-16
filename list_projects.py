# Define your Airflow DAG

from api_dataposly import DataopslyRunJobOperator, DataopslyRunJobOperatorLink, Connection
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from datetime import datetime
from airflow import DAG
from airflow.models.xcom import XCom  # Import XCom to use it in get_link
from airflow.operators.http_operator import SimpleHttpOperator
import requests
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

# Define your DBT Cloud API credentials
job_operator_link = DataopslyRunJobOperatorLink()

# Define the schedule and start date for the DAG
schedule_interval = "0 0 * * *"  # Run daily at midnight
start_date = datetime(2022, 1, 1)



with DAG(
    dag_id="list_projects",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:    

    def list_project(**kwargs):
        # Instantiate DataopslyRunJobOperator with necessary parameters
        dataopsly_job_operator = DataopslyRunJobOperator(
            task_id="list_job",
            dataopsly_conn_id="http://host.docker.internal:8000/api/project",
            job_id=2,
            dag=dag
        )
        
        # Trigger the job run using the operator's trigger_job_run method
        response = dataopsly_job_operator.list_projects(
            job_id=2,
            dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
            token="09e14c195cf9479788cf4db6740434a691f8051a"
        )
        
        # Construct the data to be sent in the request body as JSON
        data = {"job_id": 2}  # Include the job_id in the request body
        
        # Make a POST request with the JSON data
        response = requests.post(
            dataopsly_job_operator.dataopsly_conn_id,
            json=data,
            headers={"Authorization": "Token 09e14c195cf9479788cf4db6740434a691f8051a"}
        )
        
        # Extract relevant data from the response
        response_data = {
            "status_code": response.status_code,
            "content": response.content.decode('utf-8') if response.content else None
        }

        # Return the response data for logging or further processing
        return response_data
    
    

    list_job_task = PythonOperator(
        task_id="list_project_job",
        python_callable=list_project,
        dag=dag,
    )



    list_job_task 