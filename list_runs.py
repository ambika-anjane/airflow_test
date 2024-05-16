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
    dag_id="list_runs",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:    


    def list_job_run(**kwargs):
        # Instantiate DataopslyRunJobOperator with necessary parameters
        dataopsly_job_operator = DataopslyRunJobOperator(
            task_id="list_job_run_task",
            dataopsly_conn_id="http://host.docker.internal:8000/api/run",
            job_id=4,
            dag=dag
        )
        
        # Trigger the job run using the operator's trigger_job_run method
        response = dataopsly_job_operator.list_job_runs(
            job_id=4,
            dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
            token="18f3aa5fe6e5f4bb9728b3d02b4f2008fdbee387"
        )
        
        # Construct the data to be sent in the request body as JSON
        data = {"job_id": 4}  # Include the job_id in the request body
        
        # Make a POST request with the JSON data
        response = requests.post(
            dataopsly_job_operator.dataopsly_conn_id,
            json=data,
            headers={"Authorization": "Token 18f3aa5fe6e5f4bb9728b3d02b4f2008fdbee387"}
        )
        
        # Extract relevant data from the response
        response_data = {
            "status_code": response.status_code,
            "content": response.content.decode('utf-8') if response.content else None
        }

        # Return the response data for logging or further processing
        return response_data
    
    list_job_task = PythonOperator(
        task_id="list_run_job",
        python_callable=list_job_run,
        dag=dag,
    )



    list_job_task 
