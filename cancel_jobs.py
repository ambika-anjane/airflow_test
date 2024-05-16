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


# Define your Airflow DAG
with DAG(
    dag_id="cancel_jobs",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:
    
 

   def cancel_jobs(**kwargs):

        dataopsly_job_operator = DataopslyRunJobOperator(
            task_id="run_cancel_job",
            dataopsly_conn_id="http://host.docker.internal:8000/api/cancel-job",
            job_id=1,
            dag=dag,
        )
        
        # Trigger the job run using the operator's trigger_job_run method
        response = dataopsly_job_operator.cancel_job(
            job_id=1,
            run_id="7c6de6b1-a4ff-45a8-952c-276a822b9579",
            dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
            token="09e14c195cf9479788cf4db6740434a691f8051a"
        )
        
        # Construct the data to be sent in the request body as JSON
        data = {"job_id":1, "run_id": "7c6de6b1-a4ff-45a8-952c-276a822b9579"
} # Include the job_id in the request body
        
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
        # return response_data
        return {"status_code": 200}
    
    
    
cancel_job_task_2 = PythonOperator(
        task_id="cancel_job_1",
        python_callable = cancel_jobs,
        dag=dag,
    )
    
    
    # Set task dependencies
    # trigger_job_task_1 >> check_job_1_status_task >> trigger_job_task_2
    # check_job_1_status_task >> end_workflow_task

cancel_job_task_2
    # list_job_task = PythonOperator(
    #     task_id="list_project_job",
    #     python_callable=list_project,
    #     dag=dag,
    # )

    # list_job_runs_task = PythonOperator(
    #     task_id="list_run",
    #     python_callable=list_job_run,
    #     dag=dag,
    # )


    # # Define task dependencies
    # list_job_task >> list_job_runs_task 
    # trigger_job_task_1 >> 
   


  