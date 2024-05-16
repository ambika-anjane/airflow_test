# from api_dataposly import DataopslyRunJobOperator, DataopslyRunJobOperatorLink, Connection
# from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
# from datetime import datetime
# from airflow import DAG
# from airflow.models.xcom import XCom  # Import XCom to use it in get_link
# from airflow.operators.http_operator import SimpleHttpOperator
# import requests
# from airflow.models import Variable
# from airflow.operators.dummy_operator import DummyOperator
# import json

# # Define your DBT Cloud API credentials
# job_operator_link = DataopslyRunJobOperatorLink()

# # Define the schedule and start date for the DAG
# schedule_interval = "0 0 * * *"  # Run daily at midnight
# start_date = datetime(2022, 1, 1)

# # Define your Airflow DAG
# with DAG(
#     dag_id="run_status",
#     default_args={
#         "owner": "airflow",
#         "start_date": start_date,
#     },
#     schedule_interval=schedule_interval,
#     catchup=False,
# ) as dag:    

#     def trigger_job_2(**kwargs):

#         dataopsly_job_operator = DataopslyRunJobOperator(
#             task_id="run_dataopsly_job_2",
#             dataopsly_conn_id="http://host.docker.internal:8000/api/run-job",
#             job_id=3,  # Add the job_id parameter
#             dag=dag,
#         )
        
#         # Trigger the job run using the operator's trigger_job_run method
#         response = dataopsly_job_operator.trigger_job_run(
#             job_id=3,
#             dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
#             token="09e14c195cf9479788cf4db6740434a691f8051a"
#         )
        
#         # Construct the data to be sent in the request body as JSON
#         data = {"job_id": 3}  # Include the job_id in the request body
        
#         # Make a POST request with the JSON data
#         response = requests.post(
#             dataopsly_job_operator.dataopsly_conn_id,
#             json=data,
#             headers={"Authorization": "Token 09e14c195cf9479788cf4db6740434a691f8051a"}
#         )
        
#         # Extract relevant data from the response
#         response_data = {
#             "status_code": response.status_code,
#             "content": response.content.decode('utf-8') if response.content else None
#         }

#         # Return the response data for logging or further processing
#         # return response_data
#         return {"status_code": 200}

#     def list_job_run(**kwargs):
#         # Instantiate DataopslyRunJobOperator with necessary parameters
#         dataopsly_job_operator = DataopslyRunJobOperator(
#             task_id="list_job_run_task",
#             dataopsly_conn_id="http://host.docker.internal:8000/api/run",
#             job_id=2,
#             # run_id="c0ca91ce-a193-45d6-b057-9d1cc3eef953",
#             dag=dag
#         )
        
#         # Trigger the job run using the operator's trigger_job_run method
#         response = dataopsly_job_operator.list_job_runs(
#             job_id=2,
#             # run_id="c0ca91ce-a193-45d6-b057-9d1cc3eef953",
#             dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
#             token="09e14c195cf9479788cf4db6740434a691f8051a"
#         )

#         print("Response",response)
#         endpoint_url = dataopsly_job_operator.dataopsly_conn_id
#         headers = {
#          "Authorization": "Token 09e14c195cf9479788cf4db6740434a691f8051a",
#          "Content-Type": "application/json"  # Assuming JSON payload
# }
#         response = requests.post(endpoint_url,headers=headers)
#         print("Response:", response)
#         status_code = response.status_code
#         response_content = response.content.decode('utf-8') if response.content else None
#         print("Status Code:", status_code)
#         print("Response Content:", response_content)
#         json_content = json.loads(response_content)
#         if "results" in json_content:
#            results = json_content["results"]
#            for result in results:
#             job_id = result.get("job_id")
#             run_id = result.get("run_id")
#             job_status = result.get("status")
#             print(f"Job ID: {job_id}, Run_id: {run_id}, Status: {job_status}, status_code: {status_code}")
#             if job_id == 2 and job_status == "Completed":
#                 print("Triggering job_2...")
#                 trigger_job_2()
#             else:
#                 print("Job ID is not in  Completed state, not triggering job_2.")
#                 return "skip_trigger_job_task_2"
#         else:
#             print("No results found in the response.")

        
        
        
#         # Check if the response is successful
#         # if response.status_code == 200:
#         #     # Extract status from the response data
#         #     response_data = response.json()
#         #     job_run_status = response_data.get("status")
            
#         #     # Log the job run status
#         #     dag.log.info(f"Job run status: {job_run_status}")
            
#         #     # Check if the job run status is Completed, Error, or Cancelled
#         #     if job_run_status in ["Completed", "Error", "Cancelled"]:
#         #         # Return the job run status
#         #         return job_run_status
#         #     else:
#         #         # Log warning if status is unexpected
#         #         dag.log.warning(f"Unexpected job run status: {job_run_status}")
#         #         return None
#         # else:
#         #     # Log error if response is not successful
#         #     dag.log.error(f"Error retrieving job run status. Status code: {response.status_code}")
#         #     return None

#     list_job_runs_task = PythonOperator(
#         task_id="list_run",
#         python_callable=list_job_run,
#         dag=dag,
#     )

#     trigger_job_task_2 = PythonOperator(
#         task_id="job_trigger_task_2",
#         python_callable=trigger_job_2,
#         dag=dag,   
#     )

#     skip_trigger_job_task_2 = DummyOperator(
#         task_id="skip_trigger_job_task_2",
#         dag=dag,
#     )

    


    
# #     # Set task dependencies
#     list_job_runs_task >> [trigger_job_task_2, skip_trigger_job_task_2]


   
from api_dataposly import DataopslyRunJobOperator, DataopslyRunJobOperatorLink, Connection
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from datetime import datetime
from airflow import DAG
from airflow.models.xcom import XCom  # Import XCom to use it in get_link
from airflow.operators.http_operator import SimpleHttpOperator
import requests
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
import json

# Define your DBT Cloud API credentials
job_operator_link = DataopslyRunJobOperatorLink()

# Define the schedule and start date for the DAG
schedule_interval = "0 0 * * *"  # Run daily at midnight
start_date = datetime(2022, 1, 1)

# Define your Airflow DAG
with DAG(
    dag_id="time_delay",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:    

    def trigger_job_2(**kwargs):

        dataopsly_job_operator = DataopslyRunJobOperator(
            task_id="run_dataopsly_job_2",
            dataopsly_conn_id="http://host.docker.internal:8000/api/run-job",
            job_id=9,  # Add the job_id parameter
            dag=dag,
        )
        
        # Trigger the job run using the operator's trigger_job_run method
        response = dataopsly_job_operator.trigger_job_run(
            job_id=9,
            dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
            token="18f3aa5fe6e5f4bb9728b3d02b4f2008fdbee387"
        )
        
        # Construct the data to be sent in the request body as JSON
        data = {"job_id": 9}  # Include the job_id in the request body
        
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
        # return response_data
        return {"status_code": 200}

    import requests
import json

def list_job_run(**kwargs):
    # Instantiate DataopslyRunJobOperator with necessary parameters
    dataopsly_job_operator = DataopslyRunJobOperator(
        task_id="list_job_run_task",
        dataopsly_conn_id="http://host.docker.internal:8000/api/run",
        job_id=8,
        dag=dag
    )
    
    # Trigger the job run using the operator's trigger_job_run method
    response = dataopsly_job_operator.list_job_runs(
        job_id=8,
        dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
        token="18f3aa5fe6e5f4bb9728b3d02b4f2008fdbee387"
    )

    print("Response", response)
    endpoint_url = dataopsly_job_operator.dataopsly_conn_id
    headers = {
        "Authorization": "Token 18f3aa5fe6e5f4bb9728b3d02b4f2008fdbee387",
        "Content-Type": "application/json"  # Assuming JSON payload
    }
    response = requests.post(endpoint_url, headers=headers)
    print("Response:", response)
    status_code = response.status_code
    response_content = response.content.decode('utf-8') if response.content else None
    print("Status Code:", status_code)
    print("Response Content:", response_content)
    json_content = json.loads(response_content)
    
    if "results" in json_content:
        results = json_content["results"]
        for result in results:
            job_id = result.get("job_id")
            run_id = result.get("run_id")
            job_status = result.get("status")
            print(f"Job ID: {job_id}, Run_id: {run_id}, Status: {job_status}, status_code: {status_code}")
            if job_id == 8:
              if job_status == "completed":
                trigger_job_2()
              elif job_status == "Cancelled":
                print("Error state and Cancelled State")
              elif job_status == "Error":
               print("Error state and Cancelled State")
           
              else:
                print("Job ID is not in Completed, Cancelled, or Error state.", job_id, job_status)
            else:
                print("No need to trigger")
            if job_id == 9:
              if job_status == "completed":
                trigger_job_2()
              elif job_status == "Cancelled":
                print("rror state and Cancelled State")
              elif job_status == "Error":
               print("Error state and Cancelled State")
              
              else:
                print("Job ID is not in Completed, Cancelled, or Error state.", job_id, job_status)
            
            else:
                print("No need to trigger")
            
           
    else:
        print("No results found in the response.")

        
        
        # Check if the response is successful
        # if response.status_code == 200:
        #     # Extract status from the response data
        #     response_data = response.json()
        #     job_run_status = response_data.get("status")
            
        #     # Log the job run status
        #     dag.log.info(f"Job run status: {job_run_status}")
            
        #     # Check if the job run status is Completed, Error, or Cancelled
        #     if job_run_status in ["Completed", "Error", "Cancelled"]:
        #         # Return the job run status
        #         return job_run_status
        #     else:
        #         # Log warning if status is unexpected
        #         dag.log.warning(f"Unexpected job run status: {job_run_status}")
        #         return None
        # else:
        #     # Log error if response is not successful
        #     dag.log.error(f"Error retrieving job run status. Status code: {response.status_code}")
        #     return None

list_job_runs_task = PythonOperator(
        task_id="list_run",
        python_callable=list_job_run,
        dag=dag,
    )

trigger_job_task_2 = PythonOperator(
        task_id="job_trigger_task_2",
        python_callable=trigger_job_2,
        dag=dag,      # Set an execution timeout of 1 minute for job_trigger_task_2
    )




    
#     # Set task dependencies
list_job_runs_task 

   
