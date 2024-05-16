# from api_dataposly import DataopslyRunJobOperator, DataopslyRunJobOperatorLink, Connection
# from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
# from datetime import datetime
# from airflow import DAG
# from airflow.models.xcom import XCom  # Import XCom to use it in get_link
# from airflow.operators.http_operator import SimpleHttpOperator
# import requests
# from airflow.models import Variable
# from airflow.operators.dummy_operator import DummyOperator
# from datetime import datetime, timedelta
# from airflow.utils.trigger_rule import TriggerRule
# from enum import Enum






# # Define your DBT Cloud API credentials
# job_operator_link = DataopslyRunJobOperatorLink()

# # Define the schedule and start date for the DAG
# schedule_interval = "0 0 * * *"  # Run daily at midnight
# start_date = datetime(2022, 1, 1)



# # Define your Airflow DAG
# with DAG(
#     dag_id="trigger_jobs",
#     default_args={
#         "owner": "airflow",
#         "start_date": start_date,
#     },
#     schedule_interval=schedule_interval,
#     catchup=False,
# ) as dag:
    
 

#     def trigger_job_1(**kwargs):
#         # Instantiate DataopslyRunJobOperator with necessary parameters
#         dataopsly_job_operator = DataopslyRunJobOperator(
#             task_id="run_dataopsly_job",
#             dataopsly_conn_id="http://host.docker.internal:8000/api/run-job",
#             job_id=1,  # Add the job_id parameter
#             dag=dag,
#         )
        
#         # Trigger the job run using the operator's trigger_job_run method
#         response = dataopsly_job_operator.trigger_job_run(
#             job_id=1,
#             dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
#             token="4721cfe68d1748c92426344636e98462b84ca35c"
#         )

        
        
#         # Construct the data to be sent in the request body as JSON
#         data = {"job_id": 1}  # Include the job_id in the request body
        
#         # Make a POST request with the JSON data
#         response = requests.post(
#             dataopsly_job_operator.dataopsly_conn_id,
#             json=data,
#             headers={"Authorization": "Token 4721cfe68d1748c92426344636e98462b84ca35c"}
#         )
        
#         # Extract relevant data from the response
#         response_data = {
#             "status_code": response.status_code,
#             "content": response.content.decode('utf-8') if response.content else None
#         }

#         # Return the response data for logging or further processing
#         # return response_data
#         return {"status_code": 200}
    
#     def trigger_job_2(**kwargs):

#         dataopsly_job_operator = DataopslyRunJobOperator(
#             task_id="run_dataopsly_job_2",
#             dataopsly_conn_id="http://host.docker.internal:8000/api/run-job",
#             job_id=2,  # Add the job_id parameter
#             dag=dag,
#         )
        
#         # Trigger the job run using the operator's trigger_job_run method
#         response = dataopsly_job_operator.trigger_job_run(
#             job_id=2,
#             dataopsly_conn_id=dataopsly_job_operator.dataopsly_conn_id,
#             token="4721cfe68d1748c92426344636e98462b84ca35c"
#         )
        
#         # Construct the data to be sent in the request body as JSON
#         data = {"job_id": 2}  # Include the job_id in the request body
        
#         # Make a POST request with the JSON data
#         response = requests.post(
#             dataopsly_job_operator.dataopsly_conn_id,
#             json=data,
#             headers={"Authorization": "Token 4721cfe68d1748c92426344636e98462b84ca35c"}
#         )
        
#         # Extract relevant data from the response
#         response_data = {
#             "status_code": response.status_code,
#             "content": response.content.decode('utf-8') if response.content else None
#         }

#         # Return the response data for logging or further processing
#         # return response_data
#         return {"status_code": 200}
    
    
    
    
     
     


    
    

#     # Create a task to trigger the Dataopsly job
#     trigger_job_task_1 = PythonOperator(
#         task_id="trigger_dataopsly_job_1",
#         python_callable=trigger_job_1,
#         dag=dag,
#     )

#     trigger_job_task_2 = PythonOperator(
#         task_id="job_trigger_task_2",
#         python_callable=trigger_job_2,
#         dag=dag,
#         execution_timeout=timedelta(minutes=1),  # Set an execution timeout of 1 minute for job_trigger_task_2
#     )


    
#     # Set task dependencies
#     trigger_job_task_1 >> trigger_job_task_2

   

    
    
    




  
   


   


  