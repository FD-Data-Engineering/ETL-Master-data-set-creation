from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import AirflowException
import time
import requests
import json
#from Gamma_Loan_Visualisation import popup_html_ber

###############################################
# Parameters
###############################################
dtStr = datetime.today().strftime('%Y%m%d')
now = datetime.now()



def getCOSconfig():
  COS_ENDPOINT = "https://s3.direct.eu-de.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
  COS_API_KEY_ID = "trM1JWYO_DAjtqAM58O2IT0iCIfPhV9I2U3GV-yGeypu" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
  COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/f2d7386c3c18406b9e2eed413aa7d007:ef1bfb54-2d01-45a0-bd5e-8f32a506cb9f::" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"
  auth_endpoint = 'https://iam.bluemix.net/oidc/token'
  # Create resource
  cos = ibm_boto3.resource("s3",
      ibm_api_key_id=COS_API_KEY_ID,
      ibm_service_instance_id=COS_INSTANCE_CRN,
      ibm_auth_endpoint=auth_endpoint,
      config=Config(signature_version="oauth"),
      endpoint_url=COS_ENDPOINT
  )
  return cos
  
def getBucketContents(cos,bucket_name,filterArgs):
    itemList = []
    print("Retrieving bucket contents from:{0}".format(bucket_name))
    try:
        arr = []
        files = cos.Bucket(bucket_name).objects.all()
        for file in files:
            arr.append("{0}".format(file.key, file.size))
    
        itemList = [item for item in arr if all(filters in item for filters in filterArgs)]
    
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))
    return itemList
  
def copy_file(bucket,srcFile,tgt):
    copy_source = {
    'Bucket': bucket,
    'Key': srcFile
    }
    bucket = getCOSconfig().Bucket(bucket)
    obj = bucket.Object(tgt)
    obj.copy(copy_source)
    
def getSrcData(tgtDate):
    itemList = getBucketContents("ingestedgammadata",["20230217"])
    for item in itemList:
        copy_file("ingestedgammadata",item,item.replace("20230217",tgtDate))
    
 
        

###############################################
# DAG Definition
###############################################  
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="SF-Gamma-data-copy", 
        description="DAG to copy data to ingestion layer for current date",
        default_args=default_args, 
        schedule_interval=@daily
        catchup=False
    )

start_load_gamma = DummyOperator(task_id="start_load_gamma", dag=dag)


copy_src_files = PythonOperator(task_id='copy_src_files', python_callable=copy_file, op_kwargs={"tgtDate":dtStr}, dag=dag)
          

start_load_gamma >> copy_src_files

