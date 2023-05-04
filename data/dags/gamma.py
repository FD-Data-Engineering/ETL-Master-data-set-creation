from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
#from Gamma_Loan_Visualisation import popup_html_ber

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
dtStr = datetime.today().strftime('%Y%m%d')

ecad_file = "/usr/local/spark/resources/data/raw/gamma/dt="+dtStr+"/ecad_20221216.csv"
er_file = "/usr/local/spark/resources/data/raw/gamma/dt="+dtStr+"/energy_rating_data.csv"
lb_file = "/usr/local/spark/resources/data/raw/gamma/dt="+dtStr+"/Loan_book_4.csv"
flood_file = "/usr/local/spark/resources/data/raw/gamma/dt="+dtStr+"/roi_202111_gamma_eircode_ta.csv"
#final_file = "/usr/local/spark/resources/data/published/enriched_loan_book/dt=20230112"
#postgres_db = "jdbc:postgresql://postgres/test"
#postgres_user = "test"
#postgres_pwd = "postgres"

###############################################
# DAG Definition
###############################################
now = datetime.now()

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
        dag_id="gamma-data", 
        description="This DAG is a sample of integration between Spark and DB. It reads CSV files, load them into a Postgres DB and then read them from the same Postgres DB.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start_load_gamma = DummyOperator(task_id="start_load_gamma", dag=dag)

spark_job_load_gamma = SparkSubmitOperator(
    task_id="spark_job_load_gamma",
    application="/usr/local/spark/app/mergegamma.py", #Spark application path created in airflow and spark cluster
    name="mergegamma",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master,
          "spark.executor.extraJavaOptions":"-Dlog4j.configuration=file:///usr/local/spark/logger/log4j.properties"},
    application_args=[ecad_file,er_file,flood_file],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)


spark_job_load_loanBook = SparkSubmitOperator(
    task_id="spark_job_load_loanBook",
    application="/usr/local/spark/app/enrich_loan_book.py", # Spark application path created in airflow and spark cluster
    name="load-loanBook",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master,
          "spark.executor.extraJavaOptions":"-Dlog4j.configuration=file:///usr/local/spark/logger/log4j.properties"},
    application_args=[lb_file],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag) 

job_load_visualization = BashOperator(
        task_id= 'job_load_visualization',
        bash_command="python /usr/local/spark/app/Gamma_Loan_Visualisation.py",
        dag=dag
   )


'''job_load_visualization = PythonOperator(
    task_id="job_load_visualization",
    application="/usr/local/dags/Gamma_Loan_Visualisation.py", # Spark application path created in airflow and spark cluster
    name="load-visualization",
    provide_context=False,
    python_callable=popup_html_ber,
    dag=dag) '''

end_load_loanbook = DummyOperator(task_id="end_load_loanbook", dag=dag)


start_load_gamma >> spark_job_load_gamma >>  spark_job_load_loanBook >> job_load_visualization >> end_load_loanbook 
#start_load_flood >> spark_job_load_flood >> end_load_flood
#start_load_loanbook >> spark_job_load_loanBook >> job_load_visualization >> end_load_loanbook


