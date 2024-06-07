#imports 
from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
import pandas as pd
from datetime import timedelta, datetime


#--------------------------------------------------------------------------------------------------------  
url = "/opt/airflow/plugins"

  
download_unzip_scrip = """
url=https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
cd {}
curl -O $url 
mkdir -p {}/tolldata
chmod +x {}/tolldata
mkdir -p {}/ex_data
chmod +x {}/ex_data
tar -xzf tolldata.tgz -C {}/tolldata
rm tolldata.tgz
""".format(url,url,url,url,url,url)



def extract_data_from_csv ():
    df = pd.read_csv("{}/tolldata/vehicle-data.csv".format(url))
    df.columns =["Rowid","Timestamp","Vehicle_num","Vehicle_type","Num_of_axles","Vehicle_code"]
    df.drop("Vehicle_code",axis=1).to_csv("{}/ex_data/csv_data.csv".format(url),index=False) 




def extract_data_from_tsv():
    df = pd.read_table("{}/tolldata/tollplaza-data.tsv".format(url), sep='\t', header=None)
    df.columns =["Rowid","Timestamp","Vehicle_num","Vehicle_type","Num_of_axles","Tollplaza_id","Tollplaza_code"]
    df.drop(["Rowid","Timestamp","Vehicle_num","Vehicle_type","Num_of_axles",],axis=1).to_csv("{}/ex_data/tsv_data.csv".format(url),index=False)



def extract_data_from_fixed_width():
    columns_specs=[(0,6),(6,31),(31,39),(42,47),(47,57),(57,61),(61,69)]
    df = pd.read_fwf("{}/tolldata/payment-data.txt".format(url), colspecs=columns_specs, header=None)
    df.columns =["Rowid","Timestamp","Vehicle_num","Tollplaza_id","Tollplaza_code","Payment_type_code","Vehicle_Code"]    
    df.drop(["Rowid","Timestamp","Vehicle_num","Tollplaza_id","Tollplaza_code"],axis=1).to_csv("{}/ex_data/fixed_width_data.csv".format(url),index=False)


# ther is something wrong with this function check the difference between extracted_data.csv transformed_data.csv
def transform_data():
    df = pd.read_csv("{}/ex_data/extracted_data.csv".format(url))
    df["Vehicle_type"] = df["Vehicle_type"].str.upper()
    df.to_csv("{}/ex_data/transformed_data.csv".format(url),index=False)



#--------------------------------------------------------------------------------------------------------    

#DAG arguments definition 
default_args ={
    "owner":"Mohamed Eldeeb",
    "start_date":days_ago(0),
    "email":"mohamedeldeeb2016@icloud.com",
    "email_on_failure":True,
    "email_on_retry":True,
    "retries":1,
    "retry_delay":timedelta(minutes=5),
}

#DAG definition
dag = DAG(
    dag_id = "ETL_toll_data",
    description= "Apach airflow finall assignment",
    default_args = default_args,
    schedule_interval = '0 0 * * *',
)


#Tasks definitons

task_1= BashOperator(
    task_id = 'unzip_data',
    bash_command = f'{download_unzip_scrip}',
    dag=dag,
)


task_2= PythonOperator(
    task_id = 'extract_data_from_csv',
    python_callable = extract_data_from_csv,
    dag=dag,
)

 
task_3= PythonOperator(
    task_id = 'extract_data_from_tsv',
    python_callable = extract_data_from_tsv,
    dag=dag,
)


task_4= PythonOperator(
    task_id = 'extract_data_from_fixed_width',
    python_callable = extract_data_from_fixed_width,
    dag=dag,
)


task_5= BashOperator(
    task_id = 'consolidate_data',
    bash_command = "paste -d , {}/ex_data/csv_data.csv {}/ex_data/tsv_data.csv {}/ex_data/fixed_width_data.csv > {}/ex_data/extracted_data.csv".format(url,url,url,url),   
    dag=dag,
)


task_6= PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_data,
    dag=dag,
)



#--------------------------------------------------------------------------------------------------------
#task pipeline definition 
task_1 >> task_2
task_2 >> task_3 
task_3 >> task_4
task_4 >> task_5
task_5 >> task_6 




