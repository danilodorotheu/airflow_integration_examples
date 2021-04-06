# -*- coding: utf-8 -*-
#
# Author: Danilo Dorotheu
# Date: 2021-04-06
# Description:
# 

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

import csv
import requests
import json
import boto3


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# 
# Download do dado (origem s3) para pasta temporaria
#
def download_csv():
    s3 = boto3.client(
        's3',
        aws_access_key_id="insert_key_id_here",
        aws_secret_access_key="insert_secret_key_here"
    )

    s3.download_file('bucket_name', 'file_name_inside_bucket.extension', '/destionation_folder/file_name.extension')


# =================================================================== #
# DAG load_data
# Objetivo:
#   Realizar a movimentacao, transformacao e carga 
#   do arquivo movies.csv (origem s3) para o ambiente do datalake,
#
dag = DAG(dag_id="load_data", schedule_interval="@daily", default_args=default_args, catchup=False)

# 
# Download do arquivo de origem
#
downloading_file = PythonOperator(
    task_id="downloading_file",
    python_callable=download_csv,
    dag=dag
)

# 
# Movimentacao do arquivo para o HDFS
#
saving_hdfs = BashOperator(
    task_id="saving_hdfs",
    bash_command="""
        hdfs dfs -mkdir -p /movies && \
        hdfs dfs -put -f /tmp/movies.csv /movies
        """,
    dag=dag
)

# 
# Criando tabela de destino no hive
#
creating_table = HiveOperator(
    task_id="creating_table",
    hive_cli_conn_id="hive_conn",
    hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS TBP_MOVIES(
            title STRING,
            genres STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """,
    dag=dag
)

# 
# Processamento da transformacao e carga 
# do arquivo de origem para a tabela destino
#
processing_etl = SparkSubmitOperator(
    task_id="processing_etl",
    conn_id="spark_conn",
    application="/usr/local/airflow/dags/scripts/processing_etl.py",
    verbose=False,
    dag=dag
)

downloading_file >> saving_hdfs >> creating_table >> processing_etl

