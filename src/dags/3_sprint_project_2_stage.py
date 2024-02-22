#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import requests
import json
import pandas as pd


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

print ('base_url  ', base_url, 'api_key ', api_key)

postgres_conn_id = 'postgresql_de'

nickname = 'Ilya'
cohort = '23'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}
args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'f_customer_retention',
        default_args=args,
        description='Provide default dag for sprint3 part 2',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:

    customer_retention = PostgresOperator(
            task_id='customer_retention',
            postgres_conn_id=postgres_conn_id,
            sql="sql/mart.f_customer_retention.sql")

    customer_retention

