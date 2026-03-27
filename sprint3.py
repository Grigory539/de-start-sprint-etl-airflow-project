import time
import requests
import json
import pandas as pd
import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'grigory234'
cohort = 11

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')
    response = requests.post(f'{base_url}/generate_report', headers=headers, timeout=30)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')
    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers, timeout=30)
        response.raise_for_status()
        data = json.loads(response.content)
        print(f'Response is {response.content}')
        status = data['status']
        
        if status == 'SUCCESS':
            report_id = data['data']['report_id']
            break
        elif status == 'FAILED':
            raise Exception(f"Report generation failed: {data}")
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    
    if not report_id:
        raise ValueError("report_id not found in XCom")
    
    response = requests.get(
        f'{base_url}/get_increment',
        params={'report_id': report_id, 'date': f'{str(date)}T00:00:00'},
        headers=headers,
        timeout=30
    )
    response.raise_for_status()
    print(f'Response is {response.content}')

    data = json.loads(response.content)
    
    if data.get('status') != 'SUCCESS':
        raise ValueError(f"API returned: {data.get('status', 'Unknown')} - {data.get('error', '')}")
    
    increment_id = data['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')



def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    
    if not increment_id:
        raise ValueError("increment_id not found in XCom")
    
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    print(f'Downloading: {s3_filename}')
    
    local_filename = date.replace('-', '') + '_' + filename
    
    try:
        response = requests.get(s3_filename, timeout=60)
        response.raise_for_status()
        with open(f"{local_filename}", "wb") as f:
            f.write(response.content)
        print(f'✓ File downloaded: {local_filename}')

        df = pd.read_csv(local_filename)
        
        if 'id' in df.columns:
            df = df.drop('id', axis=1)
        
        df = df.drop_duplicates(subset=['uniq_id'])

        if 'status' not in df.columns:
            df['status'] = 'shipped'
            print('Added status=shipped for backward compatibility')

        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        
        row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f'{row_count} rows was inserted into {pg_schema}.{pg_table}')
        return row_count
        
    except Exception as e:
        print(f'Error: {e}')
        raise
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)
            print(f'Cleaned up: {local_filename}')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report_task = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment_task = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_customer_research_inc = PythonOperator(
        task_id='upload_customer_research_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'customer_research_inc.csv',
                   'pg_table': 'customer_research',
                   'pg_schema': 'staging'})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    upload_user_activity_inc = PythonOperator(
        task_id='upload_user_activity_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_activity_log_inc.csv',
                   'pg_table': 'user_activity_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": business_dt}
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": business_dt}
    )

    (
        generate_report_task
        >> get_report_task
        >> get_increment_task
        >> [upload_customer_research_inc, upload_user_order_inc, upload_user_activity_inc]
        >> [update_d_item_table, update_d_city_table, update_d_customer_table]
        >> update_f_sales
        >> update_f_customer_retention
    )
