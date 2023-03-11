from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import pprint


from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,3,10),
    'email': ['ramjanraeen@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'queue': 'bash_queue',
    #'pool': 'backfill',
    #'priority_weight': 10,
    #end_date: datetime(2023,4,1),
}

def print_context(ds, **kwargs):
    # pprint(kwargs['execution_date'])
    print(ds)
    print(f"Another way")
    logging.info(ds)
    # logging.info(kwargs['run_id'])
    return "Python_task"

with DAG(dag_id='mydag', default_args=default_args, schedule_interval='@daily', catchup=False ) as dag:
    task01 = BashOperator(
        task_id='bash_opt',
        bash_command="echo 'Hello World'"
    )
    task02 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=3,
    )

    template_command = """
    {% for i in range(5) %}
    echo "{{ ds}}"
    echo "{{ macros.ds_add(ds, 5) }}"
    echo "{{ params.my_param }}"
    {% endfor %}
    """

    task03 = BashOperator(
        task_id='templated_command',
        bash_command=template_command,
        params={'my_param': 'I am Ramjan'},
    )

    task04 = BashOperator(
        task_id='jinja_template',
        bash_command='echo "execution_date={{ds}} | run_id={{run_id}} | dag_run={{ dag_run}}"',
    )

    task05 = PythonOperator(
        task_id='run_python_func',
        provide_context=True,
        python_callable=print_context,
        
    )

    task01 >> task02 >> task03 >> task04 >> task05
