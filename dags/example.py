from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import os
import pandas as pd
path = os.environ['AIRFLOW_HOME']

dag = DAG('imdb_data')

def clean_data():
    df = pd.read_csv('data/IMDB-Movie-Data.csv')
    df.rename(columns={
        'Runtime (Minutes)': 'Runtime', 
        'Revenue (Millions)': 'Revenue_millions'
    }, inplace=True)
    df.columns = [col.lower() for col in df]
    df.to_csv('data/clean_movie_data.csv',index=False)
    
def drop_missing_value():
    df = pd.read_csv('data/clean_movie_data.csv')
    df = df.dropna()
    df.to_csv('data/raw_movie_data.csv',index=False)


clean_data_task = PythonOperator(
                        task_id='clean_data',
                        python_callable=clean_data,
                        dag=dag
                    )

drop_missing_value_data_task = PythonOperator(
                        task_id='drop_missing_value',
                        python_callable=drop_missing_value,
                        dag=dag
                    )

clean_data_task >> drop_missing_value_data_task