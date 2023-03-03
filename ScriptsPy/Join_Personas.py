from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def _readpostgres():
    hook=PostgresHook(postgres_conn_id='postgres_localhost')
    df=hook.get_pandas_df('select * from  public.personass_v1 ')
    
    return df.to_csv('data/ETL_Buffer/personass_v1.csv',index=False)

def _transform():
    df1=pd.read_csv('data/Personas_V1.csv',';')
    df2=pd.read_csv('data/ETL_Buffer/personass_v1.csv',',')
    inner=pd.merge(df1,df2,how='right',left_on='DNI',right_on='dni',) #Joineamos con Right JOIN
    
    return inner[["dni","nombre","Deuda"]].to_csv('data/Salida_Personas_V1.csv',';',index=False) #Solamente queremos mostrar ciertas columnas


with DAG(dag_id='Join_Personas',start_date=datetime(2023,1,2),schedule_interval='@daily',catchup=False) as dag:

    task1=PythonOperator(
        task_id="task1",
        python_callable=_readpostgres
    )
    
    task2=PythonOperator(
        task_id="task2",
        python_callable=_transform
    )

task1 >>task2


'''
La idea de este DAG es levantar dos dataframes de distintos origenes, y probar joinearlos.
En este caso se realizo un Right Join
'''