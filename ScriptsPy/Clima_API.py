from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

#Descargamos el json desde la API
def downjson():
    url = "http://api.weatherapi.com/v1/current.json?q=Córdoba&key=78e2b2a5e43b4775a1d13127220912"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    df=pd.read_json(response.text)
    
    return df.to_json('data/BajadaClima_API.json')
    #Transformamos y sobreescribimos el mismo Json
def transf():
    df=pd.read_json('data/BajadaClima_API.json')
    df=df.drop(["name","lat","lon","tz_id","localtime_epoch","last_updated_epoch","temp_f","is_day","condition"],axis=0)

    return df.to_csv('data/BajadaClima_API.csv')

    #ingestamos el JSON trasnformado
def ingesta():
    hook=PostgresHook(postgres_conn_id='postgres_localhost')
    hook.copy_expert(sql="COPY ClimaAPI_TEST  FROM stdin  WITH CSV HEADER DELIMITER AS ',' ",
                      filename='data/BajadaClima_API.csv'
                    )  


            
#Nota al poner el chedule_interval='@daily' esto se trata de un shorthand que correra todos los dias una vez al dia a la medianoche  
with DAG(dag_id='Clima_API',description='',start_date=datetime(2022,12,8),schedule_interval='@daily',catchup=False) as dag:
    
    task1=PythonOperator(      
        task_id='descargardeAPI',
        python_callable=downjson
    )

    task2=PythonOperator(      
    task_id='TransformarconPandas',
    python_callable=transf
    )
    
    task3=PythonOperator(      
    task_id='CargarenPostgres',
    python_callable=ingesta
    )



    task1 >> task2 >> task3 

'''
En este proyecto la idea es consumir de una API de clima, transformar estos datos y posteriormente cargarlo 
en postgress
'''

'''
En caso de que queramos crear datos para posteriormente cargarlos en una BD, debemos tener en cuenta que solo hay dos 
opciones o usando Xcoms o creando un archivo fisico, en Airflow no se puede tener datos en memoria
ya que las tasks entre si son independientes.

'''