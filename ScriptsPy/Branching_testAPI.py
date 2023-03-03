from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests



def cba_city():
    url = "http://api.weatherapi.com/v1/current.json?q=Córdoba&key=78e2b2a5e43b4775a1d13127220912"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    df=pd.read_json(response.text)
    
    return df.to_json('data/BajadaClima_API.json')
    #Transformamos y sobreescribimos el mismo Json

def rosa_city():
    url = "http://api.weatherapi.com/v1/current.json?q=Rosario&key=78e2b2a5e43b4775a1d13127220912"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    df=pd.read_json(response.text)
    
    return df.to_json('data/BajadaClima_API.json')
    #Transformamos y sobreescribimos el mismo Json

def chose_city(**context):
    #if context["execution_date"] < datetime(2022,12,20):
    if  'a'=='a':
        return 'descargardeAPIcba' #devolvemos los ID de las task que queremos que corra
    else:
        return 'descargardeAPIrosa'



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


            

with DAG(dag_id='Branching_testAPI',description='',start_date=datetime(2022,12,8),schedule_interval='@daily',catchup=False) as dag:

    cba=PythonOperator(      
        task_id='descargardeAPIcba',
        python_callable=cba_city
    )

    rosa=PythonOperator(      
    task_id='descargardeAPIrosa',
    python_callable=rosa_city
    )

        
    taskbranching=BranchPythonOperator(
        task_id="Elegir_Ciudad",
        python_callable=chose_city

    )
    

    task1=PythonOperator(      
    task_id='TransformarconPandas',
    python_callable=transf,
    trigger_rule="none_failed"
    )
    
    task2=PythonOperator(      
    task_id='CargarenPostgres',
    python_callable=ingesta
    )



    taskbranching>>[cba,rosa] >> task1 >> task2 
   

'''
La idea de este DAG es correr unas task en la que le decimos de acuerdo a ciertas
condiciones que corra una u otra task, en este caso que corra la task que descarga info
del clima de Rosario o la task que descarga info de CBA

'''