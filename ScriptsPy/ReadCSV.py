from datetime import datetime,timedelta
from airflow import DAG,task
from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
import psycopg2 as ps


def metodo(ti):
        # opening the CSV file
        csvFile=0
        path=r'data/Test.csv'
        df=pd.read_csv(path,sep=';')
        cs=df.to_json(orient='columns')
        res=json.dumps(cs) #Serializamos json antes de subir a una Xcom
        
       
        return ti.xcom_push(key='my_key',value=res) 
 

def traer(ti):
    value=ti.xcom_pull(key='my_key',task_ids='first_task')
    df=pd.read_json(json.loads(value)) 

    hook=PostgresHook(postgres_conn_id='postgres_localhost')
    hook.copy_expert(
    sql="COPY test_v1  FROM stdin  WITH CSV HEADER DELIMITER AS ';' ", # Si tenemos columnas con nombres poner HEADER DELIMITER AS ';'
    filename='data/Test.csv' #De esta ,amera  podemos cargar CSV directamente a la correspondiente tabla
                    )        #Asi de esta manera podriamos agarrar un archivo, transformalo con pandas y dpues cargarlo


#     #conn=hook.get_conn()
#    # cursor=hook.get_cursor()
#     # for row in df.itertuples():
        
#     #     cursor.execute('''
#     #                    INSERT INTO public.test_v1 (nombre,apellido) 
#     #                    VALUES(?,?);
#     #                   ''',
#     #                   row.Nombre,
#     #                   row.Apellido
                      
#     #                   )
#    # conn.commit()
#     return print(df)
    

with DAG(dag_id='ReadCSV',description='',start_date=datetime(2022,11,27),schedule_interval='@daily') as dag:
    

    

    task1=PythonOperator(      
        task_id='first_task',
        python_callable=metodo
    )

    task2=PythonOperator(      
        task_id='second_task',
        python_callable=traer
    )



    task1 >> task2