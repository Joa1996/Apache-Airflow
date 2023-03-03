from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor



def _confirma_archivo():
    
    return print("Archivo Cargado")

def _confirma_archivo2():
    
    return print("Archivo Segundo Cargado")

with DAG(dag_id='SensorsTest',start_date=datetime(2022,12,30),schedule_interval='@daily',catchup=False) as dag:

    EsperaArchivo=FileSensor(
        task_id="EsperaArchivo",
        poke_interval=30, # Intervalo de tiempo que queremos que airflow valide si esta o no el archivo, por default son 30s
        timeout=60*5, # Tiempo que queremos que nuestro Task se quede escuchando si llega o no un Archivo, por default son 7 dias.
        filepath='DataSet(Sensors)1.csv',#Poner el nombre del archivo que estamos esperando
        fs_conn_id='file_system'#Debemos crear una conexion con la path donde se dejara nuestro archivo
        
    )
    EsperaArchivo2=FileSensor(
        task_id="EsperaArchivo2",
        poke_interval=30, # Intervalo de tiempo que queremos que airflow valide si esta o no el archivo, por default son 30s
        timeout=60, # Tiempo que queremos que nuestro Task se quede escuchando si llega o no un Archivo, por default son 7 dias.
        filepath='DataSet(Sensors)2.csv',#Poner el nombre del archivo que estamos esperando
        fs_conn_id='file_system'#Debemos crear una conexion con la path donde se dejara nuestro archivo
        ,mode="reschedule" #De esta manera nos aseguramos de que si falla porque no encuentra el archivo en el tiempo dado, el mismo se reinicie
    )

    

    ConfirmaArchivo=PythonOperator(
          task_id='ConfirmaArchivo',
          python_callable=_confirma_archivo

    )
    ConfirmaArchivo2=PythonOperator(
          task_id='ConfirmaArchivo2',
          python_callable=_confirma_archivo2

    )

    EsperaArchivo >> ConfirmaArchivo
    EsperaArchivo2 >> ConfirmaArchivo2


'''
Con los FileSensors podemos hacer diversas cosas, desde esperar un Archivo en algun repositorio, algun 
registro en alguna base de datos, etc. para que esto dispare la task

'''