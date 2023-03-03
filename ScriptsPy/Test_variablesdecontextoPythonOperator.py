from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def variables(**context):
    
    return print(context["execution_date"] )

    


            
#Nota al poner el chedule_interval='@daily' esto se trata de un shorthand que correra todos los dias una vez al dia a la medianoche  
with DAG(dag_id='Test_variablesdecontextoPythonOperator',start_date=datetime(2022,12,17),schedule_interval='@daily',catchup=False) as dag:
    
    task1=PythonOperator(      
        task_id='task1',
        python_callable=variables
    )

    task1

