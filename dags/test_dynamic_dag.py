
from airflow import Dataset
from airflow.decorators import dag, task, task_group
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
import pandas as pd
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")

# v_date_now  = '{{ ds }}'
v_date_now = '{{macros.ds_add(ds, 1)}}' ## Add 1 Day, because this date using UTC

def create_dag(dag_id, schedule, dag_number, p_host_ip, p_app_name, default_args):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def hello_world_dag():        

        sample_dict = {
            'num': ["1", "2", "3"],
            'name': ['chain_task', 'dery', 'arvis']
        }        

        # Convert the dictionary to a pandas DataFrame
        df = pd.DataFrame(sample_dict)

        start_empty_opr=EmptyOperator(task_id="start_empty_opr")

        @task()
        def hello_world():
            print("Hello World")
            print("This is DAG: {}".format(dag_number))

        @task_group(group_id='task_group')
        def task_grp():
            list_bash_opr_tg = []
            for idx in df.index:
                bash_opr_tg = BashOperator(
                task_id="bash_opr_data_loading_"+ df['num'][idx] +"_"+ df['name'][idx],
                bash_command="echo test_"+ df['name'][idx],
                )
                list_bash_opr_tg.append(bash_opr_tg)
            chain(*list_bash_opr_tg)
        
        bash_opr = BashOperator(
            task_id="bash_opr",
            bash_command="echo Hello $MY_NAME! && echo $A_LARGE_NUMBER | rev  2>&1\
                            | tee $AIRFLOW_HOME/include/my_secret_number.txt",
            env={"MY_NAME": "<my name>", "A_LARGE_NUMBER": "231942"},
            append_env=True,
        )

        end_empty_opr=EmptyOperator(task_id="end_empty_opr")
        
        hello_world = hello_world()
        task_grp = task_grp()
        start_empty_opr >> hello_world >> task_grp >> bash_opr >> end_empty_opr
        
    generated_dag = hello_world_dag()

    return generated_dag


# build a dag for each data in dataframe
sample_dict = {
    'num': ["001", "002", "003"],
    'host_ip': ['1.1.1.1', '2.2.2.2', '3.3.3.3'],
    'app_name': ['app1', 'app2', 'app3']
}

# Convert the dictionary to a pandas DataFrame
df = pd.DataFrame(sample_dict)

for idx in df.index:
    
    v_num = df['num'][idx]
    v_ip_server = df['host_ip'][idx]
    v_app_name = df['app_name'][idx]
    
    dag_id = "gpdl_test_"+ v_num +"_data_loading_"+ v_app_name
    default_args = {"owner": "airflow", "start_date": datetime(2024, 12, 1)}
    schedule = None
    
    globals()[dag_id] = create_dag(dag_id, schedule, v_num, v_ip_server, v_app_name, default_args)
