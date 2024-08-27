from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_pascal_triangle():
    triangle = []

    for level in range(10):
        row = [1] * (level+1)
        for index in range(1, level):
            row[index] = triangle[level-1][index-1] + triangle[level-1][index]
        triangle.append(row)

    for i in triangle:
        for j in i:
            print(j, end=' ')
        print()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 27, 11, 44),
}

dag = DAG('pascal', default_args=default_args, schedule_interval='@daily')

python_task = PythonOperator(
    task_id='print_pascal_triangle',
    python_callable=print_pascal_triangle,
    dag=dag,
)