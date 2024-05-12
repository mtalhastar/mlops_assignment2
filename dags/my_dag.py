import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import dvc.api

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']
DVC_REPO = 'https://github.com/mtalhastar/mlops_assignment2'
DVC_REMOTE = 'https://github.com/mtalhastar/mlops_assignment2'   
METADATA_FILE = 'metadata.json'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow-demo',
    'start_date': datetime(2024, 5, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'mlops_dag',
    default_args=default_args,
    description='Web scraping, transformation, DVC versioning, and Git push',
    schedule_interval='@daily' 
)

def extract(**context):
    extracted_data = []
    for source in sources:
        response = requests.get(source)
        soup = BeautifulSoup(response.content, 'html.parser')

        for article in soup.find_all('article'):
            title = article.find('h1' or 'h2' or 'h3').text
            description = article.find('p').text
            extracted_data.append({'title': title, 'description': description})

    with open('extracted_data.json', 'w') as f:
        json.dump(extracted_data, f)

    return extracted_data

def transform(**context):
    ti = context['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')

    transformed_data = []
    for item in extracted_data:
        transformed_data.append({
            'title': item['title'].lower().strip(),
            'description': item['description'].lower().strip()
        })

    with open('transformed_data.json', 'w') as f:
        json.dump(transformed_data, f)

    # Generate metadata
    metadata = {
        'timestamp': datetime.now().isoformat(),
        'sources': sources,
        'dvc_commit': dvc.api.get_rev()
    }

    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f)

    # DVC versioning
    dvc.api.add('transformed_data.json')
    dvc.api.add(METADATA_FILE)
    dvc.api.push(remote=DVC_REMOTE)

    return transformed_data
 

def load(**context):
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    for item in transformed_data:
        print(item)


# Tasks
task_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
    provide_context=True,
)

task_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
    provide_context=True,
)

task_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
    provide_context=True,
)

# BashOperator for Git commands
task_git_add_commit_push = BashOperator(
    task_id='git_add_commit_push',
    bash_command=f'''
    cd {DVC_REPO}
    git add .
    git commit -m "Automated DVC push and metadata update"
    git push
    ''',
    dag=dag
)

# Set up task dependencies
task_extract >> task_transform >> task_load >> task_git_add_commit_push