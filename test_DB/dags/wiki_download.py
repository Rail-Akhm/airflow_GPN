import json
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'your_owner',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
}

dag = DAG(
    dag_id="all_wikipedia_pages",
    default_args=default_args,
    schedule_interval=None,  # You can set a suitable schedule interval here
    catchup=False,
    tags=["example"],
)

def download_wikipedia_data():
    url = "https://ru.wikipedia.org/w/api.php?action=query&list=allpages&aplimit=500&apfrom=A&format=json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open("/tmp/wikipedia_data.json", "w") as file:
            json.dump(data, file)
        print("Wikipedia data downloaded successfully.")
    else:
        print(f"Failed to download Wikipedia data. Status code: {response.status_code}")

download_pages = PythonOperator(
    task_id="download_wikipedia_data",
    python_callable=download_wikipedia_data,
    dag=dag,
)

def parse_wikipedia_data():
    try:
        with open("/tmp/wikipedia_data.json", "r") as file:
            data = json.load(file)

            # Ваши действия с данными
            # Например, извлечь список страниц, начинающихся с "A" и их pageid
            count = 0
            page_titles = []
            page_ids = []

            allpages = data["query"]["allpages"]
            for page in allpages:
                if page["title"].startswith("A"):
                    page_titles.append(page["title"])
                    page_ids.append(page["pageid"])
                    count += 1
                    if count >= 500:
                        break

            # Вывести собранные названия и pageid
            for title, pageid in zip(page_titles, page_ids):
                print(f"Title: {title}, PageID: {pageid}")

    except FileNotFoundError:
        print("Файл 'wikipedia_data.json' не найден.")
    except json.JSONDecodeError:
        print("Ошибка при чтении JSON-файла 'wikipedia_data.json'.")

parse_data = PythonOperator(
    task_id="parse_wikipedia_data",
    python_callable=parse_wikipedia_data,
    dag=dag,
)


task1 = PostgresOperator(
    task_id = "create_postgres_table",
    start_date=default_args['start_date'],
    postgres_conn_id='postgres_localhost',
    sql = """
        create table if not exists dag_runs(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
        )
""",
    dag=dag,
)

download_pages >> parse_data>> task1
