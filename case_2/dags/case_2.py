import json
import requests
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="case_2",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
    max_active_runs=1,
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



def _fetch_pageviews():
    with open("/tmp/wikipedia_data.json", "r") as file:
        data = json.load(file)

        # Извлечь первые 500 записей, начинающихся с "A" и их pageid
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

    with open("/tmp/postgres_query.sql", "w") as f:
        for title, page_id in zip(page_titles, page_ids):
            title = title.replace("'", "_")
            f.write(
                f"INSERT INTO all_pages_1 (page_title, page_id) VALUES "
                f"('{title}', {page_id});\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag,
)

download_pages >> fetch_pageviews >> write_to_postgres
