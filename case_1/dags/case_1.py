from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from csv import DictReader
from airflow.utils.dates import days_ago
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
import airflow.utils.dates


# Создаем объект DAG
dag = DAG(
    dag_id="case_1",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
    max_active_runs=1,
)

# Задача для выполнения парсинга CSV файла
def _parse_csv_file():
    data_path = "/opt/airflow/dags/t_employee.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    with open(data_path, 'r', encoding='windows-1251') as file:
        csv_reader = DictReader(file, delimiter=';')
        
        data = {
            'employee_key': [],
            'tab_number': [],
            'department': [],
            'position': [],
            'full_name': [],
            'birth_date': [],
            'current_adress': [],
            'phone1': [],
            'phone2': [],
            'work_month': [],
            'work_time': [],
            'start_date': [],
            'end_date': [],
            'IsCurrent': [],
        }

        for row in csv_reader:
            for key, value in row.items():
                data[key].append(value)

        employee_key = data['employee_key']
        tab_number = data['tab_number']
        department = data['department']
        position = data['position']
        full_name = data['full_name']
        birth_date = data['birth_date']
        current_adress = data['birth_date']
        phone1 = data['phone1']
        phone2 = data['phone2']
        work_month = data['work_month']
        work_time = data['work_time']
#        print(employee_key, tab_number,full_name)
 
    with open("/tmp/postgres_query.sql", "w") as f:
        for employee_key, tab_number, department, position, full_name, birth_date, current_adress, phone1, phone2, work_month, work_time in zip(employee_key, tab_number, department, position, full_name, birth_date, current_adress, phone1, phone2, work_month, work_time):
            f.write(
                f"INSERT INTO employee_data (employee_key, tab_number, department, position, full_name, birth_date, current_adress, phone1, phone2, work_month, work_time) VALUES "
                f"('{employee_key}', '{tab_number}','{department}','{position}','{full_name}','{birth_date}','{current_adress}','{phone1}','{phone2}','{work_month}','{work_time}');\n"
           #если включить нижнюю строчку, все перестанет работать)
                f"CALL correct_IsCurrent();\n"
            )



parse_csv_file = PythonOperator(
    task_id="parse_csv_file",
    python_callable=_parse_csv_file,
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag,
)

# Устанавливаем порядок выполнения задач
parse_csv_file >> write_to_postgres
