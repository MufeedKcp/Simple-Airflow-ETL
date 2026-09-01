import os
import pendulum
import requests
import datetime 
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="process_employee_data",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def process_employee_data():

    create_employee_table = SQLExecuteQueryOperator(
        task_id="create_employee_table",
        conn_id="first_airflow_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS employee (
                Serial_Number INT PRIMARY KEY,
                Company_Name TEXT,
                Employee_Markme TEXT,
                Description TEXT,
                LeaveReason INTEGER
            );"""
    )
    create_employee_staging_table = SQLExecuteQueryOperator(
        task_id="create_employee_staging_table",
        conn_id="first_airflow_conn",   
        sql="""
                DROP TABLE IF EXISTS employees_temp;
                CREATE TABLE IF NOT EXISTS staging_employee (
                    Serial_Number INT PRIMARY KEY,
                    Company_Name TEXT,
                    Employee_Markme TEXT,
                    Description TEXT,
                    LeaveReason INTEGER
                );""",
    )

    @task 
    def fetch_employee_data():
        data_path = 'opt/airflow/dags/files/employee_data.csv'
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"
        response = requests.get(url)

        with open(data_path, "w") as f:
            f.write(response.text)

        mysql_hook = MySqlHook(mysql_conn_id="first_airflow_conn", local_infile=True)
        conn = mysql_hook.get_conn()
        cur = conn.cursor()
        
        # Load data from the CSV file into the staging table using LOAD DATA LOCAL INFILE
        sql = f"""
            LOAD DATA LOCAL INFILE '{data_path}'
            INTO TABLE staging_employee
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
        """
        # Execute the SQL query to load data into the staging table
        cur.execute(sql)
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()



    @task
    def merge_employee_data():
        sql = """
            INSERT INTO employee (Serial_Number, Company_Name, Employee_Markme, Description, LeaveReason)
            SELECT Serial_Number, Company_Name, Employee_Markme, Description, LeaveReason
            FROM staging_employee
            ON DUPLICATE KEY UPDATE
                Company_Name = VALUES(Company_Name),
                Employee_Markme = VALUES(Employee_Markme),
                Description = VALUES(Description),
                LeaveReason = VALUES(LeaveReason);
        """
        try:
            mysql_hook = MySqlHook(mysql_conn_id="first_airflow_conn")
            conn = mysql_hook.get_conn()
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            return 1
        except Exception as e:
            return 0

    [create_employee_table, create_employee_staging_table] >> fetch_employee_data() >> merge_employee_data()

process_employee_data()
