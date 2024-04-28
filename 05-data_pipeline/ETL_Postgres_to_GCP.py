
import glob
import os
import psycopg2
import csv
import json

from airflow import DAG
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils import timezone
# from datetime import datetime
from datetime import timedelta
from typing import List


def _get_files(filepath="/opt/airflow/dags"):

# def _get_files(filepath: str = "/opt/airflow/dags") -> List[str]:
#     """
#     Description: This function is responsible for listing the files in a directory
#     """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.csv"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files



def _main(dataset_id, table_id, file_path):
    # โค้ดส่วนนี้จะเป็นการใช้ Keyfile เพื่อสร้าง Credentials เอาไว้เชื่อมต่อกับ BigQuery
    # โดยการสร้าง Keyfile สามารถดูได้จากลิ้งค์ About Google Cloud Platform (GCP)
    # ที่หัวข้อ How to Create Service Account
    #
    # การจะใช้ Keyfile ได้ เราต้องกำหนด File Path ก่อน ซึ่งวิธีกำหนด File Path เราสามารถ
    # ทำได้โดยการเซตค่า Environement Variable ที่ชื่อ KEYFILE_PATH ได้ จะทำให้เวลาที่เราปรับ
    # เปลี่ยน File Path เราจะได้ไม่ต้องกลับมาแก้โค้ด
    # keyfile = os.environ.get("KEYFILE_PATH")
    #
    # แต่เพื่อความง่ายเราสามารถกำหนด File Path ไปได้เลยตรง ๆ
    keyfile = "/opt/airflow/dags/project-pipeline-ds525-neon-to-bigquery-26d2169ea978.json"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    # โค้ดส่วนนี้จะเป็นการสร้าง Client เชื่อมต่อไปยังโปรเจค GCP ของเรา โดยใช้ Credentials ที่
    # สร้างจากโค้ดข้างต้น
    project_id = "project-pipeline-ds525"
    client = bigquery.Client(
        project=project_id,
        credentials=credentials,
    )

    # โค้ดส่วนนี้เป็นการ Configure Job ที่เราจะส่งไปทำงานที่ BigQuery โดยหลัก ๆ เราก็จะกำหนดว่า
    # ไฟล์ที่เราจะโหลดขึ้นไปมีฟอร์แมตอะไร มี Schema หน้าตาประมาณไหน
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("id", bigquery.SqlTypeNames.INT64),
            bigquery.SchemaField("province_id", bigquery.SqlTypeNames.INT64),
            bigquery.SchemaField("province_name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("province_name_en", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("min_rain", bigquery.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("max_rain", bigquery.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("avg_rainny", bigquery.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("region", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("year", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("month", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("date", bigquery.SqlTypeNames.DATE),
            bigquery.SchemaField("month_th", bigquery.SqlTypeNames.STRING),
        ],
    )

    # โค้ดส่วนนี้จะเป็นการอ่านไฟล์ CSV และโหลดขึ้นไปยัง BigQuery
    with open("/opt/airflow/dags/rainfall.csv", "rb") as f:
        table_id = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    # โค้ดส่วนนี้จะเป็นการดึงข้อมูลจากตารางที่เราเพิ่งโหลดข้อมูลเข้าไป เพื่อจะตรวจสอบว่าเราโหลดข้อมูล
    # เข้าไปทั้งหมดกี่แถว มีจำนวน Column เท่าไร
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

if __name__ == "__main__":
    all_files = get_files(filepath="/opt/airflow/dags")
    print(all_files)

    dataset_id = "rainfall_Bigquery"
    table_id = "rainfall"
    file_path = "rainfall.csv"

    # main(dataset_id, table_id, file_path)


    with open("/opt/airflow/dags/rainfall.csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([
            "id",
            "province_id",
            "province_name",
            "province_name_en",
            "min_rain",
            "max_rain",
            "avg_rain",
            "region",
            "year",
            "month",
            "date",
            "month_th",
        ])

        for datafile in all_files:
            with open(datafile, "r") as f:
                data = csv.loads(f.read())
                for each in data:
                    writer.writerow([
                        each["id"], 
                        each["province_id"],
                        each["province_name"],
                        each["province_name_en"],
                        each["min_rain"], 
                        each["max_rain"],
                        each["avg_rainny"],
                        each["region"],
                        each["year"], 
                        each["month"],
                        each["date"],
                        each["month_th"],
                    ])

    main(dataset_id="rainfall_Bigquery", table_id="rainfall", file_path="rainfall.csv")




def _neon_to_csv():
    # เชื่อมต่อกับฐานข้อมูล Neon Postgres
    conn = psycopg2.connect(
        dbname="rainfall",
        user="rainfall_owner",
        password="y9JtKbAgsG5f",
        host="ep-spring-truth-a1owr4u7.ap-southeast-1.aws.neon.tech",
        port="5432"
    )
    
    # เปิด Cursor เพื่อทำคำสั่ง SQL
    cur = conn.cursor()

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูล
    sql_query = "SELECT * FROM stg_rainfall;"
    
    # ทำคำสั่ง SQL
    cur.execute(sql_query)

    # ดึงข้อมูลทั้งหมด
    rows = cur.fetchall()

# เขียนข้อมูลลงในไฟล์ CSV
    with open('/opt/airflow/dags/rainfall.csv', 'w') as csv_file_neon:
        writer = csv.writer(csv_file_neon)
        writer.writerow([i[0] for i in cur.description])  # เขียนหัว column
        writer.writerows(rows)  # เขียนข้อมูล



def _query():
    hook = PostgresHook(postgres_conn_id="neon_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = "select * from rainfall"
    cur.execute(sql)
    rows = cur.fetchall()
    for each in rows:
        print(each)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "ETL_Postgres_to_GCP",
    start_date=timezone.datetime(2024, 4, 20),
    schedule=None,
    tags=["DS525","project_pipeline"],
    default_args=default_args,
) as dag:


    start = EmptyOperator(task_id="start")


    query = PythonOperator(
        task_id="query",
        python_callable=_query,
    )

    rename_columns = PostgresOperator(
        task_id="rename_columns",
        postgres_conn_id="neon_conn",
        sql="""
            create or replace view stg_rainfall
            as
                select
                    ID
                    , province_id
                    , province_name
                    , province_name_en
                    , min_rain
                    , max_rain
                    , avg_rain as avg_rainny
                    , region
                    , year
                    , month
                    , date 
                    , month_th

                from rainfall
          """,
    )

    neon_to_csv = PythonOperator(
        task_id="neon_to_csv",
        python_callable=_neon_to_csv,
    )

    csv_neon_to_bq = PythonOperator(
        task_id="csv_neon_to_bq",
        python_callable=_main,
        op_kwargs={
        "dataset_id": "rainfall_Bigquery",
        "table_id": "rainfall",
        "file_path": "rainfall.csv"
    },
    dag=dag,
)

    get_files_csv_neon = PythonOperator(
        task_id="get_files_csv_neon",
        python_callable=_get_files,
    )


    end = EmptyOperator(task_id="end")

    start >> query >> rename_columns >> neon_to_csv >> get_files_csv_neon >> csv_neon_to_bq >> end