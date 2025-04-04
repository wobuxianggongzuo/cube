import os
from fastapi import FastAPI
from google.cloud import bigquery
from dotenv import load_dotenv
from crawler import get_house_detail, insert_into_bigquery

if os.path.exists(".env"):
    load_dotenv()

app = FastAPI()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


@app.get("/houses")
def get_houses(limit: int = 100):
    client = bigquery.Client()
    query = f"SELECT * FROM `{TABLE_PATH}` LIMIT {limit}"
    query_job = client.query(query)
    results = [dict(row) for row in query_job]
    return results


@app.get("/houses/{house_id}")
def get_house_detail_api(house_id: str):
    client = bigquery.Client()
    query = f"SELECT * FROM `{TABLE_PATH}` WHERE house_id = '{house_id}' LIMIT 1"
    query_job = client.query(query)
    result = list(query_job)

    if result:
        return dict(result[0])

    # 如果找不到資料，嘗試爬取
    detail = get_house_detail(house_id)
    if detail:
        # 將資料寫入 BigQuery
        insert_into_bigquery([detail])
        return detail

    return {"error": "未找到該房屋資料"}
