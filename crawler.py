import json
import time
import random
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from google.cloud import bigquery
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from typing import Optional, Dict
from pathlib import Path

# 設定基本日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"crawler_{datetime.now().strftime('%Y%m%d')}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# 設定 HTTP 請求 headers
HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/129.0.0.0 Safari/537.36",
}


def retry_request(func, max_retries: int = 3, delay: int = 5):
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                result = func(*args, **kwargs)
                elapsed_time = time.time() - start_time

                # 記錄請求時間
                logger.info(f"請求完成 - 耗時: {elapsed_time:.2f}秒")

                # 加入隨機延遲，避免被封鎖
                time.sleep(delay + random.uniform(1, 3))

                return result

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"重試{max_retries}次後仍然失敗: {str(e)}")
                    return None
                logger.warning(f"嘗試失敗 ({attempt + 1}/{max_retries}), {delay}秒後重試...")
                time.sleep(delay)

    return wrapper


def fetch_page(url):
    """取得網頁內容，若發生錯誤則回傳 None"""
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def parse_house_ids(html):
    """從網頁內容中解析並回傳唯一的房屋 ID 集合"""
    soup = BeautifulSoup(html, "html.parser")
    house_ids = set()
    for link in soup.find_all("a", href=True):
        match = re.search(r"https://rent\.591\.com\.tw/(\d+)", link["href"])
        if match:
            house_ids.add(match.group(1))
    return house_ids


@retry_request
def get_house_detail(house_id: str) -> Optional[Dict]:
    """根據房屋 ID 爬取詳細資料，並解析所有需要的欄位"""
    url = f"https://rent.591.com.tw/{house_id}"
    html = fetch_page(url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    detail = {}

    # 設定 house_id
    detail["house_id"] = house_id

    # 解析標題 (title)
    title_element = soup.select_one("title")
    detail["title"] = title_element.get_text(strip=True) if title_element else ""

    # 解析價格 (price)
    price = soup.select_one(".house-price")
    detail["price"] = price.get_text(strip=True) if price else ""

    # 解析聯絡人身份和姓名
    try:
        contact_info = soup.select_one(".contact-info .name").text.strip().split(":")
        identity = contact_info[0].strip()
        name = contact_info[1].strip() if len(contact_info) > 1 else ""
        detail["contact_identity"] = identity
        detail["contact_name"] = name
    except:
        detail["contact_identity"] = ""
        detail["contact_name"] = ""

    # 解析聯絡電話
    try:
        phone_info = soup.select_one(".phone span span").text.strip()
        detail["contact_phone"] = phone_info if phone_info else ""
    except:
        detail["contact_phone"] = ""

    # 解析房屋類型和現況
    try:
        pattern_div = soup.select_one("div.pattern")
        if pattern_div:
            text_spans = [span for span in pattern_div.find_all("span") if span.text.strip()]
            extracted_texts = [span.text.strip() for span in text_spans]
            detail["house_type"] = extracted_texts[0] if len(extracted_texts) > 0 else ""
            detail["current_status"] = extracted_texts[1] if len(extracted_texts) > 1 else ""
    except:
        detail["house_type"] = ""
        detail["current_status"] = ""

    # 解析性別限制
    gender_restriction = "男女皆可"  # 預設值
    try:
        rule_element = soup.find("p", string="房屋守則")
        if rule_element:
            rule_text = rule_element.find_next_sibling("span").text.strip()
            if "限女" in rule_text:
                gender_restriction = "限女生"
            elif "限男" in rule_text:
                gender_restriction = "限男生"
    except:
        pass
    detail["gender_restriction"] = gender_restriction

    # 解析房屋描述
    try:
        description = soup.select_one(".house-condition-content").text.strip()
        detail["description"] = description if description else ""
    except:
        detail["description"] = ""

    return detail


def search_houses(filter_params):
    """依據篩選條件搜尋房屋，並回傳房屋 ID 集合"""
    base_url = "https://rent.591.com.tw/list?"
    url = base_url + urlencode(filter_params)
    html = fetch_page(url)
    time.sleep(random.uniform(1, 3))  # 隨機延遲，避免過度請求
    return parse_house_ids(html) if html else set()


def insert_into_bigquery(rows):
    client = bigquery.Client()

    if os.path.exists(".env"):
        load_dotenv()

    project_id = os.getenv("PROJECT_ID", "")
    dataset_id = os.getenv("DATASET_ID", "")
    table_id = os.getenv("TABLE_ID", "")
    table_id = f"{project_id}.{dataset_id}.{table_id}"

    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print(f"BigQuery 插入錯誤: {errors}")
        else:
            print(f"成功寫入 {len(rows)} 筆資料到 BigQuery")
    except Exception as e:
        print(f"寫入 BigQuery 時發生錯誤: {e}")


def save_crawl_stats(stats: Dict):
    """儲存爬蟲統計資料"""
    Path("stats").mkdir(exist_ok=True)
    filename = f"stats/crawl_stats_{datetime.now().strftime('%Y%m%d')}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def main():
    start_time = time.time()
    stats = {
        "start_time": datetime.now().isoformat(),
        "total_houses": 0,
        "success_count": 0,
        "failed_ids": [],
        "total_time": 0,
    }

    # 設定篩選條件
    params = {
        "region": "8",  # 台中市
        "sort": "posttime_desc",  # 發布時間由新至舊排序
    }
    house_ids = search_houses(params)
    stats["total_houses"] = len(house_ids)

    logger.info(f"開始爬取 {len(house_ids)} 間房屋資料")

    # 爬取詳細資料
    rows_to_insert = []
    for hid in house_ids:
        detail = get_house_detail(hid)
        if detail:
            rows_to_insert.append(detail)
            stats["success_count"] += 1
        else:
            stats["failed_ids"].append(hid)
            logger.warning(f"無法取得房屋資料: {hid}")

    # 更新統計資料
    stats["total_time"] = round(time.time() - start_time, 2)
    stats["success_rate"] = round(stats["success_count"] / stats["total_houses"] * 100, 2)

    # 儲存統計資料 for local
    save_crawl_stats(stats)

    # 寫入 BigQuery
    if rows_to_insert:
        insert_into_bigquery(rows_to_insert)
        logger.info(f"成功儲存 {len(rows_to_insert)} 筆資料到 BigQuery")

    # 輸出總結
    logger.info(f"""
        爬蟲作業完成:
        - 總房屋數: {stats["total_houses"]}
        - 成功筆數: {stats["success_count"]}
        - 失敗筆數: {len(stats["failed_ids"])}
        - 成功率: {stats["success_rate"]}%
        - 總耗時: {stats["total_time"]}秒
    """)


if __name__ == "__main__":
    main()
