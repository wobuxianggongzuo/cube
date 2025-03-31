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

# 設定 HTTP 請求 headers
HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/129.0.0.0 Safari/537.36",
}


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


def get_house_detail(house_id):
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


def main():
    # 設定篩選條件：此處 region=8 代表臺中市，kind=0 代表租屋物件
    params = {"region": "8", "kind": "0", "sort": "money_desc"}
    house_ids = search_houses(params)
    print(f"總共找到 {len(house_ids)} 間房屋，ID 列表：{house_ids}")

    # 依據爬取的每筆房屋 ID，獲取詳細資料並整理成 dict
    rows_to_insert = []
    for hid in house_ids:
        detail = get_house_detail(hid)
        if detail:
            rows_to_insert.append(detail)

    # save to json
    with open("houses.json", "w", encoding="utf-8") as f:
        json.dump(rows_to_insert, f, ensure_ascii=False, indent=4)

    if rows_to_insert:
        insert_into_bigquery(rows_to_insert)


if __name__ == "__main__":
    main()
