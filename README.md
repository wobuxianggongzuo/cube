# 591租屋資料 API

此專案包含爬蟲與 API 服務，用於爬取 591 租屋網資料並提供查詢介面。

## 本地開發環境設定

1. 複製 `.env.example` 為 `.env` 並填入適當的值
2. 安裝套件：`pip install -r requirements.txt`
3. 運行 API 服務：`uvicorn api:app --reload`
4. 運行爬蟲：`python crawler.py`

## API 端點

- `GET /houses` - 獲取所有房屋資料 (可用 `limit` 參數限制數量)
- `GET /houses/{house_id}` - 獲取特定房屋詳細資料

## 部署說明

請參考 `Dockerfile` 和部署命令