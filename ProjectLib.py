
import os
from datetime import datetime

#讀取機敏設定檔
from dotenv import get_key,load_dotenv

def getenv(getenv):
    load_dotenv()
    return os.getenv(getenv)

#回傳專案時間文字格式(2024-05-31T21:07:35)
def getNowDatetime():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

#回傳專案時間文字格式(20240531)
def getNowDate():
    return datetime.now().strftime("%Y%m%d")


# 16位timestamp（微秒）時間搓轉回傳專案時間文字格式(2024-05-31T21:07:35)
def timestamp_microToDatetime(timestamp_micro):
    # 假設這是你的 16 位 timestamp（微秒）
    #timestamp_micro = 1713335681123456

    # 轉換成 datetime（除以 1_000_000 變成秒）
    dt = datetime.fromtimestamp(timestamp_micro / 1_000_000)

    # print(dt)  # 輸出: 2024-04-17 12:14:41.123456（視數字而定）
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


