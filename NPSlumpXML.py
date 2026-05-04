import os
import sys
from pathlib import Path
import shutil
import lxml.etree as ET
from datetime import datetime

# 資料庫連線相關及Orm.Model
from sqlalchemy import select
from sqlalchemy.sql import text

from db import dbinst, Result10MinData

from ProjectLib import getenv

import ProjectLib as ProjectLib

# Logger
from logger import get_logger

log_obj = get_logger()


def XMLByResult10MinData():
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    XM10MinFile = "10min_a_ds_data.xml"

    # 1. 初始化路徑
    base_path = Path("C:/FUNCTION/XML")
    am_path = base_path / "am"
    am_hist_path = base_path / "amhist" / now.strftime("%Y") / now.strftime("%m%d")

    target_file = am_path / XM10MinFile

    # 檢查目標檔案是否存在，不存在則無法更新
    if not target_file.exists():
        print(f"錯誤：找不到範本檔案 {target_file}")
        return

    # 2. 執行 SQL 取得資料庫最新資料並轉為字典以利查詢
    # 每一個SensorID，依照DatetimeString排序後，取最新的資料
    query = text("""
        WITH LatestData AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY SensorID ORDER BY DatetimeString DESC) as rn
            FROM [Result10MinData]
        )
        SELECT SensorID, [value], DatetimeString FROM LatestData WHERE rn = 1
    """)

    with dbinst.getsessionM15()() as session:
        rows = session.execute(query).fetchall()
        # 轉換為 dict: { 'SensorID': 'value' }
        db_data = {row.SensorID: row.value for row in rows}

    try:
        # 3. 讀取現有的 XML 檔案
        parser = ET.XMLParser(remove_blank_text=True)
        tree = ET.parse(str(target_file), parser)
        root = tree.getroot()

        # 4. 更新根節點的時間屬性
        root.set("time", now_str)

        # 5. 遍歷 XML 中所有的 sensor 節點進行更新
        sensor_count = 0
        for sensor in root.xpath("//sensor"):
            s_id = sensor.get("sensorId")

            # 如果資料庫有這顆 Sensor 的最新資料
            if s_id in db_data:
                sensor.set("time", now_str)  # 更新時間
                sensor.text = db_data[s_id]  # 更新數值
                sensor_count += 1

        # 6. 輸出更新後的 XML (不包含 XML 宣告)
        xml_str = ET.tostring(
            root, pretty_print=True, encoding="utf-8", xml_declaration=False
        )

        # 7. 寫入檔案與備份
        am_hist_path.mkdir(parents=True, exist_ok=True)
        backup_file = am_hist_path / f"{now.strftime('%H%M')}_{XM10MinFile}"

        with open(target_file, "wb") as f:
            f.write(xml_str)

        print(f"{target_file}, 更新完成 (共更新 {sensor_count} 筆感測器)")

        shutil.copy2(target_file, backup_file)
        print(f"{backup_file}, 製作完成")

        # 新增轉檔完成Log
        log_obj.write_log_info(f"{target_file},更新完成")
        log_obj.write_log_info(f"{backup_file},製作完成")

    except Exception as e:
        print(f"更新 XML 過程發生異常：{e}")


# 直接執行這個模組，它會判斷為 true
# 從另一個模組匯入這個模組，它會判斷為 false
if __name__ == "__main__":

    try:
        # 產生XML，資料來源抓Result10MinData
        XMLByResult10MinData()

        log_obj.write_log_info("TransProcalToResult10MinData,執行完成")
    except Exception as e:
        log_obj.write_log_exception(
            f"異常內容：{e}",
            f"發生異常: {type(e).__name__}",
        )
