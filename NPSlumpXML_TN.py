import os
import sys
from pathlib import Path
import shutil
import lxml.etree as ET
from datetime import datetime

# 資料庫連線相關及Orm.Model
from sqlalchemy import select
from sqlalchemy.sql import text

# from sqlalchemy.orm import session
from db import dbinst, Result10MinData

from ProjectLib import getenv

import ProjectLib as ProjectLib

# Logger
from logger import get_logger

log_obj = get_logger()


# def import_xml_to_sql(file_path):
#     """
#     使用 SQLAlchemy Session 處理 XML 轉檔並寫入 Result10MinData 資料表
#     """
#     # 取得 Session 類別並建立實例 (with session 結束會自動關閉連線)
#     Session = dbinst.getsessionM15()

#     try:
#         with Session() as session:
#             # 解析 XML 檔案
#             tree = XET.parse(file_path)
#             root = tree.getroot()

#             count_inserted = 0
#             count_skipped = 0

#             # 遍歷 XML 結構 (site_data -> station -> sensor)
#             for site in root.findall(".//site_data"):
#                 site_id = site.get("siteid")

#                 for station in site.findall("station"):
#                     station_id = station.get("stationId")

#                     for sensor in station.findall("sensor"):
#                         sensor_id = sensor.get("sensorId")
#                         sensor_type = sensor.get("sensor_type")
#                         obs_num = sensor.get("observation_num")
#                         status = sensor.get("sensor_status")
#                         raw_time = sensor.get("time")
#                         sensor_value = sensor.text

#                         # --- 時間格式處理 ---
#                         try:
#                             # 1. 轉換為 datetime 物件
#                             dt_obj = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")

#                             # 取得時間，用XML檔案中的時間
#                             get_time = dt_obj.strftime("%Y-%m-%dT%H:%M:%S")

#                             # 強制將秒數設為 0
#                             dt_obj = dt_obj.replace(second=0)

#                             # 3. 產生對應字串
#                             dt_string = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

#                         except Exception:
#                             dt_obj = None
#                             dt_string = raw_time
#                             get_time = raw_time

#                         # --- 建立 ORM 物件 ---
#                         new_data = Result10MinData(
#                             SiteID=site_id,
#                             StationID=station_id,
#                             SensorID=sensor_id,
#                             DataType=sensor_type,
#                             DataName="",  # 依照要求設為空字串
#                             Datetime=dt_obj,  # 傳入 datetime 物件
#                             DatetimeString=dt_string,
#                             GetTime=get_time,
#                             observation_num=obs_num,
#                             sensor_status=status,
#                             value=sensor_value,
#                             # remark="XML回寫",
#                         )

#                         # --- 執行單筆寫入與 Commit ---
#                         try:
#                             session.add(new_data)
#                             session.commit()  # 每一筆成功就立刻存入資料庫
#                             count_inserted += 1
#                         except IntegrityError:
#                             # 發生重複 (Unique Index 衝突)
#                             session.rollback()  # 必須 rollback 以清除該次失敗的 Transaction 狀態
#                             count_skipped += 1
#                             continue
#                         except Exception as e:
#                             # 發生其他寫入錯誤
#                             session.rollback()
#                             print(f"感測器 {sensor_id} 寫入失敗: {e}")
#                             continue

#             log_obj.write_log_info(
#                 amPath
#                 + XM10MinFile
#                 + f",檔案處理完成,成功新增: {count_inserted} 筆, 忽略重複: {count_skipped} 筆"
#             )
#             print(
#                 amPath
#                 + XM10MinFile
#                 + f",檔案處理完成,成功新增: {count_inserted} 筆, 忽略重複: {count_skipped} 筆"
#             )

#     except Exception as e:
#         log_obj.write_log_exception(
#             f"處理檔案時發生非預期錯誤：{e}",
#             f"發生異常: {type(e).__name__}",
#         )


# # using now() to get current time
# now = datetime.now()

# # Some other example server values are
# server = getenv("DataBaseIP")
# database = getenv("DataBaseNameProcal")
# username = getenv("DataBaseAccountProcal")
# password = getenv("DataBasePassWordProcal")
# # ENCRYPT defaults to yes starting in ODBC Driver 17. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
# # connstring='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
# connstring = (
#     "DRIVER={SQL Server};SERVER="
#     + server
#     + ";DATABASE="
#     + database
#     + ";UID="
#     + username
#     + ";PWD="
#     + password
# )
# print("連接資料庫:" + "DRIVER={SQL Server};SERVER=" + server + ";DATABASE=" + database)
# cnxn = pyodbc.connect(connstring)
# cursor = cnxn.cursor()


# #
# # Sample select query
# # 20250509 新增判斷StationName <> 'False'，避免抓取資料內容預設值有異常
# # cursor.execute("SELECT  StationID, MapCH, Title, Unit, DataTime, RealVale, StationName, ParameterBP, ParameterR FROM StationReal WHERE StationName <> '' ORDER BY  StationID")
# cursor.execute(
#     "SELECT  StationID, MapCH, Title, Unit, DataTime, RealVale, StationName, ParameterBP, ParameterR FROM StationReal WHERE StationName <> '' and StationName <> 'False' ORDER BY  StationID"
# )
# # 測試
# # cursor.execute("SELECT  StationID, MapCH, Title, Unit, DataTime, RealVale, StationName, ParameterBP, ParameterR FROM StationReal WHERE StationName in ('23D64T02','','','') ORDER BY  StationID")
# row = cursor.fetchone()
# theXValue = ""  # X傾斜
# theYValue = ""  # Y傾斜
# theX1Value = ""  # X1天傾斜變化
# theY1Value = ""  # Y1天傾斜變化
# theStationID = ""  # 傾斜ID
# StationName = []  # 測站名稱
# StationValue = []  # 更新值
# while row:
#     print(
#         row[0]
#         + ","
#         + str(row[1])
#         + ","
#         + str(row[2])
#         + ","
#         + str(row[3])
#         + ","
#         + str(row[4])
#         + ","
#         + str(row[5])
#         + ","
#         + str(row[6])
#     )
#     cnxn2 = pyodbc.connect(connstring)
#     cursorOLD = cnxn2.cursor()
#     theValue = ""  # 更新值
#     match str(row[2]):
#         case "地下水位":
#             theValue = str(row[5] - row[7]) + " " + str(row[5] - row[7] - row[8])
#             # theValue=str(row[5])
#             # print(str(row[6]) +":"+ theValue)
#             StationName.append(str(row[6]))
#             StationValue.append(theValue)
#         case "伸縮計":
#             # 產生查詢時間(一天前)
#             QueryDateTime = row[4] - timedelta(days=1)
#             # 產生查詢時間
#             cursorOLD.execute(
#                 "SELECT TOP(1) StationID,DataTime,CH1 FROM StationData WHERE DataTime <='"
#                 + str(QueryDateTime)
#                 + "' and  StationID='"
#                 + str(row[0])
#                 + "' ORDER BY  DataTime DESC"
#             )
#             rowOLD = cursorOLD.fetchone()
#             while rowOLD:
#                 # print(rowOLD[0]+","+str(rowOLD[1])+","+str(rowOLD[2])+","+str(rowOLD[3])+","+str(rowOLD[4]))
#                 theValue = (
#                     str(row[5]) + " " + str(row[5]) + " " + str(row[5] - rowOLD[2])
#                 )
#                 rowOLD = cursorOLD.fetchone()
#             # print(str(row[6]) +":"+ theValue)
#             StationName.append(str(row[6]))
#             StationValue.append(theValue)
#         case "傾斜X":
#             # 地表傾斜計(雙軸)-X
#             # 公式
#             # D方位一觀測值(秒)
#             # theXValue=SX*3600
#             # F方位一累積變位量(秒)(初始值-欄位ParameterBP)
#             # theXAllValue=theXValue-row[7]*3600
#             # H方位一速率(秒/天)
#             # theX1Value=theXValue-rowOLD[2]*3600
#             theStationID = row[0]  # 傾斜ID
#             theXValue = row[5] * 3600
#             if str(row[7]) == "None":
#                 theXAllValue = 0
#             else:
#                 theXAllValue = theXValue - row[7] * 3600
#             QueryDateTime = row[4] - timedelta(days=1)
#             ssql = (
#                 "SELECT TOP(1) StationID,DataTime,CH1 FROM StationData WHERE DataTime <='"
#                 + str(QueryDateTime)
#                 + "' and  StationID='"
#                 + str(row[0])
#                 + "' ORDER BY  DataTime DESC"
#             )
#             cursorOLD.execute(ssql)
#             rowOLD = cursorOLD.fetchone()
#             while rowOLD:
#                 print(rowOLD)
#                 theX1Value = theXValue - rowOLD[2] * 3600
#                 rowOLD = cursorOLD.fetchone()
#         case "傾斜Y":
#             # 地表傾斜計(雙軸)-Y
#             # 公式
#             # D方位一觀測值(秒)
#             # theYValue=SX*3600
#             # F方位一累積變位量(秒)(初始值-欄位ParameterBP)
#             # theYAllValue=theYValue-row[7]*3600
#             # H方位一速率(秒/天)
#             # theY1Value=theYValue-rowOLD[2]*3600
#             if theStationID == row[0]:
#                 theYValue = row[5] * 3600
#                 if str(row[7]) == "None":
#                     theYAllValue = 0
#                 else:
#                     theYAllValue = theYValue - row[7] * 3600

#                 QueryDateTime = row[4] - timedelta(days=1)
#                 ssql = (
#                     "SELECT TOP(1) StationID,DataTime,CH3 FROM StationData WHERE DataTime <='"
#                     + str(QueryDateTime)
#                     + "' and  StationID='"
#                     + str(row[0])
#                     + "' ORDER BY  DataTime DESC"
#                 )
#                 cursorOLD.execute(ssql)
#                 rowOLD = cursorOLD.fetchone()
#                 while rowOLD:
#                     theY1Value = theYValue - rowOLD[2] * 3600
#                     rowOLD = cursorOLD.fetchone()
#                 theValue = (
#                     str(theXValue)
#                     + " "
#                     + str(theYValue)
#                     + " "
#                     + str(theXAllValue)
#                     + " "
#                     + str(theYAllValue)
#                     + " "
#                     + str(theX1Value)
#                     + " "
#                     + str(theY1Value)
#                 )
#                 theStationID = ""
#                 print(str(row[6]) + ":" + theValue)
#                 StationName.append(str(row[6]))
#                 StationValue.append(theValue)
#     row = cursor.fetchone()

# # os._exit(0)

# s = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")

# # 更新XML_TN
# amPath = "C:\\FUNCTION\\XML_TN\\dsmon\\am\\"
# theYear = datetime.strftime(now, "%Y")
# theDate = datetime.strftime(now, "%m%d")
# theTime = datetime.strftime(now, "%H%M")
# amhistPath = "C:\\FUNCTION\\XML_TN\\dsmon\\amhist\\" + theYear + "\\" + theDate + "\\"

# XM10MinFile = "10min_a_ds_data.xml"


# # 使用 try 建立amhistPath目錄
# try:
#     os.makedirs(amhistPath)
# # 檔案已存在的例外處理
# except FileExistsError:
#     print(amhistPath + "資料夾已存在。")

# # 更新10MinXML
# tree = XET.parse(amPath + XM10MinFile)  # 以XET套件載入XML檔案
# root = tree.getroot()  # 取得XML表格
# root.set("time", s)
# for node in root.iter("sensor"):
#     SName = node.get("sensorId")
#     for i in range(0, len(StationName), 1):
#         if StationName[i] == SName:
#             node.text = StationValue[i]
#             node.set("time", s)
#     # print([node.tag, node.attrib, node.text])

# # GPS更新10MinXML
# try:

#     with dbinst.getsessionM15()() as session:
#         maxdatetimestring = ""
#         # 取得最新資料的日期
#         sql = text(
#             f" SELECT MAX(datetimestring) as maxdatetimestring FROM [Result10MinData] "
#         )
#         maxresult = session.execute(sql).first()

#         if maxresult is not None:
#             maxdatetimestring = maxresult.maxdatetimestring

#         # SensorType_to_find = ["GPSForecast3db", "BiTiltMeter", "ExtensoMeter"]
#         # Result10MinData1 = (
#         #     session.query(Result10MinData)
#         #     .filter(
#         #         Result10MinData.DatetimeString == maxdatetimestring,
#         #         Result10MinData.DataType.in_(SensorType_to_find),
#         #     )
#         #     .all()
#         # )

#         # 20260224 改用2.0語句
#         # 定義要尋找的感測器類型
#         SensorType_to_find = ["GPSForecast3db", "BiTiltMeter", "ExtensoMeter"]

#         # 1. 建立 select 語句
#         # 在 2.0 中，filter 建議改用 where (雖然 filter 仍可用，但 where 是標準)
#         stmt = select(Result10MinData).where(
#             Result10MinData.DatetimeString == maxdatetimestring,
#             Result10MinData.DataType.in_(SensorType_to_find),
#         )

#         # 2. 執行查詢並取得結果
#         # .scalars() 會將結果從 Row 物件轉回 Model 物件，.all() 取得清單
#         Result10MinData1 = session.execute(stmt).scalars().all()

#         if Result10MinData1 is not None:
#             # 取得所有GPS資料
#             for data in Result10MinData1:
#                 # 比對XML，有對應就更新
#                 for node in root.iter("sensor"):
#                     SName = node.get("sensorId")

#                     if data.SensorID == SName:
#                         node.text = data.value
#                         node.set("time", s)
#                 print(data.SensorID)


# except Exception as e:
#     log_obj.write_log_exception(
#         f"異常內容：{e}",
#         f"發生異常: {type(e).__name__}",
#     )


# tree.write(amPath + XM10MinFile, encoding="UTF-8")
# print(amPath + XM10MinFile + ",更新完成")
# tree.write(amhistPath + theTime + "_" + XM10MinFile, encoding="UTF-8")
# print(amhistPath + theTime + "_" + XM10MinFile + ",製作完成")

# # 20260429 結果寫入資料庫
# # 檔案路徑
# full_xml_path = amPath + XM10MinFile
# # 呼叫 Function
# import_xml_to_sql(full_xml_path)

# # 關閉連線
# cnxn.close()

# # 新增轉檔完成Log
# log_obj.write_log_info(amPath + XM10MinFile + ",更新完成")
# log_obj.write_log_info(amhistPath + theTime + "_" + XM10MinFile + ",製作完成")


def XMLByResult10MinData1():

    # 更新XML_TN
    now = datetime.now()

    XM10MinFile = "10min_a_ds_data.xml"
    # 1. 使用 pathlib 初始化路徑，增加可讀性
    base_path = Path("C:/FUNCTION/XML_TN/dsmon")

    # 產出路徑與備份路徑
    am_path = base_path / "am"
    # 使用 f-string 簡化字串拼接，一次建立層級目錄[cite: 2]
    am_hist_path = base_path / "amhist" / now.strftime("%Y") / now.strftime("%m%d")

    am_hist_path.mkdir(parents=True, exist_ok=True)
    target_file = am_path / XM10MinFile
    backup_file = am_hist_path / f"{now.strftime('%H%M')}_{XM10MinFile}"

    # 1. 執行 SQL 取得最新資料
    query = text("""
        WITH LatestData AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY SensorID ORDER BY DatetimeString DESC) as rn
            FROM [Result10MinData]
        )
        SELECT * FROM LatestData WHERE rn = 1 ORDER BY SiteID, StationID, SensorID
    """)

    with dbinst.getsessionM15()() as session:
        rows = session.execute(query).fetchall()

    # 2. 建立 XML 根節點
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    root = ET.Element(
        "file_attribute",
        file_name="10min_a_ds_data.xml",
        mteam_id="NPUST",
        time=now_str,
    )

    tenmin_data = ET.SubElement(root, "tenmin_a_ds_data")

    # 用於追蹤已建立的節點
    sites = {}
    stations = {}

    for row in rows:
        site_id = row.SiteID
        station_id = row.StationID

        # 3. 處理 site_data 層級
        if site_id not in sites:
            site_node = ET.SubElement(
                tenmin_data, "site_data", siteid=site_id, monitoring_light="Green"
            )
            ET.SubElement(site_node, "factorInfo", factors_num="0")
            sites[site_id] = site_node

        # 4. 處理 station 層級
        station_key = f"{site_id}_{station_id}"
        if station_key not in stations:
            station_node = ET.SubElement(
                sites[site_id], "station", stationId=station_id
            )
            stations[station_key] = station_node

        # 5. 處理 sensor 節點
        sensor_node = ET.SubElement(
            stations[station_key],
            "sensor",
            sensorId=row.SensorID,
            sensor_type=row.DataType,
            observation_num=str(row.observation_num),
            sensor_status=str(row.sensor_status),
            # time=row.DatetimeString,
            # 整個檔案統一時間
            time=now_str,
        )
        sensor_node.text = row.value

    # 6. 輸出格式化 XML
    xml_str = ET.tostring(
        root, pretty_print=True, encoding="utf-8", xml_declaration=False
    )

    target_file = am_path / XM10MinFile
    backup_file = am_hist_path / f"{now.strftime('%H%M')}_{XM10MinFile}"
    with open(target_file, "wb") as f:
        f.write(xml_str)
    print(f"{target_file},更新完成")
    shutil.copy2(target_file, backup_file)
    print(f"{backup_file},製作完成")

    # 新增轉檔完成Log
    log_obj.write_log_info(f"{target_file},更新完成")
    log_obj.write_log_info(f"{backup_file},製作完成")


def XMLByResult10MinData():
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    XM10MinFile = "10min_a_ds_data.xml"

    # 1. 初始化路徑
    base_path = Path("C:/FUNCTION/XML_TN/dsmon")
    am_path = base_path / "am"
    am_hist_path = base_path / "amhist" / now.strftime("%Y") / now.strftime("%m%d")

    target_file = am_path / XM10MinFile

    # 檢查目標檔案是否存在，不存在則無法更新
    if not target_file.exists():
        print(f"錯誤：找不到範本檔案 {target_file}")
        return

    # 2. 執行 SQL 取得資料庫最新資料並轉為字典以利查詢
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
