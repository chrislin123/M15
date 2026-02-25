import sys
import xml.etree.ElementTree as XET

from pathlib import Path
from datetime import datetime

# 資料庫連線相關及Orm.Model
from sqlalchemy import select
from sqlalchemy.sql import text

# from sqlalchemy.orm import session
from db import dbinst, Result10MinData


def main():
    # Todo：未來可能要新增一個補遺的時間區間

    # 1. 定義根目錄 (使用原始字串 r'' 避免反斜線問題)
    base_path = Path(r"C:\FUNCTION\XML\amhist\2026")

    # 2. 使用 rglob 遞迴搜尋所有子資料夾下的 .xml 檔案
    # 這會回傳一個生成器 (generator)，我們將其轉為清單
    xml_files = list(base_path.rglob("*.xml"))

    # 3. 輸出結果
    print(f"總共找到 {len(xml_files)} 個 XML 檔案\n")

    for file_path in xml_files:
        # file_path 是路徑物件，可以直接取得各種屬性
        print(f"檔名: {file_path.name}")
        print(f"完整路徑: {file_path}")
        print(f"上層目錄名稱: {file_path.parent.name}")
        print("-" * 30)

        # 取得XML表格
        tree = XET.parse(file_path)  # 以XET套件載入XML檔案
        root = tree.getroot()

        # 取得檔案時間
        time_text = ""
        try:
            # 假設結構固定為 ...\年\月日\時分_...
            year = file_path.parent.parent.name
            mmdd = file_path.parent.name
            hhmm = file_path.name.split("_")[0]

            # 組合成時間字串並轉換格式
            dt = datetime.strptime(f"{year}{mmdd}{hhmm}", "%Y%m%d%H%M")
            time_text = dt.strftime("%Y-%m-%d %H:%M:%S")

            print(f"檔案: {file_path.name} -> 時間: {time_text}")

        except Exception as e:
            print(f"解析失敗 {file_path.name}: {e}")

        # GPS更新10MinXML
        try:
            with dbinst.getsessionM15()() as session:

                # 20260224 改用2.0語句
                # 定義要尋找的感測器類型
                # SensorType_to_find = ["GPSForecast3db", "BiTiltMeter", "ExtensoMeter"]
                # 先更新 GPSForecast3db
                SensorType_to_find = ["GPSForecast3db"]

                # 1. 建立 select 語句
                # 在 2.0 中，filter 建議改用 where (雖然 filter 仍可用，但 where 是標準)
                stmt = select(Result10MinData).where(
                    Result10MinData.DatetimeString == time_text,
                    Result10MinData.DataType.in_(SensorType_to_find),
                )

                # 2. 執行查詢並取得結果
                # .scalars() 會將結果從 Row 物件轉回 Model 物件，.all() 取得清單
                Result10MinData1 = session.execute(stmt).scalars().all()

                if len(Result10MinData1) == 0:
                    print(f"{time_text}-資料筆數：0 ")

                if Result10MinData1 is not None:
                    # 取得所有GPS資料
                    for data in Result10MinData1:
                        # 比對XML，有對應就更新
                        for node in root.iter("sensor"):
                            SName = node.get("sensorId")

                            if data.SensorID == SName:
                                node.text = data.value
                                node.set("time", time_text)
                                print(f"{data.SensorID}-更新")

        except Exception as e:
            trace_back = sys.exc_info()[2]
            line = trace_back.tb_lineno
            print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)

        # 儲存XML
        tree.write(file_path, encoding="UTF-8")


# 直接執行這個模組，它會判斷為 true
# 從另一個模組匯入這個模組，它會判斷為 false
if __name__ == "__main__":

    main()
