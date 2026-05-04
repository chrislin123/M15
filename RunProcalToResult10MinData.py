import sys
import io
from datetime import datetime, timedelta
from pprint import pprint

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 資料庫連線相關及Orm.Model
from sqlalchemy.sql import text
from db import dbinst, Result10MinData, GpsBasSetting, StationReal, StationData

import ProjectLib as ProjectLib

# Logger
from logger import get_logger

log_obj = get_logger()


def upsert_result_10min_data(
    session, setting_row, datetimenow, result_value, source_time
):
    """
    處理 Result10MinData 的寫入或更新 (Upsert)
    """
    # 1. 查詢是否已有重複資料
    record = (
        session.query(Result10MinData)
        .filter(
            Result10MinData.SiteID == setting_row.Site,
            Result10MinData.StationID == setting_row.Station,
            Result10MinData.SensorID == setting_row.Sensor,
            Result10MinData.Datetime == datetimenow,
        )
        .first()
    )

    remark_content = (
        f"[procal]設備編號：{setting_row.TableTrans_MapName} 來源時間：{source_time}"
    )

    if record is None:
        # 2. 執行 Insert[cite: 2]
        record = Result10MinData()
        record.SiteID = setting_row.Site
        record.StationID = setting_row.Station
        record.SensorID = setting_row.Sensor
        record.DataType = setting_row.SensorType
        record.DataName = setting_row.SensorTypeSim
        record.Datetime = datetimenow
        record.DatetimeString = datetimenow.strftime("%Y-%m-%d %H:%M:%S")
        record.observation_num = setting_row.observation_num
        record.sensor_status = "0"
        record.CgiData = ""

        # 共同更新欄位
        record.value = result_value
        record.GetTime = ProjectLib.getNowDatetime()
        record.remark = remark_content

        session.add(record)
    else:
        # 3. 執行 Update[cite: 2]
        record.value = result_value
        record.GetTime = ProjectLib.getNowDatetime()
        record.remark = remark_content

    # 注意：建議將 commit 移至迴圈外以提升效能
    session.commit()
    return record


def TransProcalToResult10MinData():
    try:
        with dbinst.getsessionM15()() as session, dbinst.getsessionProcal()() as sessionProcal:

            # 目前設定轉檔procal盛邦儀器
            SensorTypeSim_to_find = ["procal"]
            GpsBasSettings = (
                session.query(GpsBasSetting)
                .filter(
                    GpsBasSetting.source.in_(SensorTypeSim_to_find),
                )
                .all()
            )

            # 依照設定檔清單進行轉檔
            for GpsBasSettingRow in GpsBasSettings:
                GpsBasSettingRow: GpsBasSetting

                # 取得程式執行時間下一個十分鐘
                datetimenow = ProjectLib.get_next_closest_ten_minutes(datetime.now())

                # [EM]地表伸縮計
                if GpsBasSettingRow.SensorTypeSim == "EM":
                    # 1. 伸張量 數字 mm
                    # 2. 累積變位量 數字 mm
                    # 3. 1日變位量 數字 mm/天 計算當下時間往前24小時的 變位量 (mm/天)，計算公式請參閱註1。
                    # 註1 日變位量(mm/天)=△D =當下時間之伸張量-當下時間前 24 小時之伸張量

                    V1 = 0
                    V2 = 0
                    V3 = 0

                    # 取得資料庫最新資料時間
                    StationRealDatas = (
                        sessionProcal.query(StationReal)
                        .filter(
                            StationReal.StationID
                            == GpsBasSettingRow.TableTrans_MapName,
                            StationReal.Title == "伸縮計",
                        )
                        .order_by(StationReal.DataTime.desc())
                        .all()
                    )

                    for StationRealDataRow in StationRealDatas:
                        StationRealDataRow: StationReal

                        # 先給預設值
                        # 1. 伸張量 數字 mm
                        V1 = StationRealDataRow.RealVale
                        # 2. 累積變位量 數字 mm(一天以前的資料)
                        # 先給V2預設值，預防抓不到資料
                        V2 = StationRealDataRow.RealVale
                        # 3. 1日變位量 數字 mm/天(當下時間之伸張量-當下時間前 24 小時之伸張量)
                        V3 = V1 - V2

                        station_data_row = (
                            sessionProcal.query(StationData)
                            .filter(
                                StationData.StationID
                                == GpsBasSettingRow.TableTrans_MapName,
                                StationData.DataTime
                                <= StationRealDataRow.DataTime - timedelta(days=1),
                            )
                            .order_by(StationData.DataTime.desc())
                            .first()
                        )

                        if station_data_row:
                            station_data_row: StationData

                            # 2. 累積變位量 數字 mm(一天以前的資料)
                            V2 = station_data_row.CH1
                            # 3. 1日變位量 數字 mm/天(當下時間之伸張量-當下時間前 24 小時之伸張量)
                            V3 = V1 - V2

                        resultValue = f"{V1} {V2} {V3}"

                        # 寫入Result10MinData-EM
                        upsert_result_10min_data(
                            session=session,
                            setting_row=GpsBasSettingRow,
                            datetimenow=datetimenow,
                            result_value=resultValue,
                            source_time=StationRealDataRow.DataTime,
                        )

                        print(
                            f"[Result10MinData]ID:{GpsBasSettingRow.Sensor}[{GpsBasSettingRow.SensorTypeSim}]-時間{datetimenow}-資料{resultValue}-轉檔完成"
                        )

                # [TM]地表傾斜計(雙軸)
                if GpsBasSettingRow.SensorTypeSim == "TM":
                    # 1. 方位一觀測值 數字 "(s) 當下的觀測值
                    # 2. 方位二觀測值 數字 "(s) 當下的觀測值
                    # 3. 方位一累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                    # 4. 方位二累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                    # 5. 方位一1日變位量 數字 s/天計算當下時間往前24小時的方位一變位量(s/天)，計算公式請參閱註1
                    # 6. 方位二1日變位量 數字 s/天計算當下時間往前24小時的方位二變位量(s/天)，計算公式請參閱註2
                    # 註 1 方位一 1 日變位量=△A=當下時間之方位一觀測值-當下時間前 24 小時之方位一觀測值
                    # 註 2 方位二 1 日變位量=△B=當下時間之方位二觀測值-當下時間前 24 小時之方位二觀測值
                    # ***舊系統V1-V6都有乘上*3600，目前比照處理
                    V1 = 0
                    V2 = 0
                    V3 = 0
                    V4 = 0
                    V5 = 0
                    V6 = 0

                    # 取得資料庫最新資料時間
                    StationRealDatas = (
                        sessionProcal.query(StationReal)
                        .filter(
                            StationReal.StationID
                            == GpsBasSettingRow.TableTrans_MapName,
                            StationReal.Title.in_(["傾斜X", "傾斜Y"]),
                        )
                        .order_by(StationReal.DataTime.desc())
                        .all()
                    )

                    for StationRealDataRow in StationRealDatas:
                        StationRealDataRow: StationReal

                        if StationRealDataRow.Title == "傾斜X":
                            # 1. 方位一觀測值 數字 "(s) 當下的觀測值
                            V1 = StationRealDataRow.RealVale
                            # 3. 方位一累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                            station_data_row = (
                                sessionProcal.query(StationData)
                                .filter(
                                    StationData.StationID
                                    == GpsBasSettingRow.TableTrans_MapName,
                                    StationData.DataTime
                                    <= StationRealDataRow.DataTime - timedelta(days=1),
                                )
                                .order_by(StationData.DataTime.desc())
                                .first()
                            )

                            if station_data_row:
                                station_data_row: StationData
                                V3 = station_data_row.CH1

                        if StationRealDataRow.Title == "傾斜Y":
                            # 2. 方位二觀測值 數字 "(s) 當下的觀測值
                            V2 = StationRealDataRow.RealVale
                            # 4. 方位二累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                            station_data_row = (
                                sessionProcal.query(StationData)
                                .filter(
                                    StationData.StationID
                                    == GpsBasSettingRow.TableTrans_MapName,
                                    StationData.DataTime
                                    <= StationRealDataRow.DataTime - timedelta(days=1),
                                )
                                .order_by(StationData.DataTime.desc())
                                .first()
                            )

                            if station_data_row:
                                station_data_row: StationData
                                V4 = station_data_row.CH3

                    # 5. 方位一1日變位量 數字 s/天計算當下時間往前24小時的方位一變位量(s/天)，計算公式請參閱註1
                    # 6. 方位二1日變位量 數字 s/天計算當下時間往前24小時的方位二變位量(s/天)，計算公式請參閱註2
                    V5 = V1 - V3
                    V6 = V2 - V4

                    resultValue = (
                        f"{V1*3600} {V2*3600} {V3*3600} {V4*3600} {V5*3600} {V6*3600}"
                    )

                    # 寫入Result10MinData-TM
                    upsert_result_10min_data(
                        session=session,
                        setting_row=GpsBasSettingRow,
                        datetimenow=datetimenow,
                        result_value=resultValue,
                        source_time=StationRealDataRow.DataTime,
                    )

                    print(
                        f"[Result10MinData]ID:{GpsBasSettingRow.Sensor}[{GpsBasSettingRow.SensorTypeSim}]-時間{datetimenow}-資料{resultValue}-轉檔完成"
                    )

                # [GW]水位觀測井
                if GpsBasSettingRow.SensorTypeSim == "GW":
                    # 1. 水位高     數字 M 地表以下為負值，例如”-20”。
                    # 2. 相對水位高 數字 M 與常時水位的差值

                    V1 = 0
                    V2 = 0

                    # 取得資料庫最新資料時間
                    StationRealDatas = (
                        sessionProcal.query(StationReal)
                        .filter(
                            StationReal.StationID
                            == GpsBasSettingRow.TableTrans_MapName,
                            StationReal.Title == "地下水位",
                        )
                        .order_by(StationReal.DataTime.desc())
                        .all()
                    )

                    for StationRealDataRow in StationRealDatas:
                        StationRealDataRow: StationReal

                        # 1. 水位高     數字 M 地表以下為負值
                        # 依照舊系統規則row[5] - row[7]
                        V1 = (
                            StationRealDataRow.RealVale - StationRealDataRow.ParameterBP
                        )
                        # 2. # 2. 相對水位高 數字 M 與常時水位的差值
                        # 依照舊系統規則row[5] - row[7] - row[8]
                        V2 = (
                            StationRealDataRow.RealVale
                            - StationRealDataRow.ParameterBP
                            - StationRealDataRow.ParameterR
                        )

                        resultValue = f"{V1} {V2}"

                        # 寫入Result10MinData-GW
                        upsert_result_10min_data(
                            session=session,
                            setting_row=GpsBasSettingRow,
                            datetimenow=datetimenow,
                            result_value=resultValue,
                            source_time=StationRealDataRow.DataTime,
                        )

                        print(
                            f"[Result10MinData]ID:{GpsBasSettingRow.Sensor}[{GpsBasSettingRow.SensorTypeSim}]-時間{datetimenow}-資料{resultValue}-轉檔完成"
                        )

    except Exception as e:
        log_obj.write_log_exception(
            f"異常內容：{e}",
            f"發生異常: {type(e).__name__}",
        )
        print(f"An error occurred: {e}")


# 直接執行這個模組，它會判斷為 true
# 從另一個模組匯入這個模組，它會判斷為 false
if __name__ == "__main__":

    try:
        # Procal轉檔至Result10MinData
        TransProcalToResult10MinData()

        log_obj.write_log_info("TransProcalToResult10MinData,執行完成")
    except Exception as e:
        log_obj.write_log_exception(
            f"異常內容：{e}",
            f"發生異常: {type(e).__name__}",
        )
