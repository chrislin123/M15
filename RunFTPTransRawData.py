import sys
import io
from datetime import datetime, timedelta
from pprint import pprint

import pandas as pd
import requests

# 資料庫連線相關及Orm.Model
from sqlalchemy.sql import text
from db import dbinst, Result10MinData, GpsBasSetting, M15StationData

import ProjectLib as ProjectLib


def DownloadToDB():

    # 參數
    format_string = "%Y/%m/%d %H:%M:%S"
    enc = "utf-8"
    header_row = 16

    SourceBases = []
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_2503.csv",
            "filename": "VD2_2503.csv",
            "stationid": "LGE",
            "machineid": "2503",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6671.csv",
            "filename": "VD2_6671.csv",
            "stationid": "LGT1",
            "machineid": "6671",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6677.csv",
            "filename": "VD2_6677.csv",
            "stationid": "LGT2",
            "machineid": "6677",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6675.csv",
            "filename": "VD2_6675.csv",
            "stationid": "LGT3",
            "machineid": "6675",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6676.csv",
            "filename": "VD2_6676.csv",
            "stationid": "SDMT1",
            "machineid": "6676",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6673.csv",
            "filename": "VD2_6673.csv",
            "stationid": "SDMT2",
            "machineid": "6673",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6672.csv",
            "filename": "VD2_6672.csv",
            "stationid": "SDMT3",
            "machineid": "6672",
        }
    )
    SourceBases.append(
        {
            "url": r"http://111.70.17.18:89/Tiltmeter/VD2_6674.csv",
            "filename": "VD2_6674.csv",
            "stationid": "SDMT4",
            "machineid": "6674",
        }
    )

    for SourceBase in SourceBases:

        # 取得CSV檔案 by web
        url = SourceBase["url"]
        s = requests.get(url).content
        c = pd.read_csv(io.StringIO(s.decode(enc)), skiprows=header_row, dtype=str)

        SourceDatas = []
        for index, row in c.iterrows():
            Sourcedata = M15StationData()
            Sourcedata.RawID = SourceBase["stationid"]
            Sourcedata.DataTime = datetime.strptime(row.iloc[0], format_string)
            Sourcedata.FileName = SourceBase["filename"]
            Sourcedata.CH1 = row.iloc[2]
            Sourcedata.CH2 = row.iloc[3]
            Sourcedata.CH3 = row.iloc[4]
            Sourcedata.CH4 = row.iloc[5]
            Sourcedata.GetTime = ProjectLib.getNowDatetime()
            SourceDatas.append(Sourcedata)

        try:
            with dbinst.getsessionM15()() as session:

                # 取得最新資料時間
                M15StationDataMaxNew = (
                    session.query(M15StationData)
                    .filter(
                        M15StationData.RawID == SourceBase["stationid"],
                    )
                    .order_by(M15StationData.DataTime.desc())
                    .first()
                )

                if M15StationDataMaxNew:
                    print(
                        f'[{Sourcedata.RawID}-{SourceBase["machineid"]}]目前最新時間 : {M15StationDataMaxNew.DataTime}'
                    )

                for Sourcedata in SourceDatas:
                    # pprint(Sourcedata)
                    if Sourcedata.DataTime > M15StationDataMaxNew.DataTime:
                        print(
                            f"{Sourcedata.RawID}-{Sourcedata.DataTime} 大於 目前最新時間:{M15StationDataMaxNew.DataTime}-進行更新"
                        )

                        M15StationDataTemp = (
                            session.query(M15StationData)
                            .filter(
                                M15StationData.RawID == Sourcedata.RawID,
                                M15StationData.DataTime == Sourcedata.DataTime,
                            )
                            .first()
                        )

                        if M15StationDataTemp is None:
                            session.add(Sourcedata)
                            session.commit()

                        print(
                            f'[{Sourcedata.RawID}-{SourceBase["machineid"]}]({Sourcedata.DataTime})-轉檔完成'
                        )

        except Exception as e:
            trace_back = sys.exc_info()[2]
            line = trace_back.tb_lineno
            print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)


def TransToResult10MinData():
    try:
        with dbinst.getsessionM15()() as session:

            # 目前設定轉檔TM、EM
            SensorTypeSim_to_find = ["TM", "EM"]
            GpsBasSettings = (
                session.query(GpsBasSetting)
                .filter(GpsBasSetting.SensorTypeSim.in_(SensorTypeSim_to_find))
                .all()
            )

            # 依照設定檔清單進行轉檔
            for GpsBasSettingRow in GpsBasSettings:

                # 取得程式執行時間下一個十分鐘
                datetimenow = ProjectLib.get_next_closest_ten_minutes(datetime.now())

                # 地表伸縮計nm
                if GpsBasSettingRow.SensorTypeSim == "EM":
                    # 1. 伸張量 數字 mm
                    # 2. 累積變位量 數字 mm
                    # 3. 1日變位量 數字 mm/天 計算當下時間往前24小時的 變位量 (mm/天)，計算公式請參閱註1。
                    # 註1 日變位量(mm/天)=△D =當下時間之伸張量-當下時間前 24 小時之伸張量

                    V1 = 0
                    V2 = 0
                    V3 = 0

                    # 取得資料庫最新資料時間
                    SensorDatatime = (
                        session.query(M15StationData)
                        .filter(
                            M15StationData.RawID == GpsBasSettingRow.TableTrans_MapName,
                        )
                        .order_by(M15StationData.DataTime.desc())
                        .first()
                    )

                    if SensorDatatime:
                        # 1. 伸張量 數字 mm
                        V1 = SensorDatatime.CH1
                        # 2. 累積變位量 數字 mm(一天以前的資料)
                        # 先給V2預設值，預防抓不到資料
                        V2 = SensorDatatime.CH1
                        SensorDatatimeBefore1Day = (
                            session.query(M15StationData)
                            .filter(
                                M15StationData.RawID
                                == GpsBasSettingRow.TableTrans_MapName,
                                M15StationData.DataTime
                                == SensorDatatime.DataTime + timedelta(days=-1),
                            )
                            .order_by(M15StationData.DataTime.desc())
                            .first()
                        )

                        if SensorDatatimeBefore1Day:
                            V2 = SensorDatatimeBefore1Day.CH1
                        # 3. 1日變位量 數字 mm/天(當下時間之伸張量-當下時間前 24 小時之伸張量)
                        V3 = V1 - V2

                        resultValue = f"{V1} {V2} {V3}"

                        # 寫入Result10MinData-EM
                        Result10MinDataEM = (
                            session.query(Result10MinData)
                            .filter(
                                Result10MinData.SiteID == GpsBasSettingRow.Site,
                                Result10MinData.StationID == GpsBasSettingRow.Station,
                                Result10MinData.SensorID == GpsBasSettingRow.Sensor,
                                Result10MinData.Datetime == datetimenow,
                            )
                            .first()
                        )

                        if Result10MinDataEM is None:
                            Result10MinDataEM = Result10MinData()
                            Result10MinDataEM.SiteID = GpsBasSettingRow.Site
                            Result10MinDataEM.StationID = GpsBasSettingRow.Station
                            Result10MinDataEM.SensorID = GpsBasSettingRow.Sensor
                            Result10MinDataEM.DataType = GpsBasSettingRow.SensorType
                            Result10MinDataEM.DataName = GpsBasSettingRow.SensorTypeSim
                            Result10MinDataEM.Datetime = datetimenow
                            Result10MinDataEM.DatetimeString = datetimenow.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            Result10MinDataEM.GetTime = ProjectLib.getNowDatetime()
                            Result10MinDataEM.observation_num = (
                                GpsBasSettingRow.observation_num
                            )
                            Result10MinDataEM.sensor_status = "0"
                            Result10MinDataEM.value = resultValue
                            Result10MinDataEM.remark = f"設備編號：{GpsBasSettingRow.TableTrans_MapName} 來源時間：{SensorDatatime.DataTime}"
                            Result10MinDataEM.CgiData = ""

                            session.add(Result10MinDataEM)
                            session.commit()
                        else:
                            # 更新資料
                            Result10MinDataEM.GetTime = ProjectLib.getNowDatetime()
                            Result10MinDataEM.value = resultValue
                            Result10MinDataEM.remark = f"設備編號：{GpsBasSettingRow.TableTrans_MapName} 來源時間：{SensorDatatime.DataTime}"
                            session.commit()

                        print(
                            f"[Result10MinData]ID:{GpsBasSettingRow.Sensor}[{GpsBasSettingRow.SensorTypeSim}]-時間{datetimenow}-資料{resultValue}-轉檔完成"
                        )

                # 地表傾斜計(雙軸)
                if GpsBasSettingRow.SensorTypeSim == "TM":
                    # 1. 方位一觀測值 數字 "(s) 當下的觀測值
                    # 2. 方位二觀測值 數字 "(s) 當下的觀測值
                    # 3. 方位一累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                    # 4. 方位二累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                    # 5. 方位一1日變位量 數字 s/天計算當下時間往前24小時的方位一變位量(s/天)，計算公式請參閱註1
                    # 6. 方位二1日變位量 數字 s/天計算當下時間往前24小時的方位二變位量(s/天)，計算公式請參閱註2
                    # 註 1 方位一 1 日變位量=△A=當下時間之方位一觀測值-當下時間前 24 小時之方位一觀測值
                    # 註 2 方位二 1 日變位量=△B=當下時間之方位二觀測值-當下時間前 24 小時之方位二觀測值
                    V1 = 0
                    V2 = 0
                    V3 = 0
                    V4 = 0
                    V5 = 0
                    V6 = 0

                    # 取得資料庫最新資料時間
                    SensorDatatime = (
                        session.query(M15StationData)
                        .filter(
                            M15StationData.RawID == GpsBasSettingRow.TableTrans_MapName,
                        )
                        .order_by(M15StationData.DataTime.desc())
                        .first()
                    )

                    if SensorDatatime:
                        # 1. 方位一觀測值 數字 "(s) 當下的觀測值
                        V1 = SensorDatatime.CH1
                        # 2. 方位二觀測值 數字 "(s) 當下的觀測值
                        V2 = SensorDatatime.CH3

                        # 3. 方位一累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                        # 4. 方位二累積變位量 數字 "(s)從觀測開始到現在的累計的數值
                        SensorDatatimeBefore1Day = (
                            session.query(M15StationData)
                            .filter(
                                M15StationData.RawID
                                == GpsBasSettingRow.TableTrans_MapName,
                                M15StationData.DataTime
                                == SensorDatatime.DataTime + timedelta(days=-1),
                            )
                            .order_by(M15StationData.DataTime.desc())
                            .first()
                        )

                        if SensorDatatimeBefore1Day:
                            V3 = SensorDatatimeBefore1Day.CH1
                            V4 = SensorDatatimeBefore1Day.CH3

                        # 5. 方位一1日變位量 數字 s/天計算當下時間往前24小時的方位一變位量(s/天)，計算公式請參閱註1
                        # 6. 方位二1日變位量 數字 s/天計算當下時間往前24小時的方位二變位量(s/天)，計算公式請參閱註2
                        V5 = V1 - V3
                        V6 = V2 - V4

                        resultValue = f"{V1} {V2} {V3} {V4} {V5} {V6}"

                        # 寫入Result10MinData-EM
                        Result10MinDataEM = (
                            session.query(Result10MinData)
                            .filter(
                                Result10MinData.SiteID == GpsBasSettingRow.Site,
                                Result10MinData.StationID == GpsBasSettingRow.Station,
                                Result10MinData.SensorID == GpsBasSettingRow.Sensor,
                                Result10MinData.Datetime == datetimenow,
                            )
                            .first()
                        )

                        if Result10MinDataEM is None:
                            Result10MinDataEM = Result10MinData()
                            Result10MinDataEM.SiteID = GpsBasSettingRow.Site
                            Result10MinDataEM.StationID = GpsBasSettingRow.Station
                            Result10MinDataEM.SensorID = GpsBasSettingRow.Sensor
                            Result10MinDataEM.DataType = GpsBasSettingRow.SensorType
                            Result10MinDataEM.DataName = GpsBasSettingRow.SensorTypeSim
                            Result10MinDataEM.Datetime = datetimenow
                            Result10MinDataEM.DatetimeString = datetimenow.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            Result10MinDataEM.GetTime = ProjectLib.getNowDatetime()
                            Result10MinDataEM.observation_num = (
                                GpsBasSettingRow.observation_num
                            )
                            Result10MinDataEM.sensor_status = "0"
                            Result10MinDataEM.value = resultValue
                            Result10MinDataEM.remark = f"設備編號：{GpsBasSettingRow.TableTrans_MapName} 來源時間：{SensorDatatime.DataTime}"
                            Result10MinDataEM.CgiData = ""

                            session.add(Result10MinDataEM)
                            session.commit()
                        else:
                            # 更新資料
                            Result10MinDataEM.GetTime = ProjectLib.getNowDatetime()
                            Result10MinDataEM.value = resultValue
                            Result10MinDataEM.remark = f"設備編號：{GpsBasSettingRow.TableTrans_MapName} 來源時間：{SensorDatatime.DataTime}"
                            session.commit()

                        print(
                            f"[Result10MinData]ID:{GpsBasSettingRow.Sensor}[{GpsBasSettingRow.SensorTypeSim}]-時間{datetimenow}-資料{resultValue}-轉檔完成"
                        )

    except Exception as e:
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)


# 直接執行這個模組，它會判斷為 true
# 從另一個模組匯入這個模組，它會判斷為 false
if __name__ == "__main__":
    # 下載資料同步到資料庫
    try:  # 使用Try except，預防對方主機沒有連線導致下載程序異常影響後面轉檔
        DownloadToDB()
    except Exception as e:
        pass

    # 轉檔至Result10MinData
    TransToResult10MinData()
