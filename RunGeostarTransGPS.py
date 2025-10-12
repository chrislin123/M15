import sys
from datetime import datetime, timedelta

# 資料庫相關
# import pymssql
from collections import deque

from sqlalchemy import select
import ProjectLib as ProjectLib


# 資料庫連線相關及Orm.Model
from sqlalchemy.sql import text

# from sqlalchemy.orm import session
from db import dbinst, Result10MinData, GpsBasSetting


from pprint import pprint


# 每日解算(欄位8,9,10)
def getDailyCal(cond):
    # StationID = f'RTK_{cond["StationID"]}'
    StationID = cond["StationID"]
    DatetimeQuery = cond["RAWMaxDatetime"].strftime("%Y-%m-%d %H:%M:%S")

    # 初始值
    DailyCal = {"DailyCal_E": "0", "DailyCal_N": "0", "DailyCal_H": "0"}

    try:
        with dbinst.getsessionGeostar()() as session:

            params = {"DATETIME": DatetimeQuery}
            sql = text(
                f"""
                    DECLARE @GivenDateTime DATETIME = :DATETIME;

                    SELECT sDateTime,
                            x_E_Avg,
                            y_N_Avg,
                            z_H_Avg
                    FROM [{StationID}]

                    WHERE sDateTime >= DATEADD(day, -1, CAST(@GivenDateTime AS DATE))
                    AND sDateTime < CAST(@GivenDateTime AS DATE);

                """
            )

            result = session.execute(sql, params)

            # 劉守恆LINE訊息，計算方式
            # 前一日觀測資料扣掉有問題紀錄的平均值
            # 目前的處理方法是 假設觀測沒特殊狀況的前提下，直接排序然後踢掉(前後)10%極端資料去作平均
            listDailyCal_E = []
            listDailyCal_N = []
            listDailyCal_H = []

            # 轉成 list of dict
            rows = [dict(row._mapping) for row in result]
            # print(rows)

            for (
                row
            ) in rows:  # .scalars() extracts the first column of each row as an object
                # print(row.sDateTime, row.x_E_Avg, row.z_H_Avg)
                listDailyCal_E.append(row["x_E_Avg"])
                listDailyCal_N.append(row["y_N_Avg"])
                listDailyCal_H.append(row["z_H_Avg"])

            # 排序
            listDailyCal_E.sort()
            listDailyCal_N.sort()
            listDailyCal_H.sort()
            # 去頭去尾
            newlistDailyCal_E = listDailyCal_E[14:-14]
            newlistDailyCal_N = listDailyCal_N[14:-14]
            newlistDailyCal_H = listDailyCal_H[14:-14]
            # 計算平均
            DailyCal_E = sum(newlistDailyCal_E) / len(newlistDailyCal_E)
            DailyCal_N = sum(newlistDailyCal_N) / len(newlistDailyCal_N)
            DailyCal_H = sum(newlistDailyCal_H) / len(newlistDailyCal_H)

            DailyCal["DailyCal_E"] = DailyCal_E
            DailyCal["DailyCal_N"] = DailyCal_N
            DailyCal["DailyCal_H"] = DailyCal_H

        return DailyCal
    except Exception as e:
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)
        return DailyCal


def getDisplacementTotal(cond):
    # StationID = f'RTK_{cond["StationID"]}'
    StationID = cond["StationID"]
    DatetimeQuery = cond["RAWMaxDatetime"]
    MinDatetimeQuery = cond["RAWMaxDatetime"]
    # 初始值
    DisplacementTotal = "0"
    try:
        with dbinst.getsessionGeostar()() as session:

            # 該站觀測開始的第一筆資料
            sql = text(f" SELECT MIN(sDateTime) as maxdatetime FROM [{StationID}] ")
            minresult = session.execute(sql).first()

            if minresult is not None:
                MinDatetimeQuery = minresult.maxdatetime

            params = {
                "DATETIME": DatetimeQuery.strftime("%Y-%m-%d %H:%M:%S"),
                "MinDatetimeQuery": MinDatetimeQuery.strftime("%Y-%m-%d %H:%M:%S"),
            }
            sql = text(
                f"""
                    DECLARE @GivenDateTime DATETIME = :DATETIME;
                    DECLARE @MinDatetimeQuery DATETIME = :MinDatetimeQuery;

                    WITH CurrentData AS (
                        -- 選擇當前時間的記錄
                        SELECT 
                            x_E_Avg,
                            y_N_Avg,
                            z_H_Avg
                        FROM [{StationID}]
                        WHERE sDateTime = @GivenDateTime
                    ),
                    FirstData AS (
                        -- 監測開始的第一筆記錄
                        SELECT  top 1
                            x_E_Avg,
                            y_N_Avg,
                            z_H_Avg
                        FROM [{StationID}]
                        WHERE sDateTime = @MinDatetimeQuery
                        ORDER BY sDateTime
                    ) SELECT 
                        c.x_E_Avg,p.x_E_Avg,c.y_N_Avg , p.y_N_Avg,c.z_H_Avg , p.z_H_Avg,
                        -- 三軸變位速率
                        ROUND(SQRT(POWER(c.x_E_Avg - p.x_E_Avg,2)+POWER(c.y_N_Avg - p.y_N_Avg,2)+POWER(c.z_H_Avg - p.z_H_Avg,2))*1000,3) AS DisplacementTotal
                    FROM CurrentData c
                        CROSS JOIN FirstData p;

                    """
            )

            row = session.execute(sql, params).first()

            if row is not None:

                # 累積變位量
                # 屏科-顏志憲LINE訊息，計算方式
                # 欄位7，當下時間的三軸變位量-初始值(監測第一筆資料)的三軸變位量
                # todo 每天一點執行一次
                DisplacementTotal = row.DisplacementTotal

        return DisplacementTotal
    except Exception as e:
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)
        return DisplacementTotal


def CalGps(cond):
    # StationID = f'RTK_{cond["StationID"]}'
    StationID = cond["StationID"]
    cond["RAWMaxDatetime"] = cond["DatetimeQuery"]
    geoResult = ""
    try:
        with dbinst.getsessionGeostar()() as session:

            # 取得最新資料的日期
            sql = text(f" SELECT MAX(sDateTime) as maxdatetime FROM [{StationID}] ")
            maxresult = session.execute(sql).first()

            if maxresult is not None:
                cond["RAWMaxDatetime"] = maxresult.maxdatetime

            # 取得結果
            params = {"DATETIME": cond["RAWMaxDatetime"].strftime("%Y-%m-%d %H:%M:%S")}
            sql = text(
                f"""DECLARE @GivenDateTime DATETIME = :DATETIME;

                    WITH CurrentData AS (
                        -- 選擇當前時間的記錄
                        SELECT 
                            sDateTime,
                            x_E_Avg,
                            y_N_Avg,
                            z_H_Avg,
                            SQRT(POWER(x_E_Avg, 2) + POWER(y_N_Avg, 2) + POWER(z_H_Avg, 2)) AS current_vector
                        FROM [{StationID}]
                        WHERE sDateTime = @GivenDateTime
                    ),
                    PreviousData AS (
                        -- 尋找24小時前最近的記錄
                        SELECT TOP 1
                            sDateTime,
                            x_E_Avg,
                            y_N_Avg,
                            z_H_Avg,
                            SQRT(POWER(x_E_Avg, 2) + POWER(y_N_Avg, 2) + POWER(z_H_Avg, 2)) AS previous_vector
                        FROM [{StationID}]
                        WHERE sDateTime <= DATEADD(HOUR, -24, @GivenDateTime)
                        ORDER BY sDateTime DESC
                    ),
                    VectorData AS (
                        -- 計算向量變化
                        SELECT 
                            c.sDateTime AS CurrentDateTime,
                            c.x_E_Avg AS Current_E,
                            c.y_N_Avg AS Current_N,
                            c.z_H_Avg AS Current_H,
                            c.current_vector AS CurrentVector,
                            p.sDateTime AS PreviousDateTime,
                            p.x_E_Avg AS Previous_E,
                            p.y_N_Avg AS Previous_N,
                            p.z_H_Avg AS Previous_H,
                            p.previous_vector AS PreviousVector,
                            (c.current_vector - p.previous_vector) AS VectorDifference,
                            (c.x_E_Avg - p.x_E_Avg) AS Delta_E,
                            (c.y_N_Avg - p.y_N_Avg) AS Delta_N,
                            (c.z_H_Avg - p.z_H_Avg) AS Delta_H
                        FROM CurrentData c
                        CROSS JOIN PreviousData p
                    )
                    SELECT 
                        *
                        -- 三軸變位速率
                        , ROUND(SQRT(POWER(Delta_E,2)+POWER(Delta_N,2)+POWER(Delta_H,2))*1000,3) AS DisplacementRate_3
                        -- 平面變位速率
                        , ROUND(SQRT(POWER(Delta_E,2)+POWER(Delta_N,2))*1000, 3) AS DisplacementRate_2
                        -- 計算方位角（水平面角度）
                        , (CAST( DEGREES(ATN2(Delta_E, Delta_N)) AS INT) +360  ) % 360  AS AzimuthAngle
                    FROM VectorData;

                    """
            )

            # sql = text()
            # result1 = session.execute(ssql,params)
            # result1 = session.execute(ssql)
            # ttt = result1.fetchone()
            # print(result1)

            row = session.execute(sql, params).first()

            if row is not None:

                # 解算後ENH值
                Cal_E = row.Current_E
                Cal_N = row.Current_N
                Cal_H = row.Current_H

                # 方位角
                AzimuthAngle = row.AzimuthAngle
                # 三軸變位速率
                DisplacementRate_3 = row.DisplacementRate_3
                # 平面變位速率
                DisplacementRate_2 = row.DisplacementRate_2
                # 累積變位量
                # 屏科-顏志憲LINE訊息，計算方式
                # 欄位7，當下時間的三軸變位量-初始值(監測第一筆資料)的三軸變位量
                DisplacementTotal = getDisplacementTotal(cond)

                # 每日解算後ENH值
                DailyCal = getDailyCal(cond)
                DailyCal_E = DailyCal["DailyCal_E"]
                DailyCal_N = DailyCal["DailyCal_N"]
                DailyCal_H = DailyCal["DailyCal_H"]

                # 215953.9566 2545576.9122 348.9318 29.0 9.1 7.4 0 215953.953 2545576.906 348.926
                geoResult = f"{round(Cal_E,3)} {round(Cal_N,3)} {round(Cal_H,3)} {round(AzimuthAngle,3)} {round(DisplacementRate_3,3)} {round(DisplacementRate_2,3)} {round(DisplacementTotal,3)} {round(DailyCal_E,3)} {round(DailyCal_N,3)} {round(DailyCal_H,3)}"

                print(geoResult)

        return geoResult
    except Exception as e:
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)


def insResult10MinData(cond):
    try:
        # Cond = {"StationID":"Moten-4","DatetimeQuery" : "2025-01-05 00:00:00"}
        StationID = cond["StationID"]
        DatetimeQuery = cond["DatetimeQuery"].strftime("%Y-%m-%d %H:%M:%S")
        geoResult = cond["geoResult"]
        with dbinst.getsessionM15()() as session:

            Result10MinData1 = (
                session.query(Result10MinData)
                .filter(
                    Result10MinData.SensorID == StationID,
                    Result10MinData.DatetimeString == DatetimeQuery,
                )
                .first()
            )

            if Result10MinData1 is None:
                Result10MinData1 = Result10MinData()
                Result10MinData1.SiteID = StationID
                Result10MinData1.StationID = StationID
                Result10MinData1.SensorID = StationID
                Result10MinData1.DataType = "GPSForecast3db"
                Result10MinData1.DataName = "GPSForecast3db"
                Result10MinData1.Datetime = DatetimeQuery
                Result10MinData1.DatetimeString = DatetimeQuery
                Result10MinData1.GetTime = ProjectLib.getNowDatetime()
                Result10MinData1.observation_num = "10"
                Result10MinData1.sensor_status = "0"
                Result10MinData1.value = geoResult
                Result10MinData1.remark = ""
                Result10MinData1.CgiData = ""

                session.add(Result10MinData1)
                session.commit()
            else:
                # 更新資料
                Result10MinData1.GetTime = ProjectLib.getNowDatetime()
                Result10MinData1.value = geoResult
                session.commit()

    except Exception as e:
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)


# 取得下一個十分鐘
def get_next_closest_ten_minutes(datetimenow: datetime):
    # now = datetime.datetime.now()
    now = datetimenow
    current_minute = now.minute
    ten_minutes_tick = (current_minute // 10) * 10
    closest_ten_minutes = now.replace(minute=ten_minutes_tick, second=0, microsecond=0)
    closest_ten_minutes = closest_ten_minutes + timedelta(minutes=10)

    return closest_ten_minutes


def main():

    # 取得程式執行時間下一個十分鐘
    datetimenow = get_next_closest_ten_minutes(datetime.now())
    # print(f"最接近的十分钟: {datetimenow}")

    # 六龜三個-'LGN047-G1','LGN047-G2','LGN047-G3'
    # 山地門四個-?
    geoStation = ["LGN047-G1", "LGN047-G2", "LGN047-G3"]
    try:
        with dbinst.getsessionM15()() as session:

            datas = (
                session.query(GpsBasSetting)
                .filter(GpsBasSetting.TableTrans_YN == "Y")
                .all()
            )

            geoStation = [data.TableTrans_MapName for data in datas]
            print(geoStation)

    except Exception as e:
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print("{0}，Error Line:{1}".format(f"Encounter exception: {e}"), line)

    for station in geoStation:
        geoResult = ""
        Cond = {"StationID": station, "DatetimeQuery": datetimenow}
        # StationID = "Moten-4"
        # DatetimeQuery = "2025-01-05 00:00:00"
        # geoResult = ""

        # 取得GNSS十個欄位計算結果
        geoResult = CalGps(Cond)

        Cond["geoResult"] = geoResult

        # 寫入記錄檔
        insResult10MinData(Cond)

        print(f"站點[{station}]-{datetimenow}-轉檔完成")


# 直接執行這個模組，它會判斷為 true
# 從另一個模組匯入這個模組，它會判斷為 false
if __name__ == "__main__":

    main()
