import pymssql
from collections import deque

from ProjectLib import getenv

# urlparse
from urllib.parse import quote_plus

# 資料庫相關sqlalchemy
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Text, DateTime, DECIMAL
from sqlalchemy import select, insert
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.sql import text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession


# 產生資料庫連線的類別
class dbinst:

    def __init__(self) -> None:
        pass

    def getsession():
        connection_format = "mssql+pyodbc://{0}:{1}@{2}/{3}?charset=utf8&driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        connection_str = connection_format.format(
            getenv("DataBaseAccount"),
            quote_plus(getenv("DataBasePassWord")),
            getenv("DataBaseIP"),
            getenv("DataBaseName"),
        )
        # echo標誌是設置SQLAlchemy日誌記錄的快捷方式
        # 使用NullPool，則session結束的時候就關閉連線，否則要等engine關閉或是程式關閉，連線才會停止
        mssql_engine = create_engine(connection_str, echo=False, poolclass=NullPool)
        return sessionmaker(bind=mssql_engine)

    def getsessionM15():
        connection_format = "mssql+pyodbc://{0}:{1}@{2}/{3}?charset=utf8&driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        connection_str = connection_format.format(
            getenv("DataBaseAccountM15"),
            quote_plus(getenv("DataBasePassWordM15")),
            getenv("DataBaseIP"),
            getenv("DataBaseNameM15"),
        )
        # echo標誌是設置SQLAlchemy日誌記錄的快捷方式
        # 使用NullPool，則session結束的時候就關閉連線，否則要等engine關閉或是程式關閉，連線才會停止
        mssql_engine = create_engine(connection_str, echo=False, poolclass=NullPool)
        return sessionmaker(bind=mssql_engine)

    def getsessionGeostar():
        connection_format = "mssql+pyodbc://{0}:{1}@{2}/{3}?charset=utf8&driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        connection_str = connection_format.format(
            getenv("DataBaseAccountGeostar"),
            quote_plus(getenv("DataBasePassWordGeostar")),
            getenv("DataBaseIP"),
            getenv("DataBaseNameGeostar"),
        )
        # echo標誌是設置SQLAlchemy日誌記錄的快捷方式
        # 使用NullPool，則session結束的時候就關閉連線，否則要等engine關閉或是程式關閉，連線才會停止
        mssql_engine = create_engine(connection_str, echo=False, poolclass=NullPool)
        return sessionmaker(bind=mssql_engine)

    def getsessionProcal():
        connection_format = "mssql+pyodbc://{0}:{1}@{2}/{3}?charset=utf8&driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        connection_str = connection_format.format(
            getenv("DataBaseAccountProcal"),
            quote_plus(getenv("DataBasePassWordProcal")),
            getenv("DataBaseIP"),
            getenv("DataBaseNameProcal"),
        )
        # echo標誌是設置SQLAlchemy日誌記錄的快捷方式
        # 使用NullPool，則session結束的時候就關閉連線，否則要等engine關閉或是程式關閉，連線才會停止
        mssql_engine = create_engine(connection_str, echo=False, poolclass=NullPool)
        return sessionmaker(bind=mssql_engine)

    def get_asyncsession():
        # MSSQL 非同步連線需要使用ODBC Driver(aioodbc)
        DB_USER = getenv("DataBaseAccount")
        DB_PASSWORD = getenv("DataBasePassWord")
        DB_HOST = getenv("DataBaseIP")
        DB_NAME = getenv("DataBaseName")
        driver = "ODBC Driver 17 for SQL Server"
        connection_str = (
            f"mssql+aioodbc://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}/{DB_NAME}"
            f"?driver={quote_plus(driver)}"
        )

        # echo標誌是設置SQLAlchemy日誌記錄的快捷方式
        # 使用NullPool，則session結束的時候就關閉連線，否則要等engine關閉或是程式關閉，連線才會停止
        mssql_engine = create_async_engine(
            connection_str, echo=False, poolclass=NullPool
        )
        return sessionmaker(bind=mssql_engine, class_=AsyncSession)


# ================= ORM.Model =================
class Base(DeclarativeBase):
    pass


# 轉檔結果資料表
class Result10MinData(Base):
    __tablename__ = "Result10MinData"
    no = Column(Integer, primary_key=True, autoincrement=True)
    SiteID = Column(Text)
    StationID = Column(Text)
    SensorID = Column(Text)
    DataType = Column(Text)
    DataName = Column(Text)
    Datetime = Column(Text)
    DatetimeString = Column(Text)
    GetTime = Column(Text)
    observation_num = Column(Text)
    sensor_status = Column(Text)
    value = Column(Text)
    remark = Column(Text)
    CgiData = Column(Text)


# GPS轉檔基本設定表
class GpsBasSetting(Base):
    __tablename__ = "GpsBasSetting"
    no = Column(Integer, primary_key=True, autoincrement=True)
    Site = Column(Text)
    Station = Column(Text)
    Sensor = Column(Text)
    TableTrans_MapName = Column(Text)
    TableTrans_YN = Column(Text)
    RenderXML_YN = Column(Text)
    Remark = Column(Text)
    SensorType = Column(Text)
    SensorTypeSim = Column(Text)
    observation_num = Column(Text)


class GeostarRaw(Base):
    __tablename__ = "GeostarRaw"
    no = Column(Integer, primary_key=True, autoincrement=True)
    CurrentDateTime = Column(Text)
    Current_E = Column(Text)
    Current_N = Column(Text)
    Current_H = Column(Text)
    CurrentVector = Column(Text)
    PreviousDateTime = Column(Text)
    Previous_E = Column(Text)
    Previous_N = Column(Text)
    Previous_H = Column(Text)
    PreviousVector = Column(Text)
    VectorDifference = Column(Text)
    Delta_E = Column(Text)
    Delta_N = Column(Text)
    Delta_H = Column(Text)
    AzimuthAngle = Column(Text)


class M15StationData(Base):
    __tablename__ = "M15StationData"
    no = Column(Integer, primary_key=True, autoincrement=True)
    RawID = Column(Text)
    DataTime = Column(DateTime)
    FileName = Column(Text)
    CH1 = Column(DECIMAL)
    CH2 = Column(DECIMAL)
    CH3 = Column(DECIMAL)
    CH4 = Column(DECIMAL)
    CH5 = Column(DECIMAL)
    CH6 = Column(DECIMAL)
    CH7 = Column(DECIMAL)
    CH8 = Column(DECIMAL)
    CH9 = Column(DECIMAL)
    CH10 = Column(DECIMAL)
    GetTime = Column(Text)
