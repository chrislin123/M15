import pymssql
from collections import deque

from ProjectLib import getenv

# urlparse
from urllib.parse import quote_plus

# 資料庫相關sqlalchemy
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    MetaData,
    Text,
    DateTime,
    DECIMAL,
    Numeric,
)
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
    area = Column(Text)
    source = Column(Text)


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


# DB:Procal
class StationReal(Base):
    __tablename__ = "StationReal"
    StationID = Column(String(8), primary_key=True, nullable=False)
    MapCH = Column(Integer, primary_key=True, nullable=False)
    Title = Column(Text)
    Unit = Column(Text)
    DataTime = Column(Text)
    RealVale = Column(Text)
    StationCH = Column(Text)
    StationIDReal = Column(Text)
    ParameterBP = Column(Text)
    ParameterR = Column(Text)
    ParameterC = Column(Text)
    ParameterD = Column(Text)
    ParameterL = Column(Text)
    Alarm1 = Column(Text)
    Alarm2 = Column(Text)
    Alarm3 = Column(Text)
    RealShow = Column(Text)
    MaxValue = Column(Text)
    MinValue = Column(Text)
    Coefficient = Column(Text)
    StationName = Column(Text)


# DB:Procal
class StationData(Base):
    __tablename__ = "StationData"
    StationID = Column(String(8), primary_key=True, nullable=False)
    DataTime = Column(DateTime, primary_key=True, nullable=False)
    FileName = Column(String(60))
    CH1 = Column(Numeric(10, 3))
    CH2 = Column(Numeric(10, 3))
    CH3 = Column(Numeric(12, 5))  # 注意 CH3 的精度較大[cite: 1]
    CH4 = Column(Numeric(10, 3))
    CH5 = Column(Numeric(10, 3))
    CH6 = Column(Numeric(10, 3))
    CH7 = Column(Numeric(10, 3))
    CH8 = Column(Numeric(10, 3))
    CH9 = Column(Numeric(10, 3))
    CH10 = Column(Numeric(10, 3))
    CH11 = Column(Numeric(10, 3))
    CH12 = Column(Numeric(10, 3))
    CH13 = Column(Numeric(10, 3))
    CH14 = Column(Numeric(10, 3))
    CH15 = Column(Numeric(10, 3))
    CH16 = Column(Numeric(10, 3))
    CH17 = Column(Numeric(10, 3))
    CH18 = Column(Numeric(10, 3))
    CH19 = Column(Numeric(10, 3))
    CH20 = Column(Numeric(10, 3))
    CH21 = Column(Numeric(10, 3))
    CH22 = Column(Numeric(10, 3))
    CH23 = Column(Numeric(10, 3))
    CH24 = Column(Numeric(10, 3))
