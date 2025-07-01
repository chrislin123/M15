import pyodbc
import os
import xml.etree.ElementTree as XET
from datetime import datetime, timedelta

# using now() to get current time
now = datetime.now()

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'DESKTOP-5CGSQOL\SQLEXPRESS'  # to specify an alternate port
server = 'BV550234820001\SQLEXPRESS' 
database = 'Procal' 
username = 'npust' 
password = 'swcb@npust2024!'
# ENCRYPT defaults to yes starting in ODBC Driver 17. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
# connstring='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
connstring='DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
print('連接資料庫:'+'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database)
cnxn = pyodbc.connect(connstring)
cursor = cnxn.cursor()



#Sample select query
#20250509 新增判斷StationName <> 'False'，避免抓取資料內容預設值有異常
# cursor.execute("SELECT  StationID, MapCH, Title, Unit, DataTime, RealVale, StationName, ParameterBP, ParameterR FROM StationReal WHERE StationName <> '' ORDER BY  StationID") 
cursor.execute("SELECT  StationID, MapCH, Title, Unit, DataTime, RealVale, StationName, ParameterBP, ParameterR FROM StationReal WHERE StationName <> '' and StationName <> 'False' ORDER BY  StationID") 
#測試
#cursor.execute("SELECT  StationID, MapCH, Title, Unit, DataTime, RealVale, StationName, ParameterBP, ParameterR FROM StationReal WHERE StationName in ('23D64T02','','','') ORDER BY  StationID") 
row = cursor.fetchone()
theXValue="" #X傾斜
theYValue="" #Y傾斜
theX1Value="" #X1天傾斜變化
theY1Value="" #Y1天傾斜變化
theStationID="" #傾斜ID
StationName=[] #測站名稱
StationValue=[] #更新值
while row: 
    print(row[0]+","+str(row[1])+","+str(row[2])+","+str(row[3])+","+str(row[4])+","+str(row[5])+","+str(row[6]))
    cnxn2 = pyodbc.connect(connstring)
    cursorOLD = cnxn2.cursor()
    theValue="" #更新值
    match str(row[2]):
        case "地下水位":
            theValue=str(row[5]-row[7])+" "+str(row[5]-row[7]-row[8])
            #theValue=str(row[5])
            #print(str(row[6]) +":"+ theValue)
            StationName.append(str(row[6]))
            StationValue.append(theValue)  
        case "伸縮計":
            #產生查詢時間(一天前)
            QueryDateTime=row[4]-timedelta(days=1)
            #產生查詢時間
            cursorOLD.execute("SELECT TOP(1) StationID,DataTime,CH1 FROM StationData WHERE DataTime <='"+ str(QueryDateTime) +"' and  StationID='"+str(row[0]) +"' ORDER BY  DataTime DESC")
            rowOLD = cursorOLD.fetchone()
            while rowOLD: 
                #print(rowOLD[0]+","+str(rowOLD[1])+","+str(rowOLD[2])+","+str(rowOLD[3])+","+str(rowOLD[4]))
                theValue=str(row[5])+" "+ str(row[5])+" "+ str(row[5]-rowOLD[2])
                rowOLD = cursorOLD.fetchone()
            #print(str(row[6]) +":"+ theValue)
            StationName.append(str(row[6]))
            StationValue.append(theValue)  
        case "傾斜X":
            # 地表傾斜計(雙軸)-X
            #公式
            # D方位一觀測值(秒)
            # theXValue=SX*3600
            # F方位一累積變位量(秒)(初始值-欄位ParameterBP)
            # theXAllValue=theXValue-row[7]*3600
            # H方位一速率(秒/天)
            # theX1Value=theXValue-rowOLD[2]*3600
            theStationID=row[0] #傾斜ID            
            theXValue=row[5]*3600
            if str(row[7])=="None" :
                theXAllValue=0
            else :
                theXAllValue=theXValue-row[7]*3600
            QueryDateTime=row[4]-timedelta(days=1)
            ssql = "SELECT TOP(1) StationID,DataTime,CH1 FROM StationData WHERE DataTime <='"+ str(QueryDateTime) +"' and  StationID='"+str(row[0]) +"' ORDER BY  DataTime DESC"
            cursorOLD.execute(ssql)
            rowOLD = cursorOLD.fetchone()
            while rowOLD:
                  print(rowOLD)
                  theX1Value=theXValue-rowOLD[2]*3600
                  rowOLD = cursorOLD.fetchone()          
        case "傾斜Y":
            # 地表傾斜計(雙軸)-Y
            # 公式
            # D方位一觀測值(秒)
            # theYValue=SX*3600
            # F方位一累積變位量(秒)(初始值-欄位ParameterBP)
            # theYAllValue=theYValue-row[7]*3600
            # H方位一速率(秒/天)
            # theY1Value=theYValue-rowOLD[2]*3600
            if theStationID==row[0] :
                theYValue=row[5] *3600
                if str(row[7])=="None" :
                    theYAllValue=0
                else :
                    theYAllValue=theYValue-row[7]*3600
                    
                QueryDateTime=row[4]-timedelta(days=1)
                ssql = "SELECT TOP(1) StationID,DataTime,CH3 FROM StationData WHERE DataTime <='"+ str(QueryDateTime) +"' and  StationID='"+str(row[0]) +"' ORDER BY  DataTime DESC"
                cursorOLD.execute(ssql)
                rowOLD = cursorOLD.fetchone()
                while rowOLD:
                    theY1Value=theYValue-rowOLD[2]*3600
                    rowOLD = cursorOLD.fetchone()
                theValue=str(theXValue) + " " +str(theYValue)+ " " +str(theXAllValue) + " " +str(theYAllValue)+ " " +str(theX1Value)+ " " +str(theY1Value)
                theStationID=""
                print(str(row[6]) +":"+ theValue)
                StationName.append(str(row[6]))
                StationValue.append(theValue)     
    row = cursor.fetchone()
    
#os._exit(0)

s = datetime.strftime(now,'%Y-%m-%d %H:%M:%S')

#更新XML
amPath="C:\\FUNCTION\\XML\\am\\"
theYear=datetime.strftime(now,'%Y')
theDate= datetime.strftime(now,'%m%d')
theTime= datetime.strftime(now,'%H%M')
amhistPath="C:\\FUNCTION\\XML\\amhist\\"+theYear +"\\"+ theDate+"\\"

XM10MinFile="10min_a_ds_data.xml"


# 使用 try 建立amhistPath目錄
try:
  os.makedirs(amhistPath)
# 檔案已存在的例外處理
except FileExistsError:
  print(amhistPath+"資料夾已存在。")
    
#更新10MinXML
tree = XET.parse(amPath+XM10MinFile)  # 以XET套件載入XML檔案
root = tree.getroot()         # 取得XML表格
root.set('time',s)
for node in root.iter('sensor'):
    SName=node.get('sensorId')
    for i in range(0,len(StationName),1):
        if StationName[i]==SName :
            node.text= StationValue[i]
            node.set('time',s)            
    # print([node.tag, node.attrib, node.text])
tree.write(amPath+XM10MinFile, encoding='UTF-8')
print(amPath+XM10MinFile+",更新完成")
tree.write(amhistPath+theTime+"_"+XM10MinFile, encoding='UTF-8')
print(amhistPath+theTime+"_"+XM10MinFile+",製作完成")




