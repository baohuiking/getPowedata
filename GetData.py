# coding:utf-8
import pandas as pd
import os
import datetime
import http.client
import urllib
import json
from io import *
from .. import config

#最终数据写入地址
DATAPATH = config.Config.DATAPATH

print(DATAPATH)

GET = 'GET'
POST = 'POST'
CITY_CODE = "370100"
url = 'jngffp.cn'

'''
    通过请求的方式调用api接口
    date:2017-8-3 14:40:01

'''

def getJsonData(method, uri, body=None):
    connection = http.client.HTTPConnection('jngffp.cn')  # 设置请求主机地址，如果不是80端口，后面多加一个参数
    requestHeaders = {'Content-Type': 'text/html;charset=UTF-8',
                      'Accept': 'text/html,application/xhtml+xml, */*'}  # 设置返回值类型
    connection.request(method, uri, body, requestHeaders)  # 开始请求
    response = connection.getresponse()  # 接收返回值
    print("状态:", response.status)  # 返回状态
    datas = response.read();  # 读取返回值
    return datas

'''
    获取所有电站ID和经纬度信息
    获取内容：
    stationCode：电站编码，
    changZhanId：电站ID，
    titile：电站名称，
    还有最后经纬度，
    最后把数据保存到本地
'''
def getPositionAll():
    urlPosition = 'http://jngffp.cn/getMapController/mapData?StationStat=0&provinceIds=284'
    print(urlPosition)
    result = getJsonData(POST, urlPosition)  # 调用请求函数
    # print(type(BytesIO(result)))
    # dataWeatherInfo = json.load(StringIO(result))["data"]  # 请求返回的字符串转换为json字符串,result必须是str or none
    dataPosition = json.load(BytesIO(result))["attributes"]["mapdata"]  # 请求返回的字符串转换为json字符串
    #取得信息，并保存到本地
    df = pd.DataFrame(dataPosition)
    df = df.sort_values(by='changZhanId')
    df = df.reset_index()
    df.drop(['index'], axis=1)
    station = df[['stationCode', 'changZhanId', 'title']]
    # 把Series转为list获取其中的字典
    list_temp = df['position'].values.tolist()
    temp = pd.DataFrame(list_temp)
    station['longitude'] = temp['lng']
    station['latitude'] = temp['lat']
    file = os.path.join(DATAPATH, 'station.csv')
    station.to_csv(file, index=False)

'''
    根据相应条件获取天气
    date:2017-8-3 15:12:06
'''

def getWeatherInfo(nowDate, cityID):
    paramWeatherInfo = {"city_Code": cityID, "startTime": nowDate + " 07",
                        "endTime": nowDate + " 18"}  # 传递参数   为什么时间选择7点到18点，因为这个数据库存储的时间是天气更新时间
    bodyWeatherInfo = urllib.parse.urlencode(paramWeatherInfo)  # 参数转码
    urlWeatherInfo = 'http://' + url + '/getSolarDataByPython/getWeatherList?' + bodyWeatherInfo
    print(urlWeatherInfo)
    result = getJsonData(POST, urlWeatherInfo)  # 调用请求函数
    # dataWeatherInfo = json.load(StringIO(result))["data"]  # 请求返回的字符串转换为json字符串,result必须是str or none
    dataWeatherInfo = json.load(BytesIO(result))["data"]  # 请求返回的字符串转换为json字符串
    # print(dataWeatherInfo)
    return dataWeatherInfo


'''
    获取日电量
    date:2017-8-3 14:40:01
'''
def getPower(forecastDate, regionId):
    paramPower = {"regionType": 5, "regionId": regionId, "dataType": 2, "batchType": 0,
                  "dayTime": forecastDate}  # 传递参数   为什么时间选择7点到18点，因为这个数据库存储的时间是天气更新时间
    bodyPower = urllib.parse.urlencode(paramPower)  # 参数转码
    urlPower = 'http://' + url + '/getSolarDataByPython/getSimilardayPower?' + bodyPower
    print(urlPower)
    result = getJsonData(POST, urlPower)  # 调用请求函数
    if (len(result) > 2):  # 返回为{}len(result)=2
        # print(result)
        data = json.load(BytesIO(result))["data"]  # 请求返回的字符串转换为json字符串
        return data
    else:
        print("为空")
        return None


'''
    获取所有电站2017-1-1 至 2020-3-1的日发电量
    获取内容：电站ID，time，日发电量，容量
'''
def getPowerAll():
    station=pd.read_csv(file,encoding='GB2312')
    regionId=station['changZhanId']
    startDate='2017-1-1 22'
    endDate='2020-3-1 22'
    startDate = datetime.datetime.strptime(startDate, '%Y-%m-%d %H')
    endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d %H')
    for id in regionId:
        print(id)
        #if(id<400):
            #continue
        i = datetime.timedelta(days=1)
        dayPowerAll=pd.DataFrame(columns=['id','time','dayPower','Capacity'])
        while i <= (endDate - startDate + datetime.timedelta(days=1)):
            listdata = startDate + i-datetime.timedelta(days=1)#在date1的基础上加i天
            date=(listdata).strftime('%Y-%m-%d %H')
            #print(date)
            dayPower=getPower(date,id)
            if(dayPower==None):
                #默认值
                dayPower=pd.DataFrame([[str(id),date,0,50]],columns=['id','time','dayPower','Capacity'])
                i += datetime.timedelta(days=1)#i++
                dayPowerAll=pd.concat([dayPowerAll,dayPower])
            else:
                dayPower=pd.DataFrame(dayPower)[['id','time','dayPower','Capacity']]
                i += datetime.timedelta(days=1)#i++
                dayPowerAll=pd.concat([dayPowerAll,dayPower])
        dayPowerAll=dayPowerAll[['id','time','dayPower']]
        dayPowerAll.to_csv('../datasets/PowerData/station' + str(id) + '.csv',index=False)

'''
    获得所有天气信息
    获取内容：
    city_Code:城市编码
    city_Name:城市名字
    weatherCode:天气编码
    weatherTypeName:天气别称
    humidity:湿度
    temperature:温度
    wind:风向和风速
'''
def getWeatherAll():
    weatherInfo = pd.DataFrame(
        columns=['city_Code', 'city_Name', 'time', 'weatherCode', 'weatherTypeName', 'humidity', 'temperature', 'wind'])
    startDate = '2017-1-1'
    endDate = '2020-3-1'
    startDate = datetime.datetime.strptime(startDate, '%Y-%m-%d')
    endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
    i = datetime.timedelta(days=1)
    while i <= (endDate - startDate + datetime.timedelta(days=1)):
        listdata = startDate + i - datetime.timedelta(days=1)  # 在date1的基础上加i天
        date = (listdata).strftime('%Y-%m-%d')
        i += datetime.timedelta(days=1)  # i++

        historyWeather = getWeatherInfo(date, '370100')
        if len(historyWeather) == 0:
            print(date, '没有数据')
            historyWeather = pd.DataFrame([['370100', '济南', date, 0, 0, 0, 0, 0]],
                                          columns=['city_Code', 'city_Name', 'time', 'weatherCode', 'weatherTypeName',
                                                   'humidity', 'temperature', 'wind'])
            historyWeather = historyWeather[
                ['city_Code', 'city_Name', 'time', 'weatherCode', 'weatherTypeName', 'humidity', 'temperature', 'wind']]
            weatherInfo = pd.concat([weatherInfo, historyWeather])
            continue
        else:
            historyWeather = pd.DataFrame(historyWeather)
            historyWeather = historyWeather[
                ['city_Code', 'city_Name', 'time', 'weatherCode', 'weatherTypeName', 'humidity', 'temperature', 'wind']]
            weatherInfo = pd.concat([weatherInfo, historyWeather])
    weatherInfo.to_csv('../datasets/weather.csv', index=False)
#getWeatherAll()