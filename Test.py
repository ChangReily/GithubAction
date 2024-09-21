import sys
import os
import pandas as pd
import datetime
import requests
import json
from bs4 import BeautifulSoup
from io import StringIO
import time
import re
# print (now.isoweekday())



def GetOpenDayList ():
    # Get holiday list
    url = f'https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule'
    r = requests.get(url)
    jsondata = json.loads (r.text)
    df = pd.DataFrame(jsondata)
    now = datetime.datetime.now()
    # update year
    df['Date'] = df.Date.apply(lambda x: str(now.year) + x[-4:])
    HolidayList = df['Date'].to_list()


    Count = 0
    OpenDayList= list ()
    now = datetime.datetime.now()

    for index in range(0, 30, 1):
        CheckDay = now - datetime.timedelta(days=index)
        if CheckDay.isoweekday() >= 6:
            continue
        if CheckDay.strftime('%Y%m%d') in HolidayList:
            continue
        # print (CheckDay.strftime('%Y%m%d'))
        if Count < 10:
            OpenDayList.append(CheckDay.strftime('%Y%m%d'))
            Count=Count+1
        else :
            break

    return OpenDayList


def DownloadCompanyInfo():
    url = 'https://mops.twse.com.tw/mops/web/ajax_t51sb01'
    r = requests.post(url, data={
                            'encodeURIComponent':1,
                            'step':1,
                            'firstin':1,
                            'off':1,
                            'TYPEK':'sii',
                            'code':''})

    r.encoding = 'utf8'
    soup = BeautifulSoup(r.text, 'html.parser')
    Filename=(soup.html.find_all('input', {'name':'filename'}))
    NameList=list()
    for i, name in enumerate(Filename):
        NameList.append(name['value'])
    # print (NameList[0])

    print (f'下載上市公司基本資料彙總表.........')
    r = requests.post(url='https://mops.twse.com.tw/server-java/t105sb02', 
                        data={'firstin':'true',
                            'step':'10',
                            'filename':NameList[0]})
    r.encoding = 'cp950'
    CsvBuffer=r.text   
    r.close()
    df = pd.read_csv(StringIO(CsvBuffer))
    df = df[["公司代號","公司簡稱","產業類別", "實收資本額(元)"]]
    df = df.rename(columns={'公司代號': 'SecurityCode',
                            '公司簡稱': 'CompanySymbol',
                            '產業類別': 'Industry',
                            '實收資本額(元)': 'PaidinCapital',
                            }) 
    df = df[df['PaidinCapital'] >= 2000000000]
    df = df.reset_index(drop = True)
    return df


def DownloadFundInfo():
    url='https://mops.twse.com.tw/mops/web/ajax_t51sb11'
    r = requests.post(url, data={'encodeURIComponent':1,
                                'TYPEK':'sii',
                                'step':0,
                                'run':'Y',
                                'firstin':1,
                                'off':1})
    r.encoding = 'utf8'
    soup = BeautifulSoup(r.text, 'html.parser')
    Filename=(soup.html.find_all('input', {'name':'filename'}))
    NameList=list()
    for i, name in enumerate(Filename):
        NameList.append(name['value'])
    # print (NameList[0])

    print (f'下載基金基本資料彙總表.........')
    r = requests.post(url='https://mops.twse.com.tw/server-java/t105sb02', 
                        data={'firstin':'true',
                            'step':'10',
                            'filename':NameList[0]})
    r.encoding = 'cp950'
    CsvBuffer=r.text        
    r.close()
 
    df = pd.read_csv(StringIO(CsvBuffer.replace("=", "")))
    df = df[["基金代號","標的指數/追蹤指數名稱"]]
    df = df.rename(columns={'基金代號': 'SecurityCode',
                            '標的指數/追蹤指數名稱': 'CompanySymbol',
                            })
    df['Industry'] = 'ETF'
    
    return df

def GetList():
    while(True):
        try:
            HttpsSession = requests.Session()
            cookies = dict(HttpsSession.cookies)
            headers = dict(HttpsSession.headers)
            headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
            resp = HttpsSession.get(f'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2', cookies = cookies, headers = headers)
            if resp.status_code == 200:
                HttpsSession.close()
                break 
        except ConnectionError as err:
            print (f'ERROR: ConnectionError! (Open https://isin.twse.com.tw/isin/C_public.jsp?strMode=2)')
            HttpsSession.close()
            time.sleep(10)
            continue
    df = pd.read_html(resp.text)[0]
    df.columns = df.iloc[0]
    df = df.drop(['備註'], axis=1)
    df = df.rename(columns={'有價證券代號及名稱': 'SecurityCodeName',
                            '國際證券辨識號碼(ISIN Code)': 'ISINCode',
                            '上市日': 'DateListed',
                            '市場別': 'Market',
                            '產業別':'IndustrialGroup'})
    df = df.reset_index().drop(columns='index')
    df['SecurityCodeName']=df['SecurityCodeName'].str.replace('\u3000', ' ')
    tempdf=df['SecurityCodeName'].str.split(' ', n=1, expand=True).rename(columns={0:'SecurityCode', 1:'SecurityName'})
    df = df.merge (tempdf, left_index=True, right_index=True)
    df = df.drop(columns=['SecurityCodeName'])
    df = df[df['SecurityName'].notna()]
    return df

def FilterWarrant(SecurityCode):
    resut = re.match(r'0[3-8][0-9][0-9][0-9][0-9PUTFQCBXY]', SecurityCode)
    if resut:
        # print (SecurityCode)
        return 'V'
    return ''

def GetInfoData():
    CompanyDf = DownloadCompanyInfo()
    CompanyDf.PaidinCapital = pd.to_numeric(CompanyDf.PaidinCapital, errors='coerce')
    # print (CompanyDf)

    FundDf = DownloadFundInfo()
    FundDf['PaidinCapital'] = 0
    # print (FundDf)

    CompanyFundDf = pd.concat([CompanyDf, FundDf])
    CompanyFundDf['SecurityCode'] = CompanyFundDf['SecurityCode'].astype(str)

    TradeListDf = GetList()
    TradeListDf['warrant'] = TradeListDf.apply(lambda x: FilterWarrant(x['SecurityCode']), axis=1)
    TradeListDf = TradeListDf[TradeListDf['warrant'] != 'V']
    TradeListDf = TradeListDf.drop(columns='warrant')
    # print (TradeListDf)

    TotalDf= pd.merge (CompanyFundDf, TradeListDf , how='left', left_on=CompanyFundDf.SecurityCode, right_on=TradeListDf.SecurityCode)
    TotalDf = TotalDf.rename(columns={'key_0':'SecurityCode'}).drop(columns=['SecurityCode_x','SecurityCode_y'])
    TotalDf = TotalDf[['SecurityCode', 'SecurityName', 'Industry', 'PaidinCapital']]
    
    TotalDf['PaidinCapital'] = TotalDf.apply(lambda x: x['PaidinCapital']/100000000, axis=1)
    TotalDf['PaidinCapital'] = TotalDf['PaidinCapital'].round(decimals = 2)   
    TotalDf = TotalDf.dropna()
    TotalDf = TotalDf.reset_index(drop = True)

    # print (TotalDf)
        
    return TotalDf 

if __name__ == '__main__':
    OpenDayList = GetOpenDayList()
    print(OpenDayList)
    
    InfoDf = GetInfoData()
    print (InfoDf)