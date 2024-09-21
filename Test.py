import sys
import pandas as pd
import datetime
import requests
import json
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

if __name__ == '__main__':
    OpenDayList = GetOpenDayList()
    print(OpenDayList)
    print(OpenDayList)
    print(OpenDayList)
    print(OpenDayList)
    print(OpenDayList)
    output_file = os.getenv('GITHUB_OUTPUT')  
    with open(output_file, "a") as myfile:
        myfile.write(f"changelog={OpenDayList}")
