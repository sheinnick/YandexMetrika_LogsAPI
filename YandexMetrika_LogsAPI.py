#!/usr/bin/env python
# coding: utf-8

# # блокнот с запросами, который получает выгрузки из logs api


import requests
import json
import time
from datetime import datetime
from datetime import date


# # Параметры отчета
# + токен
# + даты  
# + счетчик  
# + поля, которые получим:  
#     + список возможных полей для визитов (сессий) [из хелпа яндекса](https://tech.yandex.ru/metrika/doc/api2/logs/fields/visits-docpage/) 
#     + список возможных полей для просмотров (хитов) [из хелпа яндекса](https://tech.yandex.ru/metrika/doc/api2/logs/fields/hits-docpage/)


token='……………………'     # <<<<<<<<<<<< введите токен
counter = '………………'   # <<<<<<<<<<<< введите номер счетчика
fields = 'ym:s:goalsID,ym:s:visitID,ym:s:counterID,ym:s:dateTimeUTC,ym:s:dateTime,ym:s:clientID,ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastReferalSource,ym:s:lastSearchEngineRoot,ym:s:lastSearchEngine,ym:s:params,ym:s:ipAddress,ym:s:bounce,ym:s:lastSocialNetwork,ym:s:visitDuration,ym:s:pageViews,ym:s:startURL,ym:s:endURL,ym:s:isNewUser,ym:s:regionCityID,ym:s:regionCountry,ym:s:regionCity,ym:s:deviceCategory,ym:s:clientTimeZone,ym:s:UTMCampaign,ym:s:UTMContent,ym:s:UTMMedium,ym:s:UTMSource,ym:s:UTMTerm,ym:s:referer,ym:s:lastDirectClickOrder,ym:s:lastDirectBannerGroup,ym:s:lastDirectClickBanner,ym:s:lastDirectClickOrderName,ym:s:lastClickBannerGroupName,ym:s:lastDirectClickBannerName,ym:s:lastDirectPhraseOrCond,ym:s:lastDirectPlatformType,ym:s:lastDirectPlatform,ym:s:lastDirectConditionType,ym:s:hasGCLID,ym:s:lastGCLID,ym:s:firstGCLID,ym:s:lastSignificantGCLID'
start_date = '2019-01-01' # input('Please enter start date in YYYY-MM-DD format')
end_date = '2019-01-01' # input('Please enter end date in YYYY-MM-DD format')
source = 'visits'


# # функции
headers={'Authorization': f'OAuth {token}'}
#отправляет запрос на создание лога
def create_log(fields, counter, start_date, end_date, token, source):
    url = f'https://api-metrika.yandex.ru/management/v1/counter/{counter}/logrequests?date1={start_date}&date2={end_date}&fields={fields}&source={source}'
    r = requests.post(url,headers=headers)
    if r.status_code == 200:
        print('Log id is', r.json()['log_request']['request_id'])
        return r.json()['log_request']['request_id']
    else:
        print(r.json()) #если статус не 200, то печатаем ответ полностью, возвращаем 'error'
        return 'error'
    
#проверяет готовность логов. возвращает количество частей у готового лога
def check_log(counter, log_id, token):
    if log_id=='error':
        print('create_log вернул error, по этому check_log не выполняем')
        return 'error'
    while True:
        r = requests.get(f'https://api-metrika.yandex.ru/management/v1/counter/{counter}/logrequest/{log_id}',headers=headers)
        if r.status_code == 200:
            if r.json()['log_request']['status'] == 'processed':
                print('Log parts to download - ', len(r.json()['log_request']['parts']))
                return len(r.json()['log_request']['parts'])
            else:
                print(r.json()['log_request']['status'])
        else:
            print('error getting status ',r.json())
            return 'error'
        
        print(str(datetime.now())+' ждем 60 секунд, чтобы снова проверить')
        time.sleep(60) #60 секунд перерыв
        
#скачивает лог (на входе нужен лог id, counter и количество частей (количество частей возвращает check_log))
def download_log(counter, log_id, checker, token):
    if log_id=='error':
        print('create_log вернул error, по этому download_log не выполняем')
        return'error'
    if checker=='error':
        print('check_log вернул error, по этому download_log не выполняем')
        return'error'
    
    for i in range(0, checker):
        r = requests.get(f'https://api-metrika.yandex.ru/management/v1/counter/{counter}/logrequest/{log_id}/part/{i}/download',headers=headers, stream=True)
        if r.status_code == 200:
            with open(f'ym_logsAPI_{counter}_{start_date}_{end_date}_part_{i}.tsv', 'wb') as f:
                # shutil.copyfileobj(r.raw, f)
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk:
                        f.write(chunk)
                print(f'{i} part downloaded')
    print('\n все скачали')
    requests.post(f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{log_id}/clean?',headers=headers)
    print('\n удалили лог после себя')
    
    
#удаляет все логи, которые уже готовы
def clean_ready_logs(counter):
    b=requests.get('https://api-metrika.yandex.net/management/v1/counter/'+counter+'/logrequests?',headers=headers)
    for x in b.json()['requests']:
        print(str(x['request_id']))
        a=requests.post('https://api-metrika.yandex.net/management/v1/counter/'+counter+'/logrequest/'+str(x['request_id'])+'/clean?',headers=headers)
        if a.json().get('log_request',None):
            print('Лог',a.json()['log_request']['request_id'],a.json()['log_request']['status'])
        else:
            print(a.json())

#функции закончились
############################################################################


# # вызываем функции по очереди


#запрос лога → лог ид кладется в переменную log_id  
log_id = create_log(fields, counter, start_date, end_date, token, source)


#проверка готовности лога → если все ок, то количество частей для скачивания кладем в переменную checker
checker = check_log(counter, log_id, token) 


#скачиваем все файлы по очереди
download_log(counter, log_id, checker, token)