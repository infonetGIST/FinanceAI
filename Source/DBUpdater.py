#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pymysql

import pandas as pd
from bs4 import BeautifulSoup
import requests

from datetime import datetime
import time
import random
import calendar
from threading import Timer
import threading

import json


# In[4]:


class DBUpdater:
    def __init__(self):
        """initiator: connects MariaDB"""
        self.conn = pymysql.connect(host='localhost', user='root', passwd='1111', db='practice', charset='utf8')
        
        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info(
                code VARCHAR(20),
                company VARCHAR(20),
                last_update DATE,
                PRIMARY KEY (code)
            )
            """
            curs.execute(sql)
            
            sql="""
            CREATE TABLE IF NOT EXISTS daily_price(
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date)
            )
            """
            curs.execute(sql)
        self.conn.commit()
        
        self.codes = dict()
        
    def __del__(self):
        """destructor: disconnects MariaDB"""
        self.conn.close()
        
    def read_krx_code(self):
        """Reads list of listed firms from KRX webpage and converts it to a Data frame"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='        'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx
    
    def update_comp_info(self):
        """Updates list of listed firms into company_info table"""
        
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]
            
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last_update)"                    f" VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} REPLACE INTO company_info "                         f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print('')
                
    def read_naver(self, code, company, pages_to_fetch):
        """Reads OHLC data from Naver Finance and converts it to a Data frame"""

        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '               'Chrome/87.0.4280.141 Safari/537.36'}
            r = requests.get(url, headers=headers)
            html = BeautifulSoup(r.text, 'lxml')
            pgrr = html.find("td", class_="pgRR")
            if pgrr is None:
                print('pgrr is None.')
                return None
            s = str(pgrr.a["href"]).split('=')
            lastpage = s[-1]

            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)

            for page in range(1, pages+1):
                t = random.uniform(2, 10)
                print('sleeping for ', t, 'seconds')
                time.sleep(t)
                pg_url = '{}&page={}'.format(url, page)
                r = requests.get(pg_url, headers=headers)
                print('page : ', page, ' ', r)
                page_table = pd.read_html(r.text)[0]
                df = df.append(page_table)
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
                    format(tmnow, company, code, page, pages), end="\n\n")
            df = df.rename(columns={'날짜':'date', '종가':'close', '전일비':'diff',
                                    '시가':'open', '고가':'high', '저가':'low', '거래량':'volume'})
            df['date'] = df['date'].str.replace('.','-')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]

        except Exception as e:
            print('Exception occured : ', str(e))
            return None

        return df
    
    def replace_into_db(self, df, num, code, company):
        """Updates OHLC data into DB"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', "                f"'{r.date}', {r.open}, {r.high}, {r.low}, {r.close}, "                f"{r.diff}, {r.volume})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO '                  'daily_price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), 
                                        num+1, company, code, len(df)))
            
    """def update_daily_price(self, pages_to_fetch):
        #Reads OHLC data of every listed companys and updates it into DB

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])""" # original code
    
    def update_daily_price(self, code, company, pages_to_fetch): # Use this to update DB
        """Reads OHLC data of a company and updates it into DB"""

        df = self.read_naver(code, company, pages_to_fetch)
        if df is None:
            print('df is None.')
        self.replace_into_db(df, 0, code, company)
        
    def execute_daily(self, code, company):    # Just for reference
        """Updates daily_price table at 5 p.m. every day"""

        self.update_comp_info()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch=5
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)

        self.update_daily_price(code, company, pages_to_fetch)

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year+1, month=1, day=1, 
                       hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month+1, day=1, 
                       hour=17, minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day+1, hour=17, minute=0, second=0)

        tmdiff = tmnext-tmnow
        seconds = tmdiff.seconds

        #t = Timer(seconds, self.execute_daily)
        #t.daemon = True
        #print("Waiting for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        #t.start()
        #Set Timer as a demon, add a keyboard trigger


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




