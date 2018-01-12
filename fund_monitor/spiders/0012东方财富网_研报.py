# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: proxy_spider.py
    @time: 2017/3/9 16:27
--------------------------------
"""
import sys
import os

import pandas as pd
import scrapy
import fund_monitor.items
import re
import numpy as np
import traceback
import datetime
import bs4
import pymysql
from contextlib import closing
import time
import json


log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')

import driver_manager
driver_manager = driver_manager.driver_manager()
import requests_manager
requests_manager = requests_manager.requests_manager()
import mysql_connecter
mysql_connecter = mysql_connecter.mysql_connecter()
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger(log_path, set_log.logging.WARNING, set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=True) # 是否需要在每次运行程序前清空Log文件


file_path = os.getcwd() + u'/研报/'
# if os.path.exists(file_path):
#     shutil.rmtree(file_path)
    # os.remove(file_path)


class Spider(scrapy.Spider):
    name = "0012"

    def __init__(self, file_name, date1=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                                  date2=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')):
        self.date1 = datetime.datetime.strptime(date1, '%Y-%m-%d').replace(hour=0,minute=0,second=0,microsecond=0)
        self.date2 = datetime.datetime.strptime(date2, '%Y-%m-%d').replace(hour=23,minute=59,second=59,microsecond=0)
        # if self.date1 == self.date2
        print 'self.date1', self.date1
        print 'self.date2', self.date2

        with open(file_name, 'r') as f:
            self.stock_dict = json.load(f)

        self.stock_list = self.stock_dict.keys()
        print u'使用的股票池为:', file_name

    def start_requests(self):
        url = 'http://datainterface.eastmoney.com//EM_DataCenter/js.aspx?type=SR&sty=GGSR&ps=8000&p=1&mkt=0&stat=0&cmd=2&code=&rt=50504751'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            date_today = datetime.datetime.now()
            date = datetime.datetime(year=date_today.year, month=date_today.month, day=date_today.day)
            data = json.loads(response.text[1:-1])
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['datetime'])

            # print df
            df = df[df['datetime'].between(self.date1, self.date2)]
            df.to_excel('json_data.xlsx')

            df = df[df['secuFullCode'].apply(lambda s:s.split('.')[0]).isin(self.stock_list)]

            for i in range(df.shape[0]):
                item = fund_monitor.items.FundMonitorItem()
                url = 'http://data.eastmoney.com/report/%s/%s.html' %(df['datetime'].iloc[i].strftime('%Y%m%d'), df['infoCode'].iloc[i])
                item['title'] = df['title'].iloc[i]
                item['data'] = {'insName': df['insName'].iloc[i],
                                'datetime': df['datetime'].iloc[i].strftime(u'%m月%d日'),
                                'stock_name': df['secuName'].iloc[i],
                                'stock_code': df['secuFullCode'].iloc[i].split('.')[0]}
                yield scrapy.Request(url, meta={'item': item}, callback=self.parse1)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        s = re.search(r'(?<=<div class="newsContent">).+?(?=</div>)', response.text, re.S).group()
        str_list = re.findall(r'(?<=<p>).+?(?=</p>)', s)
        item = response.meta['item']

        stock_type = self.stock_dict[item['data']['stock_code']]

        global file_path
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        file_name = (u"%s(%s).txt" %(stock_type, item['data']['stock_name'])).replace('*','')
        with open(file_path + file_name,'a') as f:
            title = u"%s%s:%s" %(item['data']['insName'], item['data']['datetime'], item['title'])
            f.write(title + '\n')
            for s in str_list:
                f.write(s.replace(r'</p><p>', '\n'))
            f.write('\n\n')

        with open(file_path + u'更新日志.log', 'a') as f:
            f.write("%s,%s(%s)\n" %(item['data']['datetime'], item['data']['insName'], stock_type))

        time.sleep(2)

        # with open(file_path + "_" + stock_type + '.txt','a') as f:
        #     title = u"%s%s:%s" %(item['data']['insName'], item['data']['datetime'], item['title'])
        #     f.write(title + '\n')
        #     for s in str_list:
        #         f.write(s.replace(r'</p><p>', '\n\n'))
        #     f.write('\n')



if __name__ == '__main__':
    Spider = Spider()
    # Spider.stock_flow()