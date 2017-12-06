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

token = "7bc05d0d4c3c22ef9fca8c2a912d779c"

class Spider(scrapy.Spider):
    name = "0006"

    def start_requests(self):
        # self.urls = ["http://quote.eastmoney.com/center/BKList.html#notion_0_0?sortRule=0",
        #              "http://quote.eastmoney.com/center/BKList.html#trade_0_0?sortRule=0",
        #              "http://quote.eastmoney.com/center/BKList.html#area_0_0?sortRule=0"]
        self.urls = ["http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C._BKGN&sty=FPGBKI&st=c&sr=-1&p=1&ps=5000&cb=&token=" +token,
                     "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C._BKDY&sty=FPGBKI&st=c&sr=-1&p=1&ps=5000&cb=&token=" +token,
                     "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C._BKHY&sty=FPGBKI&st=c&sr=-1&p=1&ps=5000&cb=&token=" +token]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

# http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C.BK0822&sty=FCOIATA&sortType=C&sortRule=-1&page=1&pageSize=20&js=var%20quote_123%3d%7Brank:[(x)],pages:(pc)%7D&token=7bc05d0d4c3c22ef9fca8c2a912d779c&jsName=quote_123
# http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C.BK04801&sty=FCOIATA&sortType=C&sortRule=-1&page=1&pageSize=20&js=var%20quote_123%3d{rank:[(x)],pages:(pc)}&token=7bc05d0d4c3c22ef9fca8c2a912d779c&jsName=quote_123&_g=0.7343213301013858
    def parse(self, response):
        # item = response.meta['item']
        try:
            # print u"正在爬取%s板块的历史资金流数据" %item['plate_name']
            str_list = re.findall(r"(?<=\").+?(?=\")", response.text)
            print response.text
            for s in str_list:
                item = fund_monitor.items.FundMonitorItem()
                cmd = re.search(r'(?<=cmd=C\._).+?(?=&)', response.url)
                if cmd:
                    item['plate_type'] = cmd.group()
                else:
                    item['plate_type'] = 'unknown'

                l = s.split(',')
                # print s
                if not len(''.join(l)):
                    continue
                item['plate_code'] = l[1]
                item['plate_name'] = l[2]

                url = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?" \
                      "type=CT&cmd=C.%s1" %item['plate_code'] + \
                      "&sty=FCOIATA&sortType=C&sortRule=-1&page=1&pageSize=500" \
                      "&token=%s" %token + \
                      "&jsName=quote_123"
                yield scrapy.Request(url, meta={'item': item}, callback=self.parse1, dont_filter=False)

        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        item = response.meta['item']
        try:
            str_list = eval(response.text)
            df = pd.DataFrame([])
            for s in str_list:
                l = s.split(',')
                df0 = pd.Series([l[1],l[2], item['plate_name'], item['plate_type']], index=['stock_code', 'stock_name', 'plate_name', 'plate_type'])
                df = df.append(df0, ignore_index=True)

            if not df.empty:
                df['crawler_key'] = df['stock_code'] + '/' + df['plate_name']
                mysql_connecter.insert_df_data(df, 'stock_info', method='UPDATE')

        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))


if __name__ == '__main__':
    pass