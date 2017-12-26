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

token = "1942f5da9b46b069953c873404aad4b5"

class Spider(scrapy.Spider):
    name = "0009"

    def start_requests(self):
        with open('stock_list.txt','r') as f:
            stock_list = f.read().split('\n')
        self.urls = ["http://ff.eastmoney.com//EM_CapitalFlowInterface/api/js?type=hff&rtntype=2&check=TMLBMSPROCR&acces_token=%s&id=%s2"
                   # http://ff.eastmoney.com//EM_CapitalFlowInterface/api/js?type=hff&rtntype=2&js=({data:[(x)]})&cb=var%20aff_data=&check=TMLBMSPROCR&acces_token=1942f5da9b46b069953c873404aad4b5&id=0000012&_=1514274668035
                     %(token, stock_code) for stock_code in stock_list]
        for url in self.urls:
            print url
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            # print response.text
            l = eval(response.text)
            print l
            data = [s.split(',') for s in l]
            # print data
            print pd.DataFrame(data)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))


if __name__ == '__main__':
    pass