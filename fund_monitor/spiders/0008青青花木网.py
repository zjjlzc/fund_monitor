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
from selenium.webdriver.common.keys import Keys

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
    name = "0008"

    def start_requests(self):
        self.urls = ['http://www.312green.com/price/view-c1005-s%D5%E3%BD%AD-t-v-p1.html', ]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            e_form = bs_obj.find('form', attrs={'name':'frmPage'})
            page_num = re.search(ur'(?<=共).*?(?=页)', e_form.get_text(strip=True)).group().strip()
            df_sum = pd.DataFrame([])
            for i in range(int(page_num)):
                url = 'http://www.312green.com/price/view-c1005-s%s-t-v-p%s.html' %('%D5%E3%BD%AD',i+1)
                html = requests_manager.get_html(url)
                bs_obj = bs4.BeautifulSoup(html, 'html.parser')
                e_table = bs_obj.find('table', class_='table_gridline')
                df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8', header=0)[0]
                # df = df.set_index(0, axis=1)
                print df
                df_sum = df_sum.append(df,ignore_index=True)
                df_sum.to_excel('tree_data.xlsx')
                time.sleep(1)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))




if __name__ == '__main__':
    pass