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


class Spider(scrapy.Spider):
    name = "0005"

    def start_requests(self):

        sql = """
        SELECT `plate_name`, `url` FROM `capital_flow_info`
        """

        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            url_df = pd.read_sql(sql, conn)

        for t in url_df.iterrows():
            ser = t[1]
            item = fund_monitor.items.FundMonitorItem()
            item['plate_name'] = ser['plate_name']
            yield scrapy.Request(url='http://data.eastmoney.com' + ser['url'], meta={'item': item}, callback=self.parse)

    def parse(self, response):
        item = response.meta['item']
        try:
            print u"正在爬取%s板块的历史资金流数据" %item['plate_name']
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            e_table = bs_obj.find('table', id='tb_lishi')

            plate_name = item['plate_name']

            df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8')[0]
            df.columns = [
                u'日期',
                u'主力净流入净额',
                u'主力净流入净占比',
                u'超大单净流入净额',
                u'超大单净流入净占比',
                u'大单净流入净额',
                u'大单净流入净占比',
                u'中单净流入净额',
                u'中单净流入净占比',
                u'小单净流入净额',
                u'小单净流入净占比'
            ]
            df = df.rename({
                u'日期':'value_date',
                u'主力净流入净额':'main_flow_amount',
                u'主力净流入净占比':'main_flow_ratio',
                u'超大单净流入净额':'super_flow_amount',
                u'超大单净流入净占比':'super_flow_ratio',
                u'大单净流入净额':'big_flow_amount',
                u'大单净流入净占比':'big_flow_ratio',
                u'中单净流入净额':'median_flow_amount',
                u'中单净流入净占比':'median_flow_ratio',
                u'小单净流入净额':'small_flow_amount',
                u'小单净流入净占比':'small_flow_ratio'
            }, axis=1)
            df['plate_name'] = plate_name
            df['crawler_key'] = df['plate_name'] + '/' + df['value_date']

            if not df.empty:
                mysql_connecter.insert_df_data(df, 'capital_flow_data', method='UPDATE')

        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))


if __name__ == '__main__':
    pass