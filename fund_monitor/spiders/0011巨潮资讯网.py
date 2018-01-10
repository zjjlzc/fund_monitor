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
import traceback
import datetime
import bs4
import time
import json
import numpy as np

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
import requests

import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
log_obj = set_log.Logger(log_path, set_log.logging.WARNING, set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=True) # 是否需要在每次运行程序前清空Log文件

with open('blacklist.txt','r') as f:
    blacklist = f.read().split('\n')

class Spider(scrapy.Spider):
    name = "0011"

    def start_requests(self):
        self.urls = ["http://www.cninfo.com.cn/cninfo-new/disclosure/szse_main_latest",
                     "http://www.cninfo.com.cn/cninfo-new/disclosure/szse_sme_latest",
                     "http://www.cninfo.com.cn/cninfo-new/disclosure/szse_gem_latest",
                     "http://www.cninfo.com.cn/cninfo-new/disclosure/sse_latest"]
        for url0 in self.urls:
            i = 0
            while True:
                try:
                    i = i + 1
                    col_str = re.search(r'(?<=http://www.cninfo.com.cn/cninfo-new/disclosure/).+(?=_latest)', url0).group()
                    data = {
                        "column": col_str,
                        "pageNum": i,
                        "tabName": "latest",
                        "pageSize": "30"
                    }
                    print 'url0', url0
                    print 'data', data
                    resp = requests.post(url0, data=data)
                    data = json.loads(resp.content)
                    df = pd.DataFrame()
                    for d in data['classifiedAnnouncements']:
                        df = df.append(pd.DataFrame(d), ignore_index=True)
                    if df.empty:
                        break

                    df['announcementTime'] = df['announcementTime'].astype(np.str).apply(lambda s:datetime.datetime.fromtimestamp(int(s[:10])).date())
                    df = df[df['announcementTime'] == datetime.datetime.now().date()]

                    # 筛选股票
                    with open(u'股票池结构.json', 'r') as f:
                        stock_dict = json.load(f)
                        # print 'stock_dict',stock_dict
                    stock_list = reduce(lambda l1,l2: l1 + l2, stock_dict.values())

                    output = df[df['secCode'].isin(stock_list)]

                    if not output.empty:
                        output.loc[:,'adjunctUrl'] = output.loc[:,'adjunctUrl'].apply(lambda s:'http://www.cninfo.com.cn/' + s)
                        # print output['adjunctUrl']
                        url_list = output['adjunctUrl'].tolist()
                        for j in range(len(url_list)):
                            url = url_list[j]
                            print url
                            file_path = u"%s/公告/%s/" %(os.getcwd(), output['secName'].tolist()[j])
                            if not os.path.exists(file_path):
                                os.makedirs(file_path)

                            requests_manager.get_file(url, file_path + output['announcementTitle'].tolist()[j] + '.pdf')
                    if df.empty:
                        break

                    time.sleep(2)
                except:
                    print traceback.format_exc()

if __name__ == '__main__':
    pass