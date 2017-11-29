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

sql = "SELECT `fund_code`, max(`value_date`) as latest_date FROM `eastmoney_daily_data` GROUP BY `fund_code`;"

with open('blacklist.txt','r') as f:
    blacklist = f.read().split('\n')

class Spider(scrapy.Spider):
    name = "0004"

    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
        lastest_date_df = pd.read_sql(sql, conn)

    def start_requests(self):
        self.urls = ["http://fund.eastmoney.com/fund.html#os_0;isall_0;ft_;pt_1",]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        driver = driver_manager.initialization()
        try:
            driver.get('about:blank')
            driver.get(response.url)
            while driver.find_elements_by_class_name('checkall'):
                driver.find_element_by_class_name('checkall').click()
                print u"等待数据加载完成"
                time.sleep(2)

            bs_obj = bs4.BeautifulSoup(driver.page_source, 'html.parser')
            #log_obj.update_error(bs_obj.prettify(encoding='utf8'))
            e_trs = bs_obj.find('table', id='oTable').tbody.find_all('tr')
            for e_tr in e_trs:
                item = fund_monitor.items.FundMonitorItem()

                item['fund_code'] = e_tr.find('td', class_='bzdm').get_text(strip=True)
                item['fund_name'] = e_tr.find('td', class_='tol').a.get('title')
                item['url'] = 'http://fund.eastmoney.com/' + e_tr.find('td', class_='tol').a.get('href')

                yield scrapy.Request(item['url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))
        finally:
            driver.quit()

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            if item['fund_code'] in blacklist:
                raise Exception('此基金已列入黑名单')

            # 基金信息
            e_div = bs_obj.find('div', class_='infoOfFund')
            e_table = e_div.table
            s = re.sub(r'\s+|\xa0','',u','.join([e.get_text(strip=True) for e in e_table.find_all('td')]))
            ser = pd.Series([])
            d = {
                u'fund_type':u'基金类型：',
                u'fund_size':u'基金规模：',
                u'found_date':u'成立日：',
                u'manager':u'管理人：'
            }
            ser['fund_code'] = item['fund_code']
            for key in d:
                ser[key] = re.search(ur'(?<=%s).+?(?=\,)' %d[key], s).group() if re.search(ur'(?<=%s).+(?=,)' %d[key], s) else None

            df = pd.DataFrame(ser).T
            df.index = [item['fund_code'], ]

            mysql_connecter.insert_df_data(df, 'fund_info', method='UPDATE')



        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            #yield response.meta['item']

if __name__ == '__main__':
    pass