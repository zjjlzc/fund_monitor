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
    name = "0003"

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
            df = pd.read_html(e_table.prettify(encoding='gbk'), encoding='gbk')[0]
            df = pd.DataFrame(np.array(df).reshape(-1,1))
            #print df
            df = df.fillna('：')
            df['title'] = df[0].apply(lambda s:re.sub(r'\s','',str(s)).split(u'：')[0])
            df['value'] = df[0].apply(lambda s:re.sub(r'\s','',str(s)).split(u'：')[1])
            #value_list = [str(s) for s in df['value'].tolist() if s]
            #title_list = [str(s) for s in df['title'].tolist() if s]
            #ser = pd.Series(value_list, index=title_list)
            ser = pd.Series(df['value'].tolist(), index=df['title'].tolist())
            #print ser
            ser['fund_code'] = str(item['fund_code'])

            #if os.path.exists('C:\\Users\\Administrator\\Desktop\\fund_info.xls'):
            #    file_df = pd.read_excel('C:\\Users\\Administrator\\Desktop\\fund_info.xls', encoding='utf8')#, index_col=0)#, dtype=np.str)
            #else:
            #    file_df = pd.DataFrame([])
#
            #file_df = file_df.append(ser, ignore_index=True)
            #file_df.to_excel('C:\\Users\\Administrator\\Desktop\\fund_info.xls', encoding='utf8')
            pd.DataFrame(ser).T.to_csv('C:\\Users\\Administrator\\Desktop\\fund_info%s.csv' %ser.shape, mode='a')

            #ser = pd.Series(data+[data_type,data_date],index=['净值','涨跌值','涨跌幅','数据类型','数据日期'])

        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            #yield response.meta['item']

if __name__ == '__main__':
    pass