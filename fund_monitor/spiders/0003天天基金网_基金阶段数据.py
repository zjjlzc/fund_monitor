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

with open('blacklist.txt','r') as f:
    blacklist = f.read().split('\n')


class Spider(scrapy.Spider):
    name = "0003"
    sql = """
    SELECT `fund_code`, `value_date` AS newest_date, `estimate_net_value`, `estimate_daily_growth_rate`, `net_asset_value`, `accumulative_net_value`, `daily_growth_rate` FROM `eastmoney_daily_data` L
        INNER JOIN
            (SELECT `fund_code` a, max(`value_date`) b FROM `eastmoney_daily_data` GROUP BY `fund_code`) R
        ON L.`fund_code` = R.a AND L.`value_date` = R.b
    """
    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
        newest_date_df = pd.read_sql(sql, conn)
    #print newest_date_df

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

        if item['fund_code'] in blacklist:
            raise Exception('此基金已列入黑名单')

        try:
            # js.v中的数据
            url = 'http://fund.eastmoney.com/pingzhongdata/%s.js?v=%s' %(item['fund_code'],datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            js_data = requests_manager.get_html(url)
            js_data = re.sub('\s+','',js_data)
            re_func = lambda key:re.search((r'(?<=%s\=).+?(?=;)' %key), js_data, re.S).group() if re.search((r'%s\=.+?;' %key), js_data) else None

            # 规模变动
            Data_fluctuationScale = pd.read_json(re_func('Data_fluctuationScale'))# , columns = [u'value_date', u'fund_shares_positions']).astype(np.str)

            for i in range(Data_fluctuationScale.shape[0]):
                #print Data_fluctuationScale.loc[i,'series']
                #print type(Data_fluctuationScale.loc[i,'series'])
                ser = pd.Series(Data_fluctuationScale.loc[i,'series'])
                ser = ser.rename({'mom':u'较上期环比', 'y':u'净资产规模(亿)'})
                ser['value_date'] = Data_fluctuationScale.loc[i, 'categories']
                Data_fluctuationScale.loc[i, 'series'] = ser.to_json()

            Data_fluctuationScale['fund_code'] = item['fund_code']
            Data_fluctuationScale['data_type'] = u'规模变动'
            Data_fluctuationScale['crawler_key'] = Data_fluctuationScale['fund_code'] + '/' + Data_fluctuationScale['data_type'] + '/' + Data_fluctuationScale['categories']
            Data_fluctuationScale = Data_fluctuationScale.drop(['categories', ], axis=1)
            Data_fluctuationScale = Data_fluctuationScale.rename({'series':'json_data'}, axis=1)

            Data_fluctuationScale.index = Data_fluctuationScale['crawler_key']
            #print Data_fluctuationScale
            if not Data_fluctuationScale.empty:
                mysql_connecter.insert_df_data(Data_fluctuationScale, 'fund_mixed_data')

            # 持有人结构
            Data_holderStructure = json.loads(re_func('Data_holderStructure'))# , columns = [u'value_date', u'fund_shares_positions']).astype(np.str)
            #print Data_holderStructure
            categories = Data_holderStructure['categories']
            series = Data_holderStructure['series']

            d = {d0['name']:d0['data'] for d0 in series}

            df = pd.DataFrame(d, index=categories)
            df['value_date'] = df.index
            ser = df.T.apply(lambda ser:ser.to_json())
            ser.name = 'json_data'

            Data_holderStructure = pd.DataFrame(ser, index=categories)
            Data_holderStructure['fund_code'] = item['fund_code']
            Data_holderStructure['data_type'] = u'持有人结构'
            Data_holderStructure['crawler_key'] = item['fund_code'] + '/' + Data_holderStructure['data_type'] + '/' + Data_holderStructure.index

            Data_holderStructure.index = Data_holderStructure['crawler_key']
            if not Data_holderStructure.empty:
                mysql_connecter.insert_df_data(Data_holderStructure, 'fund_mixed_data')

            # 资产配置
            Data_assetAllocation = json.loads(re_func('Data_assetAllocation'))
            categories = Data_assetAllocation['categories']
            series = Data_assetAllocation['series']

            d = {d0['name']:d0['data'] for d0 in series}

            df = pd.DataFrame(d, index=categories)
            df['value_date'] = df.index
            ser = df.T.apply(lambda ser:ser.to_json())
            ser.name = 'json_data'

            Data_assetAllocation = pd.DataFrame(ser, index=categories)
            Data_assetAllocation['fund_code'] = item['fund_code']
            Data_assetAllocation['data_type'] = u'资产配置'
            Data_assetAllocation['crawler_key'] = item['fund_code'] + '/' + Data_assetAllocation['data_type'] + '/' + Data_assetAllocation.index

            Data_assetAllocation.index = Data_assetAllocation['crawler_key']
            if not Data_assetAllocation.empty:
                mysql_connecter.insert_df_data(Data_assetAllocation, 'fund_mixed_data')

            # 基金经理变动一览
            e_table = bs_obj.find('li', class_='fundManagerTab').table
            df0 = pd.read_html(e_table.prettify(encoding='utf8'),encoding='utf8')[0]
            df0.columns = df0.loc[0,:]
            df0.columns.name = None
            df0 = df0.drop([0,])
            df0.index = range(df0.shape[0])
            df = pd.DataFrame({
                'crawler_key':item['fund_code'] + '/' + u'基金经理变动',
                'fund_code':item['fund_code'],
                'data_type':u'基金经理变动',
                'json_data':df0.to_json()
            }, index=[0,])
            if not df.empty:
                mysql_connecter.insert_df_data(df, 'fund_mixed_data', method='UPDATE')

        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))

if __name__ == '__main__':
    pass