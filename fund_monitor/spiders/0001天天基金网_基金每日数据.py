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

with open('blacklist.txt','r') as f:
    blacklist = f.read().split('\n')


class Spider(scrapy.Spider):
    name = "0001"
    sql = """
    SELECT `fund_code`, `value_date` AS newest_date, `estimate_net_value`, `estimate_daily_growth_rate`, `net_asset_value`, `accumulative_net_value`, `daily_growth_rate` FROM `eastmoney_daily_data` L
        INNER JOIN
            (SELECT `fund_code` a, max(`value_date`) b FROM `eastmoney_daily_data` GROUP BY `fund_code`) R
        ON L.`fund_code` = R.a AND L.`value_date` = R.b
    """

    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
        newest_date_df = pd.read_sql(sql, conn)
    #print newest_date_df

    def __init__(self, method='all'):
        self.method = method
        print 'self.method=', self.method

    def start_requests(self):
        self.urls = ["http://fund.eastmoney.com/fund.html#os_0;isall_0;ft_;pt_1",]

        for url in self.urls:
            if self.method == 'all':
                yield scrapy.Request(url=url, callback=self.parse)
            elif self.method == 'daily':
                yield scrapy.Request(url=url, callback=self.parse00)

    def parse00(self, response):
        try:
            with open('important_fund.txt','r') as f:
                l = f.read().split('\n')

            for fund_code in l:
                if not fund_code:
                    continue

                item = fund_monitor.items.FundMonitorItem()
                item['fund_code'] = fund_code
                item['url'] = 'http://fund.eastmoney.com/%s.html' %fund_code

                yield scrapy.Request(item['url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))


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

        # 读取最新净值日期
        lastest_date = self.newest_date_df['newest_date'][self.newest_date_df['fund_code'] == item['fund_code']]
        if lastest_date.empty:
            raise Exception('本地数据库没有找到基金代号%s' % item['fund_code'])
        lastest_date = lastest_date.iat[0]
        lastest_date = datetime.datetime(lastest_date.year, lastest_date.month, lastest_date.day)  # 从date格式转为datetime

        print "本地%s的最新净值日期为%s" % (item['fund_code'], lastest_date)

        try:
            # 净值估算
            e_dl = bs_obj.find('dl', class_='dataItem01')
            data = [e.get_text(strip=True) for e in e_dl.find('dd',class_='dataNums').find_all('span')]
            data_type = e_dl.find('span', class_='sp01').get_text(strip=True)
            data_date = e_dl.find('span', id='gz_gztime').get_text(strip=True)

            if data_date != '--':
                data_date = datetime.datetime.strptime(re.sub(r'\(|\)','',data_date),'%y-%m-%d %H:%M')
                # 周六，周日按周五算
                data_date = data_date - datetime.timedelta(days=1) if data_date.isoweekday() == 6 else data_date
                data_date = data_date - datetime.timedelta(days=2) if data_date.isoweekday() == 7 else data_date

                df = pd.DataFrame(data + [data_type, data_date], index=[u'净值', u'涨跌值', u'涨跌幅', u'数据类型', u'数据日期']).T
                df = df.drop([u'涨跌值', u'数据类型'], axis=1)
                df = df.rename({u'净值':u'estimate_net_value',u'涨跌幅':u'estimate_daily_growth_rate', u'数据日期':u'value_date'}, axis=1)
                df[u'fund_code'] = item['fund_code']
                df[u'value_date'] = df[u'value_date'].apply(lambda date0: date0.strftime('%Y-%m-%d'))
                df[u'crawler_key'] = df[u'fund_code'] + '/' + df[u'value_date']
                df.index = df[u'crawler_key']
                print u"网页日期:",df[u'value_date'].iat[0],u'本地日期：', lastest_date
                # if datetime.datetime.strptime(df[u'value_date'].iat[0],'%Y-%m-%d').date() <= lastest_date.date():
                #     mysql_connecter.update_df_data(df, u'eastmoney_daily_data', u'crawler_key')
                # else:
                #     mysql_connecter.insert_df_data(df, u'eastmoney_daily_data', method='UPDATE')
                if not df.empty:
                    mysql_connecter.insert_df_data(df, 'eastmoney_daily_data', method='UPDATE')
                else:
                    print u"无最新数据"
        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            with open(u'净值估算.html', 'w') as f:
                f.write(response.text)

        try:
            # 基金净值
            e_div = bs_obj.find_all('div', class_='poptableWrap singleStyleHeight01')[0] #有三个标签页，分别是净值，分红，评级
            e_table = e_div.table
            df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8', header=0)[0]

            # 此处有时间BUG
            year_num = datetime.datetime.now().year
            df[u'日期'] = pd.to_datetime(df[u'日期'].apply(lambda s:'%s-%s' %(year_num,s)))

            #print df[u'日期'].dtype
            #print type(lastest_date)

            df = df.astype(np.str)
            df[u'crawler_key'] = df[u'日期'].apply(lambda date: "%s/%s" % (item['fund_code'], date))
            df[u'fund_code'] = item['fund_code']
            df = df.rename({u'日期': u'value_date', u'单位净值': u'net_asset_value', u'累计净值': u'accumulative_net_value', u'日增长率': u'daily_growth_rate'},axis=1)
            df.index = df[u'crawler_key']

            if not df.empty:
                mysql_connecter.insert_df_data(df, 'eastmoney_daily_data', method='UPDATE')
            else:
                print u"无最新数据"
        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            with open(u'基金净值.html', 'w') as f:
                f.write(response.text)

        try:
            # js.v中的数据
            url = 'http://fund.eastmoney.com/pingzhongdata/%s.js?v=%s' %(item['fund_code'],datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            js_data = requests_manager.get_html(url)
            js_data = re.sub('\s+','',js_data)
            re_func = lambda key:re.search((r'(?<=%s\=).+?(?=;)' %key), js_data, re.S).group() if re.search((r'%s\=.+?;' %key), js_data) else None

            # 股票仓位
            Data_fundSharesPositions = pd.DataFrame(eval(re_func('Data_fundSharesPositions')), columns = [u'value_date', u'fund_shares_positions']).astype(np.str)

            Data_fundSharesPositions[u'value_date'] = Data_fundSharesPositions[u'value_date'].apply(lambda s:datetime.datetime.fromtimestamp(int(s[:10])).strftime('%Y-%m-%d'))
            Data_fundSharesPositions[u'fund_shares_positions'] = Data_fundSharesPositions[u'fund_shares_positions'] + '%'

            Data_fundSharesPositions[u'crawler_key'] = item['fund_code'] + '/' + Data_fundSharesPositions[u'value_date']
            Data_fundSharesPositions = Data_fundSharesPositions.drop([u'value_date', ], axis=1)
            Data_fundSharesPositions.index = Data_fundSharesPositions[u'crawler_key']

            if not Data_fundSharesPositions.empty:
                mysql_connecter.insert_df_data(Data_fundSharesPositions, 'eastmoney_daily_data', method='UPDATE')

        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            with open(u'js_v中的数据.html', 'w') as f:
                f.write(response.text)
            #yield response.meta['item']

if __name__ == '__main__':
    pass