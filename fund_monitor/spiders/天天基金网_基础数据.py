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


class Spider(scrapy.Spider):
    name = "0001"

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
            """
            <span class="checkall" id="checkall" onclick="checkAll()">一键查看全部</span>
            <span class="checkpage" id="checkall" onclick="checkPage()">返回分页方式看净值</span>
            """
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
            # 读取最新净值日期
            lastest_date = self.lastest_date_df['latest_date'][self.lastest_date_df['fund_code'] == item['fund_code']]
            if lastest_date.empty:
                raise Exception('本地数据库没有找到基金代号%s' %item['fund_code'])
            lastest_date = lastest_date.iat[0]
            lastest_date = datetime.datetime(lastest_date.year, lastest_date.month, lastest_date.day) # 从date格式转为datetime

            print "%s的最新净值日期为%s" %(item['fund_code'], lastest_date)

            # 净值估算
            e_dl = bs_obj.find('dl', class_='dataItem01')
            data = [e.get_text(strip=True) for e in e_dl.find('dd',class_='dataNums').find_all('span')]
            data_type = e_dl.find('span', class_='sp01').get_text(strip=True)
            data_date = e_dl.find('span', id='gz_gztime').get_text(strip=True)
            ser = pd.Series(data + [data_type, data_date], index=['净值', '涨跌值', '涨跌幅', '数据类型', '数据日期'])
            pd.DataFrame(ser).T.to_csv('C:\\Users\\Administrator\\Desktop\\fund_value.csv', encoding='utf8', mode='a')

            # 基金信息
            e_div = bs_obj.find('div', class_='infoOfFund')
            e_table = e_div.table
            df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8')[0]
            df = pd.DataFrame(np.array(df).reshape(-1,1))
            df['title'] = df[0].apply(lambda s:re.sub(r'\s','',str(s)).split('：')[0])
            df['value'] = df[0].apply(lambda s: re.sub(r'\s', '', str(s)).split('：')[1])
            ser = pd.Series(df['value'].tolist(), index=df['title'].tolist())
            ser['fund_code'] = item['fund_code']
            pd.DataFrame(ser).T.to_csv('C:\\Users\\Administrator\\Desktop\\fund_info.csv', encoding='utf8', mode='a')

            #ser = pd.Series(data+[data_type,data_date],index=['净值','涨跌值','涨跌幅','数据类型','数据日期'])
            #ser.to_csv('C:\\Users\\Administrator\\Desktop\\test.csv', encoding='utf8',mode='a')

            """
            # 基金净值
            e_div = bs_obj.find_all('div', class_='poptableWrap singleStyleHeight01')[0] #有三个标签页，分别是净值，分红，评级
            e_table = e_div.table
            df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8', header=0)[0]

            # 此处有时间BUG
            year_num = datetime.datetime.now().year
            df[u'日期'] = pd.to_datetime(df[u'日期'].apply(lambda s:'%s-%s' %(year_num,s)))

            print df[u'日期'].dtype
            print type(lastest_date)
            df = df[df[u'日期']>lastest_date] # 筛选日期

            if not df.empty:
                df = df.astype(np.str)
                df[u'key'] = df[u'日期'].apply(lambda date:"%s/%s" %(item['fund_code'], date))
                df[u'fund_code'] = item['fund_code']

                data_str = ','.join(["(%s)" % (','.join(["%s",] * df.shape[1])) for i in range(df.shape[0])])
                #data_str = ','.join(["(%s)" %(','.join(l)) for l in np.array(df).tolist()])
                sql = "INSERT INTO `eastmoney_daily_data`(value_date,net_asset_value,accumulative_net_value,daily_growth_rate,crawler_key,fund_code) VALUES%s;" %data_str
                #print sql #
                #print np.array(df).tolist()
                data_l = []
                for l in np.array(df).tolist():
                    data_l.extend(l)
                #print data_l
                mysql_connecter.connect(sql, args=data_l, host='localhost',user='spider',password = 'jlspider', dbname = 'spider', charset='utf8')
            else:
                print u"无最新数据"
            """

        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            #yield response.meta['item']

if __name__ == '__main__':
    pass