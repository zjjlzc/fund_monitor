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
    name = "0001"

    def start_requests(self):
        self.urls = ["http://fund.eastmoney.com/fund.html#os_0;isall_0;ft_;pt_1",]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        driver = driver_manager.initialization()
        try:
            driver.get('about:blank')
            driver.get(response.url)
            driver.find_element_by_class_name('checkall').click()
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
            # 净值估算
            e_dl = bs_obj.find('dl', class_='dataItem01')
            data = [e.get_text(strip=True) for e in e_dl.find('dd',class_='dataNums').find_all('span')]
            data_type = e_dl.find('span', class_='sp01').get_text(strip=True)
            data_date = e_dl.find('span', id='gz_gztime').get_text(strip=True)
            ser = pd.Series(data+[data_type,data_date],index=['净值','涨跌值','涨跌幅','数据类型','数据日期'])
            #ser.to_csv('C:\\Users\\Administrator\\Desktop\\test.csv', encoding='utf8',mode='a')
            # 基金净值
            e_div = bs_obj.find_all('div', class_='poptableWrap singleStyleHeight01')[0] #有三个标签页，分别是净值，分红，评级
            e_table = e_div.table
            df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8')[0]
            #df.to_csv('C:\\Users\\Administrator\\Desktop\\test.csv', encoding='utf8',mode='a')
            #yield item
        except:
            log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            #yield response.meta['item']

if __name__ == '__main__':
    pass