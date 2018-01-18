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
import traceback
import datetime
import bs4
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
    name = "0014"

    def start_requests(self):
        self.urls = ['http://www.yuanlin.com/mmbj/%s.html' %(i+1) for i in range(1)]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            e_table = bs_obj.find('table', attrs={'class':'mmbj_list_tb'})

            print bs_obj.prettify(encoding='utf8')

            # title = [e_td.get_text(strip=True) for e_td in e_table.find('tr', class_='th').find_all('th')]
            # df0 = pd.DataFrame([])

            # with open('test.html', 'w') as f:
            #     f.write(response.text)

            ser = pd.Series([], name='url')
            print len(e_table.find_all('tr'))
            for e_tr in e_table.find_all('tr')[1:]:
                product_name = e_tr.find('td', class_='ProductName').a.get('href')
                # e_tds = e_tr.find_all('td')

                # ser = pd.Series([e_td.get_text(strip=True) for e_td in e_tds], index=title)
                # ser['url'] = product_name
                ser = ser.append(pd.Series([product_name,]), ignore_index=True)

            print ser
            df0 = pd.read_html(response.text, attrs={'class':'mmbj_list_tb'}, header=0)[0]
            df0['url'] = ser

            file_name = u"中国园林网.xlsx"

            if not os.path.exists(file_name):
                pd.DataFrame([]).to_excel(file_name)

            pd.read_excel(file_name).append(df0, ignore_index=None).to_excel(file_name)

            time.sleep(3)

        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))




if __name__ == '__main__':
    pass