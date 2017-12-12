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
    name = "0007"

    def start_requests(self):
        self.urls = ['http://www.simuwang.com/', ]
        for url in self.urls:
            self.cookies = self.get_cookies(url)
            cookies0 = ["%s=%s" %(d[u'name'],d[u'value']) for d in self.cookies]
            print self.cookies

            yield scrapy.Request(url=url, headers = {'cookies':';'.join(cookies0)}, callback=self.parse)

    def parse(self, response):
        try:
            driver = driver_manager.initialization()

            driver.get(response.url)
            driver.delete_all_cookies()
            for cookie in self.cookies:
                cookie0 = {'name':cookie['name'], 'value':cookie['value']}
                print cookie0
                driver.add_cookie(cookie0)

            driver.get(response.url)
            driver.save_screenshot('screenshot.png')


            with open('test.html', 'w') as f:
                f.write(response.text)
            # # print u"正在爬取%s板块的历史资金流数据" %item['plate_name']
            # str_list = re.findall(r"(?<=\").+?(?=\")", response.text)
            # print response.text
            # for s in str_list:
            #     item = fund_monitor.items.FundMonitorItem()
            #     cmd = re.search(r'(?<=cmd=C\._).+?(?=&)', response.url)
            #     if cmd:
            #         item['plate_type'] = cmd.group()
            #     else:
            #         item['plate_type'] = 'unknown'
            #
            #     l = s.split(',')
            #     # print s
            #     if not len(''.join(l)):
            #         continue
            #     item['plate_code'] = l[1]
            #     item['plate_name'] = l[2]
            #
            #     url = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?" \
            #           "type=CT&cmd=C.%s1" %item['plate_code'] + \
            #           "&sty=FCOIATA&sortType=C&sortRule=-1&page=1&pageSize=500" \
            #           "&token=%s" %token + \
            #           "&jsName=quote_123"
            #     yield scrapy.Request(url, meta={'item': item}, callback=self.parse1, dont_filter=False)

        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def get_cookies(self, url):
        while True:
            driver = driver_manager.initialization(engine='Chrome')
            # print driver.orientation
            try:
                driver.get(url) #'http://www.simuwang.com/')


                while not driver.find_elements_by_id('gr-login-box'):
                    driver.find_element_by_class_name('topRight').find_element_by_tag_name('a').click()
                    time.sleep(1)

                    # driver.save_screenshot('screenshot.png')
                cookies = driver.get_cookies()
                # print {d[u'name']:d[u'value'] for d in cookies}

                # driver.save_screenshot('screenshot.png')
                login_box = driver.find_element_by_id('gr-login-box')
                login_box.find_elements_by_tag_name('input')[0].send_keys('13575486859')
                login_box.find_elements_by_tag_name('input')[0].send_keys(Keys.TAB)
                login_box.find_elements_by_tag_name('input')[2].send_keys('137982')
                # passwd_input.click()
                # passwd_input.send_keys('137482')

                login_buttom = login_box.find_element_by_class_name('gr-big-btn')
                login_buttom.click()
                time.sleep(3)

                # driver.save_screenshot('screenshot.png')

                cookies = driver.get_cookies()
                # print {d[u'name']:d[u'value'] for d in cookies}
                return cookies# {d[u'name']:d[u'value'] for d in cookies}

            except:
                log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))
            finally:
                driver.quit()


if __name__ == '__main__':
    pass