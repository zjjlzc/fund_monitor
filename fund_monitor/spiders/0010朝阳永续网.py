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
    name = "0010"

    def start_requests(self):
        url = 'http://www.simuwang.com/'
        driver = driver_manager.initialization(engine='Chrome')
        try:
            driver.get(url)
            while not driver.find_elements_by_id('gr-login-box'):
                driver.find_element_by_class_name('topRight').find_element_by_tag_name('a').click()
                time.sleep(1)

            login_box = driver.find_element_by_id('gr-login-box')
            login_box.find_elements_by_tag_name('input')[0].send_keys('13575486859')
            login_box.find_elements_by_tag_name('input')[0].send_keys(Keys.TAB)
            login_box.find_elements_by_tag_name('input')[2].send_keys('137982')

            login_buttom = login_box.find_element_by_class_name('gr-big-btn')
            login_buttom.click()
            driver.get_screenshot_as_file('screenshot1.png')
            time.sleep(3)

        except:
            log_obj.error("%s中无法解析\n原因：%s" % (self.name, traceback.format_exc()))
            # driver.quit()

        urls = ['http://dc.simuwang.com/product/HF00001NNB.html',]
        for url in urls:
            driver.get(url)
            driver.get_screenshot_as_file('screenshot2.png')


    # def parse(self, response):
    #     try:
    #         driver = driver_manager.initialization()
    #         self.cookies = self.get_cookies(response.url)
    #         driver.get(response.url)
    #         driver.delete_all_cookies()
    #         # for cookie in self.cookies:
    #         #     cookie0 = {'name':cookie['name'], 'value':cookie['value']}
    #         #     # print cookie0
    #         driver.add_cookie(self.cookies)
    #
    #         driver.get(response.url)
    #         # driver.save_screenshot('screenshot2.png')
    #
    #         with open('test.html', 'w') as f:
    #             f.write(response.text)
    #
    #     except:
    #         log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))


if __name__ == '__main__':
    pass