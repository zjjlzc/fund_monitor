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
import json
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

cookie0 = {#"cur_ck_time": "1514945128;",
           "ck_request_key": "O5K2PsIhrWt%2F6oeM707he2IVJiVJHlIQsuVY4FJYNy0%3D;",
                            # UOUa%2F%2Bg6glCWly1TK%2F682x6EO2CAy1EDw0xVEfV%2BSQg%3D;
           "http_tK_cache": "ace8a7139a833b6878b32b4c2479b7ad828fb7c1;",
                           # b3fa88b238453e3dd8f1456d7b8eeefd7d13a1a5;
           "passport": "55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c;",
                      # 55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c;
           "rz_u_p": "d41d8cd98f00b204e9800998ecf8427e%3Duser_13575486859;",
                    # d41d8cd98f00b204e9800998ecf8427e%3Duser_13575486859;
           "rz_rem_u_p": "aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D;"
                        # aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D
           }

detail_cookie = {
    'smppw_tz_auth': '1',
    'http_tK_cache': '971eed91df0b329f5b6a9dc9da4a7729abe39ae6',
    'passport': '55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c',
    'cur_ck_time': '1515393953',
    'rz_rem_u_p': 'aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D',
    'stat_sessid': 'd11ovlf8ruebgrh68fsqfaqu11',
    'autologin_status': '0',
    'regsms': '1515393938000',
    'rz_u_p': 'd41d8cd98f00b204e9800998ecf8427e%3Duser_13575486859',
    'guest_id': '1502416353',
    'PHPSESSID': 'nq9bqbm583o1tr6hcra3il5rh5',
    'had_quiz_55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c': '1515393954000',
    'ck_request_key': 'ZHZmg4X35RHTWqEvofYkRr53HU25U93AWSua2fYPfzM%3D',
    'rz_token_6658': 'c6792ed4142577a28ab5639ef93720af.1515393938',
    'Hm_lvt_c3f6328a1a952e922e996c667234cdae': '1515393938',
    'fyr_ssid_n5776': 'fyr_n5776_jc5uf9zb',
    'Hm_lpvt_c3f6328a1a952e922e996c667234cdae': '1515393954'}

cookies = []
for key in cookie0:
    d = {}
    d['domain'] = ".simuwang.com"
    d['path'] = "/"
    d['name'] = key.strip()
    d['value'] = cookie0[key]
    cookies.append(d)


class Spider(scrapy.Spider):
    name = "0007"

    def start_requests(self):
        d1 = self.get_cookies('http://dc.simuwang.com/product/HF00001MTU')
        print {d['name']: d['value'] for d in d1}
        d1 = self.get_cookies('http://dc.simuwang.com/fund/getNavList.html?id=HF00001MTU&muid=55635&page=2')
        print {d['name']: d['value'] for d in d1}


        url = 'http://dc.simuwang.com/ranking/get?page=1&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A1%3Brating_year%3A1%3Bstrategy%3A1%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A'
        # 获取总页数
        driver = driver_manager.initialization()
        driver.get(url)
        # print driver.get_cookies()
        # print {d['name']:d['value'] for d in driver.get_cookies()}
        global cookies
        for cookie0 in cookies:
            driver.add_cookie(cookie0)
        driver.get(url)
        # print driver.page_source

        data = json.loads(re.search(r'{.+}', driver.page_source).group())
        print u"第一页数据长度%s" %len(data["data"])
        page_num = 2 # int(data["pager"]["pagecount"])

        urls = ['http://dc.simuwang.com/ranking/get?page=%s' %(i+1) +
                '&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A1%3Brating_year%3A1%3Bstrategy%3A1%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A'  for i in range(page_num)]
        cookies = {d['name']: d['value'] for d in driver.get_cookies()}
        driver.quit()

        # 每页爬取
        for url in urls:
            item = fund_monitor.items.FundMonitorItem()
            item['cookies'] = cookies
            yield scrapy.Request(url=url, meta={'item': item}, cookies=item['cookies'], callback=self.parse0)

    def parse0(self, response):
        item = response.meta['item']
        data = json.loads(response.text)
        data = data["data"]

        df = pd.DataFrame(data)
        df = df.reindex(['city', 'company_id', 'company_short_name', 'fund_id', 'fund_name', 'fund_short_name', 'inception_date', 'province',
                         'register_number', 'web_site'], axis=1)
        df['web_site'] = u'私募排排网'
        df = df.set_index('fund_id')

        for fund_id in df.index:
            item0 = item.copy()
            url = 'http://dc.simuwang.com/product/%s' %fund_id
            item0['data'] = df.loc[fund_id,:]

            # print item['cookies']
            global detail_cookie
            item0['cookies'] = detail_cookie #.update(detail_cookie)
            # print item0['cookies']
            yield scrapy.Request(url=url, meta={'item': item0}, cookies=item['cookies'], callback=self.parse1)

    def parse1(self, response):
        item = response.meta['item']

        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')

        # 产品要素
        print response.url
        print item['data']['fund_name']
        with open('test.html', 'w') as f:
            f.write(response.text)
        print pd.read_html(response.text, attrs={'id':'product-detail-table'})


    def get_cookies(self, url):

        while True:
            driver = driver_manager.initialization(engine='Chrome')
            # print driver.orientation
            try:
                driver.get(url) #'http://www.simuwang.com/')


                while not driver.find_elements_by_id('gr-login-box'):
                    driver.find_element_by_class_name('topRight').find_element_by_tag_name('a').click()
                    time.sleep(2)
                time.sleep(10)
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