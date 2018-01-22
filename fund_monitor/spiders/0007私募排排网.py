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

cookie0 = {#"cur_ck_time": "1514945128;",
           "ck_request_key": "N%2BK73PwNc8ntaYZw6YSK9X%2FJl7r5qvVo%2B4fH5uhej%2Fc%3D",
                            # O5K2PsIhrWt%2F6oeM707he2IVJiVJHlIQsuVY4FJYNy0%3D
           "http_tK_cache": "1ced45dff24b8587ff1608436eb66685c82b5e59",
                           # b3fa88b238453e3dd8f1456d7b8eeefd7d13a1a5;
           "passport": "55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c",
                      # 55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c;
           "rz_u_p": "d41d8cd98f00b204e9800998ecf8427e%3Duser_13575486859",
                    # d41d8cd98f00b204e9800998ecf8427e%3Duser_13575486859;
           "rz_rem_u_p": "aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D"
                        # aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D
           }

detail_cookie = {
    # "smppw_tz_auth": "1",
    "http_tK_cache": "0c64c99df7fc61b8fe4433b469ff2c9aa46be8a5",
    "passport": "55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c",
               # 55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c
    # "cur_ck_time": "1515648448",
    "rz_rem_u_p": "aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D",
                 # aiFB3odpeWZHIeDOt%2FJ%2BaNkn%2F8V390t5nnkC3N3%2FbHM%3D%24KzJx4oUsxjHHhuNbZ9EmS5OMgDiO8JGftPoh1SVUL24%3D
    # "stat_sessid": "i02okc46h6qacso048l85o3mh1",
    # "autologin_status": "0",
    # "rz_token_6658": "c6aa9f4cfda6e7deb2727599cd97b380.1515648423",
    "rz_u_p": "d41d8cd98f00b204e9800998ecf8427e%3Duser_13575486859",
    # "guest_id": "1515845263",
    # "PHPSESSID": "f40f1u4g89nie0eaunebgecn57",
    # "had_quiz_55635%09user_13575486859%09VFJRD1BWBQ9eUFgHBFdUUFdVCwFQBwZUCVVSVlFXUgo%3D9b207ecd5c": "1515648444000",
    "ck_request_key": "f5VUox4VuMb7H1lIvONSkzvsGm83GMfIZw3HirVFfb4%3D",
    # "regsms": "1515648423000",
    # "Hm_lvt_c3f6328a1a952e922e996c667234cdae": "1515648423",
    # "fyr_ssid_n5776": "fyr_n5776_jca1xs0m",
    # "Hm_lpvt_c3f6328a1a952e922e996c667234cdae": "1515648444",
}

catlog_cookies = []
for key in cookie0:
    d = {}
    d['domain'] = ".simuwang.com"
    d['path'] = "/"
    d['name'] = key.strip()
    d['value'] = cookie0[key]
    catlog_cookies.append(d)


class Spider(scrapy.Spider):
    name = "0007"

    def start_requests(self):
        # d1 = self.get_cookies('http://dc.simuwang.com/product/HF00001MTU')
        # d1 = {d['name']: d['value'] for d in d1}
        # for key in d1:
        #     print '"%s":"%s",' %(key,d1[key])

        url_dict = {
            '股票策略':[
                "http://dc.simuwang.com/ranking/get?page=",
                "&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bstrategy%3A1%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A"
            ],
            '宏观策略':[
                "http://dc.simuwang.com/ranking/get?page=",
               "&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bstrategy%3A2%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A"
            ],
            '管理期货':[
                "http://dc.simuwang.com/ranking/get?page=",
                "&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bstrategy%3A3%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A"
            ],
            '事件驱动':[
                "http://dc.simuwang.com/ranking/get?page=",
                "&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bstrategy%3A4%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A"
            ],
            '相对价值':[
                "http://dc.simuwang.com/ranking/get?page=",
                "&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bstrategy%3A5%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A"
            ],
            '固定收益':[
                "http://dc.simuwang.com/ranking/get?page=",
                "&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bstrategy%3A6%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A"
            ]
        }

        for key in url_dict:
            url = "%s1%s" %(url_dict[key][0], url_dict[key][1])
            print url
            # 获取总页数
            driver = driver_manager.initialization()
            driver.get(url)
            # print driver.get_cookies()
            # print {d['name']:d['value'] for d in driver.get_cookies()}
            global catlog_cookies
            for cookie0 in catlog_cookies:
                driver.add_cookie(cookie0)
            driver.get(url)
            # print driver.page_source

            data = json.loads(re.search(r'{.+}', driver.page_source).group())
            # print u"第一页数据长度%s" %len(data["data"])
            page_num = 2 # int(data["pager"]["pagecount"])

            urls = ["%s%s%s" %(url_dict[key][0], i+1, url_dict[key][1]) for i in range(page_num)]
            cookies = {d['name']: d['value'] for d in driver.get_cookies()}
            driver.quit()

            # 每页爬取
            for url in urls:
                item = fund_monitor.items.FundMonitorItem()
                item['cookies'] = cookies
                item['data'] = {'fund_type': key}
                time.sleep(2)
                yield scrapy.Request(url=url, meta={'item': item}, cookies=item['cookies'], callback=self.parse0)

    def parse0(self, response):
        item = response.meta['item']
        data = json.loads(response.text)
        data = data["data"]

        df = pd.DataFrame(data)
        df.to_excel(u'私募排排网.xlsx')
        df = df.reindex(['city', 'company_id', 'company_short_name', 'fund_id', 'fund_name', 'fund_short_name', 'inception_date', 'province',
                         'register_number', 'web_site'], axis=1)
        df['web_site'] = u'私募排排网'
        df = df.set_index('fund_id')

        for fund_id in df.index:
            item0 = item.copy()
            global detail_cookie
            item0['cookies'] = detail_cookie
            item0['fund_name'] = df.loc[fund_id, 'fund_name']

            # 产品要素
            url = 'http://dc.simuwang.com/fund/getPinfo.html?id=%s&muid=55635' %fund_id
            item0['data'] = df.loc[fund_id,:]
            yield scrapy.Request(url=url, meta={'item': item0}, cookies=item0['cookies'], callback=self.parse1)

            # # 历史净值
            # url = "http://dc.simuwang.com/fund/getNavList.html?id=%s&muid=55635&page=1" % fund_id
            # data = requests_manager.get_html(url, cookies=item0['cookies'])
            # json_data = json.loads(data)
            # print json_data
            # print u'基金%s%s有%s页数据' %(fund_id, item0['fund_name'], json_data['pager']['pagecount'])
            # time.sleep(2)
            #
            # for i in range(json_data['pager']['pagecount']):
            #     url = "http://dc.simuwang.com/fund/getNavList.html?id=%s&muid=55635&page=%s" %(fund_id,i+1)
            #     time.sleep(3)
            #     yield scrapy.Request(url=url, meta={'item': item0}, cookies=item0['cookies'], callback=self.parse2)

    def parse1(self, response):
        item = response.meta['item']
        # print response.text
        df = pd.read_html("<table>%s</table>" %response.text)[0]
        arr = np.array(df).reshape(-1, 2)
        df = pd.DataFrame(arr).set_index(0).T
        # print df.drop([df.columns[-1],], axis=1)
        df = df.reindex([
            u"产品名称", u"认购起点", u"投资顾问", u"追加起点", u"基金管理人", u"封闭期", u"基金托管人",
            u"开放日", u"外包机构方", u"认购费率", u"证券经纪商", u"赎回费率", u"期货经纪商", u"赎回费率说明",
            u"成立日期", u"管理费率", u"运行状态", u"预警线", u"产品类型", u"止损线", u"初始规模", u"业绩报酬",
            u"投资策略/子策略", u"存续期限", u"是否分级", u"备案编号", u"是否伞形"
        ], axis=1)

        file_name = u"私募基金产品要素.xlsx"
        if not os.path.exists(file_name):
            pd.DataFrame([]).to_excel(file_name)

        xls_data = pd.read_excel(file_name)
        xls_data = xls_data.append(df)
        xls_data.to_excel(file_name, index=None)

        # print xls_data

    def parse2(self, response):
        item = response.meta['item']
        json_data = json.loads(response.text)
        file_path = '%s/%s/%s/' %(os.getcwd(), u'私募基金', item['data']['fund_type'])
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        if "data" in json_data:
            df = pd.DataFrame(json_data["data"]).rename({'c':u'净值变动',
                                                         'd':u'日期',
                                                         'n': u'单位净值',
                                                         'cn': u'累计净值(分红再投资)',
                                                         'cnw':u'累计净值(分红不投资)'}, axis=1)
            df[u'净值变动'] = df[u'净值变动'].apply(lambda s:re.sub(r'<.+?>', '', s))
            df[u'基金名称'] = item['fund_name']

            file0 = file_path + item['fund_name'] + '.xlsx'
            if not os.path.exists(file0):
                pd.DataFrame([]).to_excel(file0)

            pd.read_excel(file0).append(df,ignore_index=True).to_excel(file0, index=None)

            print df


    def get_cookies(self, url):
        while True:
            driver = driver_manager.initialization(engine='Chrome')
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
    # http://dc.simuwang.com/ranking/get?page=1&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A4%3Brating_year%3A1%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A
    # http://dc.simuwang.com/ranking/get?page=1&condition=fund_type%3A1%2C6%2C4%2C3%2C8%2C2%3Bret%3A2%3Brating_year%3A1%3Bistiered%3A0%3Bcompany_type%3A1%3Bsort_name%3Aprofit_col2%3Bsort_asc%3Adesc%3Bkeyword%3A

if __name__ == '__main__':
    pass