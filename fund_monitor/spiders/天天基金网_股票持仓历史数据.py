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
    name = "0002"

    def start_requests(self):
        self.urls = ["http://fund.eastmoney.com/data/fundranking.html",]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse00)

    def parse00(self, response):
        try:
            with open('important_fund.txt','r') as f:
                l = f.read().split('\n')

            for fund_code in l:
                item = fund_monitor.items.FundMonitorItem()
                item['fund_code'] = fund_code
                #item['fund_name'] = l[1]
                item['url'] = 'http://fund.eastmoney.com/f10/ccmx_%s.html' %fund_code

                yield scrapy.Request(item['url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse0(self, response):
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
            
    def parse(self, response):
        driver = driver_manager.initialization()
        try:
            driver.get('about:blank')
            driver.get(response.url)

            e_table = driver.find_element_by_id('dbtable')
            e_as = e_table.find_element_by_tag_name('thead').find_elements_by_tag_name('a')
            code_set = set()
            for e_a in e_as[6:15]:
                title = e_a.text
                e_a.click()
                time.sleep(1)
                bs_obj = bs4.BeautifulSoup(driver.page_source, 'html.parser')
                e_table = bs_obj.find('table', id='dbtable')
                df = pd.read_html(e_table.prettify(encoding='utf8'),encoding='utf8')[0]
                df.iloc[:, 2] = df.iloc[:,2].apply(lambda i:'{0:0>6}'.format(i)) # 数字转全6位字符串
                code_set.update(df.iloc[:,2].tolist())

            for code in code_set:
                item = fund_monitor.items.FundMonitorItem()
                item['fund_code'] = code
                item['url'] = 'http://fund.eastmoney.com/f10/ccmx_%s.html' %code

                yield scrapy.Request(item['url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
        except:
            log_obj.error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))
        finally:
            driver.quit()

    # def parse1(self, response):
    #     print "准备解析:", response.url
    #     item = response.meta['item']
    #
    #     if item['fund_code'] in blacklist:
    #         raise Exception('此基金已列入黑名单')
    #
    #     url = "http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=jjcc&code=169105&topline=200"
    #
    #     for e_div in e_boxes:
    #         # print e_div
    #         title = e_div.find('h4', class_="t").get_text(strip=True)
    #         print response.url
    #         print title
    #         converters = {u'股票代码': lambda s: str(s)}
    #         df0 = pd.read_html(e_div.table.prettify(encoding='utf8'), encoding='utf8', converters=converters)[0]
    #         df0.columns = [re.sub(r'\s+', '', s) for s in df0.columns]
    #
    #         func = lambda s: re.search(ur'占净值|持股数|持仓市值', s).group() if re.search(ur'占净值|持股数|持仓市值', s) else s
    #         df0.columns = [func(s) for s in df0.columns]
    #
    #         df0[u'标题'] = title
    #         df0[u'cut_off_date'] = title.split(u'截止至：')[-1]
    #         df0[u'对应基金'] = item[u'fund_code']
    #
    #         df0[u'年份'] = year0
    #
    #         df0 = df0.rename({u'股票代码': u'stock_code', u'股票名称': u'stock_name', u'占净值': u'net_value_ratio',
    #                           u'持股数': u'share_holding', u'持仓市值': u'market_value', u'对应基金': u'fund_code',
    #                           u'标题': u'title', u'年份': u'year'}, axis=1)
    #
    #         df0 = df0.drop([u'序号', u'相关资讯', u'最新价', u'涨跌幅'], axis=1, errors='ignore')
    #         df0[u'crawler_key'] = df0[u'fund_code'] + u'/' + df0[u'stock_code'] + u'/' + df0[u'cut_off_date']
    #         # print df0.columns
    #         # for i in range(df0.shape[0]):
    #         #    print df0.iloc[i,:].tolist()
    #         mysql_connecter.insert_df_data(df0, u'fund_holdings')
    #
    #
    #
    #     except:
    #         log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
    #         # yield response.meta['item']
    #     finally:
    #         driver.quit()

    # def parse1(self, response):
    #     print "准备解析:",response.url
    #     item = response.meta['item']
    #
    #     if item['fund_code'] in blacklist:
    #         raise Exception('此基金已列入黑名单')
    #
    #     driver = driver_manager.initialization()
    #     try:
    #         driver.get('about:blank')
    #         driver.get(response.url)
    #         time.sleep(1)
    #         page_buttons = driver.find_element_by_class_name('nt')
    #         #print page_buttons.text, '\n' *3
    #         #e_first_year = driver.find_element_by_class_name('box nt').find_element_by_tag_name('label').text
    #         #df = pd.DataFrame([])
    #         while True:
    #             #with open('C:\\Users\\Administrator\\Desktop\\test.html','w') as f:
    #             #    f.write(driver.page_source)
    #
    #             # dian
    #             page_len = int(len(driver.find_elements_by_class_name('tfoot')))
    #             i = 0
    #             while True:
    #                 # 如果点击元素后，造成元素没显示出来，则等待
    #                 tick_times = 0
    #                 if len(driver.find_elements_by_class_name('tfoot')) != page_len:
    #                     print "waiting for 'tfoot's to show"
    #                     time.sleep(1)
    #                     if tick_times > 10:
    #                         raise
    #                     continue
    #                 elif i not in range(page_len):
    #                     break
    #                 self.testing_output(driver.page_source)
    #                 button = driver.find_elements_by_class_name('tfoot')[i]
    #                 button.find_element_by_tag_name('a').click()
    #                 i = i + 1
    #
    #             bs_obj = bs4.BeautifulSoup(driver.page_source,'html.parser')
    #             e_boxes = bs_obj.find('div', id='cctable').find_all('div',class_='box')
    #             year0 = bs_obj.find('div', class_='box nt').find('label', class_='cur').get_text(strip=True)
    #
    #             for e_div in e_boxes:
    #                 #print e_div
    #                 title = e_div.find('h4',class_="t").get_text(strip=True)
    #                 print response.url
    #                 print title
    #                 converters = {u'股票代码':lambda s:str(s)}
    #                 df0 = pd.read_html(e_div.table.prettify(encoding='utf8'),encoding='utf8',converters=converters)[0]
    #                 df0.columns = [re.sub(r'\s+','',s) for s in df0.columns]
    #
    #                 func = lambda s:re.search(ur'占净值|持股数|持仓市值', s).group() if re.search(ur'占净值|持股数|持仓市值', s) else s
    #                 df0.columns = [func(s) for s in df0.columns]
    #
    #                 df0[u'标题'] = title
    #                 df0[u'cut_off_date'] = title.split(u'截止至：')[-1]
    #                 df0[u'对应基金'] = item[u'fund_code']
    #
    #
    #                 df0[u'年份'] = year0
    #
    #                 df0 = df0.rename({u'股票代码':u'stock_code', u'股票名称':u'stock_name', u'占净值':u'net_value_ratio',
    #                                   u'持股数':u'share_holding', u'持仓市值':u'market_value', u'对应基金':u'fund_code',
    #                                   u'标题':u'title', u'年份':u'year'}, axis=1)
    #
    #                 df0 = df0.drop([u'序号', u'相关资讯', u'最新价', u'涨跌幅'], axis=1, errors='ignore')
    #                 df0[u'crawler_key'] = df0[u'fund_code'] + u'/' + df0[u'stock_code'] + u'/' + df0[u'cut_off_date']
    #                 #print df0.columns
    #                 #for i in range(df0.shape[0]):
    #                 #    print df0.iloc[i,:].tolist()
    #                 mysql_connecter.insert_df_data(df0, u'fund_holdings')
    #
    #                 #df = df.append(df0,ignore_index=True)
    #                 #df.to_csv('C:\\Users\\Administrator\\Desktop\\%s.csv' %item['fund_code'],encoding='utf8')
    #
    #             # 翻页
    #             year0 = int(re.search(r'\d+',year0).group()) - 1
    #             if str(year0) not in page_buttons.text:
    #                 break
    #             else:
    #                 for button in page_buttons.find_elements_by_tag_name('label'):
    #                     if str(year0) in button.text:
    #                         button.click()
    #                         time.sleep(2)
    #                         break
    #
    #     except:
    #         log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
    #         #yield response.meta['item']
    #     finally:
    #         driver.quit()

    def testing_output(self, s):
        with open('C:\\Users\\Administrator\\Desktop\\test.html','w') as f:
            f.write(s)

if __name__ == '__main__':
    pass