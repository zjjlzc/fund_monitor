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

import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
log_obj = set_log.Logger(log_path, set_log.logging.WARNING, set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=True) # 是否需要在每次运行程序前清空Log文件

class Spider(scrapy.Spider):
    name = "0002"

    def start_requests(self):
        self.urls = ["http://fund.eastmoney.com/data/fundranking.html",]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

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

    def parse1(self, response):
        print "准备解析:",response.url
        item = response.meta['item']
        if os.path.exists(r'C:\Users\Administrator\Desktop\first50\%s.csv' %item['fund_code']):
            print item['fund_code'],'\n'
            print 'pass \n' * 3
            yield None
        else:
            driver = driver_manager.initialization()
            try:
                driver.get('about:blank')
                driver.get(response.url)
                time.sleep(1)
                page_buttons = driver.find_element_by_class_name('nt')
                #print page_buttons.text, '\n' *3
                #e_first_year = driver.find_element_by_class_name('box nt').find_element_by_tag_name('label').text
                df = pd.DataFrame([])
                while True:
                    #with open('C:\\Users\\Administrator\\Desktop\\test.html','w') as f:
                    #    f.write(driver.page_source)

                    # dian
                    page_len = int(len(driver.find_elements_by_class_name('tfoot')))
                    i = 0
                    while True:
                        # 如果点击元素后，造成元素没显示出来，则等待
                        tick_times = 0
                        if len(driver.find_elements_by_class_name('tfoot')) != page_len:
                            print "waiting for 'tfoot's to show"
                            time.sleep(1)
                            if tick_times > 10:
                                raise
                            continue
                        elif i not in range(page_len):
                            break
                        self.testing_output(driver.page_source)
                        button = driver.find_elements_by_class_name('tfoot')[i]
                        button.find_element_by_tag_name('a').click()
                        i = i + 1

                    bs_obj = bs4.BeautifulSoup(driver.page_source,'html.parser')
                    e_boxes = bs_obj.find('div', id='cctable').find_all('div',class_='box')
                    year0 = bs_obj.find('div', class_='box nt').find('label', class_='cur').get_text(strip=True)
                    print
                    for e_div in e_boxes:
                        #print e_div
                        title = e_div.find('h4',class_="t").get_text(strip=True)
                        print title
                        df0 = pd.read_html(e_div.table.prettify(encoding='utf8'),encoding='utf8')[0]
                        df0['对应基金'] = item['fund_code']
                        df0['标题'] = title

                        df0['年份'] = year0
                        df = df.append(df0,ignore_index=True)
                        df.to_csv('C:\\Users\\Administrator\\Desktop\\%s.csv' %item['fund_code'],encoding='utf8')

                    # 翻页
                    year0 = int(re.search(r'\d+',year0).group()) - 1
                    if str(year0) not in page_buttons.text:
                        break
                    else:
                        for button in page_buttons.find_elements_by_tag_name('label'):
                            if str(year0) in button.text:
                                button.click()
                                time.sleep(2)
                                break

            except:
                log_obj.error("%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
                #yield response.meta['item']
            finally:
                driver.quit()

    def testing_output(self, s):
        with open('C:\\Users\\Administrator\\Desktop\\test.html','w') as f:
            f.write(s)

if __name__ == '__main__':
    pass