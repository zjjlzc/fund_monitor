# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: net_value_cal_display.py
    @time: 2017/11/18 23:21
--------------------------------
"""
import sys
import os

import datetime
import pandas as pd
import numpy as np
import time
from contextlib import closing
import pymysql
from dateutil.relativedelta import relativedelta
import calendar


sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('net_value_cal_display.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('net_value_cal_display.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class net_value_cal_display(object):
    def __init__(self):
        pass

    def get_data(self, code, date1, date2):
        date1 = date1.strftime('%Y-%m-%d')
        date2 = date2.strftime('%Y-%m-%d')
        sql = 'SELECT `crawler_key`, `fund_code`, `value_date`,`accumulative_net_value` FROM `eastmoney_daily_data` WHERE `fund_code` = %s AND (`value_date` BETWEEN "%s" AND "%s")' %(code,date1,date2)
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        #print df.head(5)
        return df

    def mul_cal(self, summary_listing, row, fund_code, date1, date2, earnings_type, needed_data, co_col):
        ser = self.simple_cal(fund_code, date1, date2, earnings_type)
        if ser is not None:
            for i in range(len(needed_data)):
                summary_listing.loc[row, needed_data[i]] = ser[co_col[i]]
        return summary_listing

    def simple_cal(self, fund_code, date1, date2, earnings_type):
        # print 'mark00'
        date_col = u'value_date'
        value_col = u'accumulative_net_value'
        target_col = u'retracement'
        df = self.get_data(fund_code, date1, date2)  # 从数据库读取数据
        df.loc[:, date_col] = pd.to_datetime(df.loc[:, date_col])

        # 截取所需时间内的数据,数据取到小于date1的最后一个
        serx = df[date_col][df[date_col].between(datetime.datetime(1800, 1, 1), date1 - datetime.timedelta(1))]
        if not serx.empty:
            serx = serx.sort_values()
            date1 = serx.iat[-1]

        df = df[df[date_col].between(date1, date2)]
        # 修正日期
        df = df.sort_values([date_col, ])  # 按时间排序
        if df.empty:
            return None
        date1 = df[date_col].iat[0]
        date2 = df[date_col].iat[-1]

        func_max = lambda date: max(df[value_col][df[date_col] <= date])
        df.index = range(df.shape[0])

        df['accumulative_net_value'] = df['accumulative_net_value'].astype(np.float16)
        for i in range(1, df.shape[0]):
            # print 'mark1'
            value0 = df.loc[i, value_col]
            date0 = df.loc[i, date_col]
            df.loc[i, 'max'] = func_max(date0)
            if value0 < func_max(date0):
                df.loc[i, target_col] = (value0 / func_max(date0)) - 1
            else:
                df.loc[i, target_col] = 0

        #df.to_csv('caculated_data.csv', encoding='utf_8_sig')

        if earnings_type == u'年化收益':
            product_yield = (df[value_col].iat[-1] / df[value_col].iat[0] - 1) * 365.0 / (
            df[date_col].iat[-1] - df[date_col].iat[0]).days  # 最后一个除以第一个，减1
        elif earnings_type == u'收益':
            product_yield = df[value_col].iat[-1] / df[value_col].iat[0] - 1  # 最后一个除以第一个，减1
        else:
            product_yield = None
        retracement = min(df[target_col].dropna())
        earnings_retracement_ratio = product_yield / (-retracement)

        #print date1, date2, 'product_yield,retracement,earnings_retracement_ratio:', product_yield, retracement, earnings_retracement_ratio

        # df.to_csv('cal-' + file_name, encoding='utf8')
        return pd.Series([df['fund_code'].iat[0], product_yield, retracement, earnings_retracement_ratio],
                         index=[u'基金代号', earnings_type, u'回撤', u'收益回撤比'])

    def main(self):
        with open('important_fund.txt', 'r') as f:
            code_list = f.read().split('\n')

        sql = """
        SELECT DISTINCT `fund_code` FROM `eastmoney_daily_data`
        """
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df0 = pd.read_sql(sql, conn)
        for code in df0.iloc[:,0].tolist():
            if code not in code_list:
                code_list.append(code)

        df = pd.DataFrame(code_list, columns=['fund_code',])
        df.index = df['fund_code']

        for s in ['成立日期','规模（最新/6月末/3月末/最初）','机构持有/个人持有','基金类型','基金经理']:
            df[s] = ''

        date_today = datetime.datetime.now()

        start_time = time.time()
        for code in df.index:


            #code = df.loc[r, 'fund_code']

            # 近两月，月底单位净值与累计净值
            date_list = [datetime.datetime(year=date_today.year,month=(date_today - relativedelta(months=i)).month,
                                           day=calendar.monthrange(date_today.year,(date_today - relativedelta(months=i)).month)[1]) for i in [1,2]]
            for date0 in date_list:
                date_str = date0.strftime('%Y-%m-%d')
                # 万一没有数据，不会打乱格式
                df[date_str + "单位净值"] = None
                df[date_str + "累计净值"] = None
                sql = """
                SELECT `net_asset_value`, `accumulative_net_value` FROM `eastmoney_daily_data`
                WHERE `value_date` = "%s" AND `fund_code` = "%s"
                """ %(date_str,code)
                #print sql
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)
                if not df0.empty:
                    df.loc[code, date_str + "单位净值"] = df0.loc[0, 'net_asset_value']
                    df.loc[code, date_str + "累计净值"] = df0.loc[0, 'accumulative_net_value']

            # 近三天，累计净值
            for date0 in [date_today+relativedelta(days=-i) for i in [1,2,3]]:
                date_str = date0.strftime('%Y-%m-%d')
                df[date_str + "累计净值"] = None # 万一没有数据，不会打乱格式
                sql = """
                SELECT `net_asset_value`, `accumulative_net_value` FROM `eastmoney_daily_data`
                WHERE `value_date` = "%s" AND `fund_code` = "%s"
                """ %(date_str,code)
                #print sql
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)
                if not df0.empty:
                    #df.loc[code, date_str + "单位净值"] = df0.loc[0, 'net_asset_value']
                    df.loc[code, date_str + "累计净值"] = df0.loc[0, 'accumulative_net_value']

            # 今天与上个月底的股票仓位
            date_1 = datetime.datetime(year=date_today.year, month=(date_today - relativedelta(months=1)).month,
                              day=calendar.monthrange(date_today.year, (date_today - relativedelta(months=1)).month)[1]).strftime('%Y-%m-%d')
            df[date_1 + "股票仓位"] = None

            df[date_today.strftime('%Y-%m-%d') + "股票仓位"] = None

            # 今天日收益
            ser = self.simple_cal(code, date_today, date_today,u'收益')
            df.loc[code, '今日收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 近两个月的月收益率
            for date1 in [datetime.datetime(year=date_today.year,month=(date_today - relativedelta(months=i)).month,day=1) for i in [1,2]]:
                date2 = datetime.datetime(year=date1.year, month=date1.month, day=calendar.monthrange(date1.year,month=date1.month)[1])
                ser = self.simple_cal(code, date1, date2,u'收益')
                df.loc[code, '%s年%s月-月收益率' %(date1.year,date1.month)] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 今年的季度数据
            for month1 in [1,4,7,10]:
                month2 = month1 + 2
                date1 = datetime.datetime(year=date_today.year,month=month1,day=1)
                date2 = datetime.datetime(year=date_today.year,month=month2,day=calendar.monthrange(date_today.year,month=month2)[1])
                ser = self.simple_cal(code, date1, date2, u'收益')
                df.loc[code, '%s年%s月至%s月-月收益率' % (date1.year, date1.month, date2.month)] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 去年最后三个月的收益率
            for month0 in [12,11,10]:
                date1 = datetime.datetime(year=date_today.year-1, month=month0, day=1)
                date2 = datetime.datetime(year=date_today.year-1, month=month0, day=calendar.monthrange(date_today.year-1, month=month0)[1])
                ser = self.simple_cal(code, date1, date2, u'收益')
                df.loc[code, '%s年%s月-月收益率' % (date1.year, date1.month)] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 近一周收益率
            date1 = date_today
            date2 = date_today - datetime.timedelta(1)
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '近一周收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 近一个月 收益率，回撤，收益回撤比
            date1 = date_today
            date2 = date_today + relativedelta(months=-1)
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '近一个月收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '近一个月回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '近一个月收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 近三个月 收益率，回撤，收益回撤比
            date1 = date_today
            date2 = date_today + relativedelta(months=-3)
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '近三个月收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '近三个月回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '近三个月收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 近六个月 收益率，回撤，收益回撤比
            date1 = date_today
            date2 = date_today + relativedelta(months=-6)
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '近六个月收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '近六个月回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '近六个月收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 近一年 收益率，回撤，收益回撤比
            date1 = date_today
            date2 = date_today + relativedelta(years=-1)
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '近一年收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '近一年回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '近一年收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 近两年 收益率，回撤，收益回撤比
            date1 = date_today
            date2 = date_today + relativedelta(years=-2)
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '近两年收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '近两年回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '近两年收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 今年以来 收益率，回撤，收益回撤比
            date1 = datetime.datetime(year=date_today.year, month=1, day=1)
            date2 = date_today
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '今年以来收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '今年以来回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '今年以来收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 成立以来 收益率，回撤，收益回撤比
            date1 = datetime.datetime(year=1900, month=1, day=1)
            date2 = date_today
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '成立以来收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '成立以来回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '成立以来收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            # 2016年2月29日以来 收益率，回撤，收益回撤比
            date1 = datetime.datetime(year=2016, month=2, day=29)
            date2 = date_today
            ser = self.simple_cal(code, date1, date2, u'收益')
            df.loc[code, '2016年2月29日以来收益率'] = ser[u'收益'] if ser is not None and not ser.empty else None
            df.loc[code, '2016年2月29日以来回撤'] = ser[u'回撤'] if ser is not None and not ser.empty else None
            df.loc[code, '2016年2月29日以来收益回撤比'] = ser[u'收益回撤比'] if ser is not None and not ser.empty else None

            print df
            print "耗时:", time.time() - start_time

        df['2017年9月现金配置'] = ''
        df['2017年6月现金配置'] = ''
        df['2017年9月债券配置'] = ''
        df['2017年6月债券配置'] = ''
            #date1 = datetime.datetime(1900, 1, 1)
            #date2 = datetime.datetime(2017, 10, 31)
            #summary_listing = self.simple_cal(code, date1, date2, u'年化收益')







if __name__ == '__main__':
    net_value_cal_display = net_value_cal_display()
    net_value_cal_display.main()
