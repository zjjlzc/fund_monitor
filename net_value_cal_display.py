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
import traceback

import pandas as pd
import numpy as np
import time
from contextlib import closing
import pymysql
import re
import dateutil.relativedelta

import xlrd
import xlwt
from dateutil.relativedelta import relativedelta
import calendar
import json
from xlutils.copy import copy
import api51
api51 = api51.api51()
import calculation
calculation = calculation.calculation()

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('net_value_cal_display.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('net_value_cal_display.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件



user_key = '1923eae2a3054d4c92a7eb74d7f65396'

with open('stock_blacklist.txt','r') as f:
    stock_blacklist = f.read().split('\n')

d = {
    'prod_code':'000001.SS',
    'candle_mode':'0',
    'data_count':'1000',
    'get_type':'offset',
    'search_direction':'1',
    'candle_period':'6',
}
json_data = api51.connect(user_key, d)
data000001 = pd.DataFrame(json_data['data']['candle']['000001.SS'])

date_ser = data000001[0].apply(lambda num:datetime.datetime.strptime(str(num),'%Y%m%d'))
last_date = date_ser.iloc[-1]

class net_value_cal_display(object):

    def __init__(self, method='daily'):
        self.method = method
        print 'method=', self.method

        # self.date_ser = date_ser.copy()

        global last_date
        self.daily_report_date = {
            u'5天': {
                'date1': date_ser.iloc[-5],
                'date2': last_date
            },
            u'本周': {
                'date1': last_date - datetime.timedelta(last_date.weekday()),
                'date2': last_date
            },
            u'本月': {
                'date1': datetime.datetime(year=last_date.year, month=last_date.month, day=1),
                'date2': last_date
            },
            u'前月': {
                'date1': (last_date - relativedelta(months=1)).replace(day=1),
                'date2': (last_date - relativedelta(months=1)).replace(day=calendar.monthrange((last_date - relativedelta(months=1)).year,
                                                                            month=((last_date - relativedelta(months=1)).month))[1]
                )
            },
            u'本季': {
                'date1': datetime.datetime(year=last_date.year, month=int((last_date.month - 1) / 3) * 3 + 1, day=1),
                'date2': last_date
            }
        }

    # def get_data(self, code, date1, date2):
    #     date1 = date1.strftime('%Y-%m-%d')
    #     date2 = date2.strftime('%Y-%m-%d')
    #     sql = 'SELECT `crawler_key`, `fund_code`, `value_date`,`accumulative_net_value` FROM `eastmoney_daily_data` WHERE `fund_code` = %s AND (`value_date` BETWEEN "%s" AND "%s")' %(code,date1,date2)
    #     # print sql
    #     with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
    #         df = pd.read_sql(sql, conn)
    #     # print df.head(5)
    #     return df

    # def simple_cal(self, fund_code, date1, date2, earnings_type):
    #     print u"正在准备计算%s的从%s到%s的%s数据" %(fund_code, date1, date2, earnings_type)
    #     # print 'mark00'
    #     date_col = u'value_date'
    #     value_col = u'accumulative_net_value'
    #     target_col = u'retracement'
    #     df = self.get_data(fund_code, date1, date2)  # 从数据库读取数据
    #     df = df.drop(df.index[df['accumulative_net_value'] == ''], axis=0)
    #     df.loc[:, date_col] = pd.to_datetime(df.loc[:, date_col])
    #
    #     # 截取所需时间内的数据,数据取到小于date1的最后一个
    #     serx = df[date_col][df[date_col].between(datetime.datetime(1800, 1, 1), date1 - datetime.timedelta(1))]
    #     if not serx.empty:
    #         serx = serx.sort_values()
    #         date1 = serx.iat[-1]
    #
    #     df = df[df[date_col].between(date1, date2)].copy()
    #     # 修正日期
    #     df = df.sort_values([date_col, ])  # 按时间排序
    #     if df.empty:
    #         print u'从eastmoney_daily_data读取的数据为空'
    #         return None
    #     date1 = df[date_col].iat[0]
    #     date2 = df[date_col].iat[-1]
    #
    #     func_max = lambda date: max(df[value_col][df[date_col] <= date])
    #     df.index = range(df.shape[0])
    #
    #     df['accumulative_net_value'] = df['accumulative_net_value'].astype(np.float)
    #     for i in range(1, df.shape[0]):
    #         # print 'mark1'
    #         value0 = df.loc[i, value_col]
    #         date0 = df.loc[i, date_col]
    #         df.loc[i, 'max'] = func_max(date0)
    #         if value0 < func_max(date0):
    #             df.loc[i, target_col] = (value0 / func_max(date0)) - 1
    #         else:
    #             df.loc[i, target_col] = 0
    #
    #     #df.to_csv('caculated_data.csv', encoding='utf_8_sig')
    #
    #     if earnings_type == u'年化收益':
    #         product_yield = (df[value_col].iat[-1] / df[value_col].iat[0] - 1) * 365.0 / (
    #                            df[date_col].iat[-1] - df[date_col].iat[0]).days  # 最后一个除以第一个，减1
    #     elif earnings_type == u'收益':
    #         product_yield = df[value_col].iat[-1] / df[value_col].iat[0] - 1  # 最后一个除以第一个，减1
    #     else:
    #         product_yield = None
    #
    #     if target_col in df:
    #         retracement = min(df[target_col].dropna())
    #
    #         if retracement:
    #             earnings_retracement_ratio = float(product_yield) / (-retracement)
    #         else:
    #             earnings_retracement_ratio = None
    #         return pd.Series([df['fund_code'].iat[0], product_yield, retracement, earnings_retracement_ratio],
    #                          index=[u'基金代号', earnings_type, u'回撤', u'收益回撤比'])

    def weekly_report(self):
        """
        周报，默认取值为上周五
        :return:
        """
        print 'self.daily_report_date'
        for key in self.daily_report_date:
            print key, self.daily_report_date[key]

        with open('weekly_fund.txt', 'r') as f:
            code_list = f.read().split('\n')

        df = pd.DataFrame([])

        global date_ser
        global last_date
        # last_friday = max([date for date in date_ser if date.isoweekday()==5]) #  datetime.datetime(year=2017, month=10, day=31) #
        if datetime.datetime.now().isoweekday() > 5:
            last_week_num = last_date.isocalendar()[1]
        else:
            last_week_num = (last_date - relativedelta(weeks=1)).isocalendar()[1]
        last_friday = max([date for date in date_ser if date.isocalendar()[1] == last_week_num])
        last_friday = datetime.datetime(year=2018, month=1, day=19)
        date_ser = date_ser[date_ser<=last_friday]
        print u'weekly_report =>周报截止日期为', last_friday

        output_file = u'%s周报.xls' %last_friday.strftime('%Y-%m-%d')
        if os.path.exists(output_file):
            os.remove(output_file)

        start_time = time.time()
        for fund_code in code_list:
            print 'fund_code =>', fund_code
            try:
                if not fund_code:
                    df.loc[fund_code,:] = None
                    print u'空白行，略过'
                    continue

                if re.search(ur'[^\d]+', fund_code):
                    df.loc[fund_code.decode('gbk'), :] = ''
                    continue

                # 基金类型,基金名称，基金分类
                sql = """
                SELECT `fund_code`, `fund_name`, `fund_type`, `1st_class`, `2nd_class`, `3rd_class` FROM `fund_info` WHERE `fund_code` = '%s'
                """ % fund_code
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                df.loc[fund_code, u'基金名称'] = df0['fund_name'].iloc[0]
                df.loc[fund_code, u'基金类型'] = df0['fund_type'].iloc[0]
                df.loc[fund_code, u'一级分类'] = df0['1st_class'].iloc[0]
                df.loc[fund_code, u'二级分类'] = df0['2nd_class'].iloc[0]
                df.loc[fund_code, u'三级分类'] = df0['3rd_class'].iloc[0]

                # 成立日期
                sql = """
                SELECT `fund_code`, `found_date` FROM `fund_info` WHERE `fund_code` = '%s'
                """ % fund_code
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                df.loc[fund_code, u'成立日期'] = df0['found_date'].iloc[0]

                # 基金基本信息
                sql = """
                SELECT `data_type`, `json_data` FROM `fund_mixed_data` WHERE `fund_code` = '%s' AND `data_type` in ('%s','%s','%s')
                """ % (fund_code, u'规模变动', u'持有人结构', u'基金经理变动')
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    mul_data = pd.read_sql(sql, conn)

                # 规模变动
                df0 = mul_data[mul_data['data_type'] == u'规模变动'].copy()
                df0.loc[:,'value_date'] = df0.loc[:,'json_data'].apply(lambda s:json.loads(s)['value_date'])
                df0.loc[:,'data'] = df0.loc[:,'json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'净资产规模(亿)'])
                df0 = df0.sort_values(['value_date',], ascending=False)

                df.loc[fund_code, u'规模变动(最新/6月末/3月末/最初)'] = '/'.join([str(num) for num in df0.iloc[[0,1,2,-1],:]['data'].tolist()])

                # 机构持有/个人持有
                df0 = mul_data[mul_data['data_type'] == u'持有人结构'].copy()
                df0['value_date'] = df0['json_data'].apply(lambda s: json.loads(s)['value_date'])
                df0[u'机构持有比例'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'机构持有比例'])
                df0[u'个人持有比例'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'个人持有比例'])
                df0 = df0.sort_values(['value_date', ], ascending=False)


                if not df0.empty:
                    df.loc[fund_code, u'机构持有/个人持有'] = str(df0[u'机构持有比例'].iloc[0]) + '/' + str(df0[u'个人持有比例'].iloc[0])

                # 基金经理变动 任职时间
                df0 = mul_data[mul_data['data_type'] == u'基金经理变动'].copy()
                df0[u'基金经理'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'基金经理']['0'])
                df0[u'任职时间'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'任职时间']['0'])

                employment_period = df0[u'任职时间'].iloc[0]
                employment_period = re.search(r'\d{4}-\d{2}-\d{2}',employment_period).group() if re.search(r'\d{4}-\d{2}-\d{2}',employment_period) else ''
                if employment_period:
                    employment_period = str((datetime.datetime.now() - datetime.datetime.strptime(employment_period, '%Y-%m-%d')).days) + u'天'
                df.loc[fund_code, u'基金经理'] = df0[u'基金经理'].iloc[0] + employment_period


                # 预测净值
                date_str = last_friday.strftime('%Y-%m-%d')
                sql = """
                SELECT `value_date`,`estimate_net_value`,`estimate_daily_growth_rate`,`net_asset_value`,`daily_growth_rate` FROM `eastmoney_daily_data`
                WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                ORDER BY `value_date` DESC
                LIMIT 1
                """ %(date_str,fund_code)
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                df.loc[fund_code, datex + u"预估净值"] = df0.loc[0, 'estimate_net_value'] if not df0.empty else None
                df.loc[fund_code, datex + u"预估净值收益率"] = df0.loc[0, 'estimate_daily_growth_rate'] if not df0.empty else None
                df.loc[fund_code, datex + u"结算净值"] = df0.loc[0, 'net_asset_value'] if not df0.empty else None
                df.loc[fund_code, datex + u"结算净值收益率"] = df0.loc[0, 'daily_growth_rate'] if not df0.empty else None


                # 近两月，月底单位净值与累计净值
                date_list = [datetime.datetime(year=last_friday.year,month=(last_friday - relativedelta(months=i)).month,
                                               day=calendar.monthrange(last_friday.year,(last_friday - relativedelta(months=i)).month)[1]) for i in [1,2]]
                for date0 in date_list:
                    date_str = date0.strftime('%Y-%m-%d')
                    # 万一没有数据，不会打乱格式
                    sql = """
                    SELECT `value_date`, `net_asset_value`, `accumulative_net_value` FROM `eastmoney_daily_data`
                    WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                    ORDER BY `value_date` DESC
                    LIMIT 1
                    """ %(date_str,fund_code)
                    #print sql
                    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                        df0 = pd.read_sql(sql, conn)

                    datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                    df.loc[fund_code, datex + u"单位净值"] = df0.loc[0, 'net_asset_value'] if not df0.empty else None
                    # df.loc[fund_code, datex + u"累计净值"] = df0.loc[0, 'accumulative_net_value'] if not df0.empty else None


                # 近三天，单位净值
                for date0 in [date_ser.iloc[-1-i] for i in [1,2,3]]: # last_friday+relativedelta(days=-i)
                    date_str = date0.strftime('%Y-%m-%d')
                    #df[date0.strftime(u'%m月%d日') + "累计净值"] = None # 万一没有数据，不会打乱格式
                    sql = """
                    SELECT `value_date`, `net_asset_value`, `accumulative_net_value` FROM `eastmoney_daily_data`
                    WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                    ORDER BY `value_date` DESC
                    LIMIT 1
                    """ %(date_str,fund_code)
                    #print sql
                    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                        df0 = pd.read_sql(sql, conn)

                    datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                    df.loc[fund_code, datex + u"单位净值"] = df0.loc[0, 'net_asset_value'] if not df0.empty else None
                    #df.loc[fund_code, datex + u"累计净值"] = df0.loc[0, 'accumulative_net_value'] if not df0.empty else None

                # 今天与上个月底的股票仓位预测
                date_1 = datetime.datetime(year=last_friday.year, month=(last_friday - relativedelta(months=1)).month,
                                  day=calendar.monthrange(last_friday.year, (last_friday - relativedelta(months=1)).month)[1])
                for date0 in [last_friday, date_1]:
                    sql = """
                    SELECT `value_date`, `fund_shares_positions` FROM `eastmoney_daily_data`
                    WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                    ORDER BY `value_date` DESC
                    LIMIT 1
                    """ % (date0.strftime('%Y-%m-%d'), fund_code)
                    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                        df0 = pd.read_sql(sql, conn)

                    datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                    df.loc[fund_code, datex + u"股票仓位"] = df0.loc[0, 'fund_shares_positions'] if not df0.empty else None


                # 收益率 最大回撤 收益回撤比 的计算
                sql = """
                SELECT `crawler_key`, `fund_code`, `value_date`,`accumulative_net_value` FROM `eastmoney_daily_data`
                WHERE `fund_code` = %s""" %(fund_code)
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    daily_data = pd.read_sql(sql, conn)

                func = lambda date1,date2: calculation.earnings_cal(fund_code, daily_data, date1=date1, date2=date2,
                                                             date_col='value_date', value_col='accumulative_net_value', data_type='fund')

                # # 今天日收益
                # ser = func(last_friday, last_friday)
                # df.loc[fund_code, u'%s收益率' %last_friday.strftime(u'%m月%d日')] = ser[u'收益率'] if ser is not None and not ser.empty else None



                d = {
                    u'2017年9月30日至11月22日':{
                        'date1': datetime.datetime(year=2017, month=9, day=30),
                        'date2': datetime.datetime(year=2017, month=11, day=22)
                    },
                    u'2017年11月23日至12月15日':{
                        'date1': datetime.datetime(year=2017, month=11, day=23),
                        'date2': datetime.datetime(year=2017, month=12, day=15)
                    },
                    u'2017年1月1日至4月12日':{
                        'date1': datetime.datetime(year=2017, month=1, day=1),
                        'date2': datetime.datetime(year=2017, month=4, day=12)
                    },
                    u'2017年4月13日至5月10日': {
                        'date1': datetime.datetime(year=2017, month=4, day=13),
                        'date2': datetime.datetime(year=2017, month=5, day=10)
                    },
                    u'2017年4月13日至6月1日': {
                        'date1': datetime.datetime(year=2017, month=4, day=13),
                        'date2': datetime.datetime(year=2017, month=6, day=1)
                    },
                    u'2017年6月2日至9月12日': {
                        'date1': datetime.datetime(year=2017, month=6, day=2),
                        'date2': datetime.datetime(year=2017, month=9, day=12)
                    },
                    u'2017年5月12日至11月22日': {
                        'date1': datetime.datetime(year=2017, month=5, day=12),
                        'date2': datetime.datetime(year=2017, month=11, day=22)
                    },
                    u'2017年9月13日至12月15日': {
                        'date1': datetime.datetime(year=2017, month=9, day=13),
                        'date2': datetime.datetime(year=2017, month=12, day=15)
                    },
                    u'2017年11月14日至12月15日': {
                        'date1': datetime.datetime(year=2017, month=11, day=14),
                        'date2': datetime.datetime(year=2017, month=12, day=15)
                    },
                    u'近一周': {
                        'date1': last_friday-datetime.timedelta(6),
                        'date2': last_friday
                    },
                    u'近一个月': {
                        'date1': last_friday+relativedelta(months=-1),
                        'date2': last_friday
                    },
                    u'近三个月': {
                        'date1': last_friday+relativedelta(months=-3),
                        'date2': last_friday
                    },
                    u'近六个月': {
                        'date1': last_friday+relativedelta(months=-6),
                        'date2': last_friday
                    },
                    u'今年前三月': {
                        'date1': datetime.datetime(year=last_friday.year,month=1,day=1),
                        'date2': datetime.datetime(year=last_friday.year,month=3,day=31)
                    },
                    u'近一年': {
                        'date1': last_friday+relativedelta(years=-1),
                        'date2': last_friday
                    },
                    u'近两年': {
                        'date1': last_friday+relativedelta(years=-2),
                        'date2': last_friday
                    },
                    u'今年以来': {
                        'date1': datetime.datetime(year=last_friday.year,month=1,day=1),
                        'date2': last_friday
                    },
                    u'成立以来': {
                        'date1': datetime.datetime(year=1900,month=1,day=1),
                        'date2': last_friday
                    },
                    u'2016年2月29日以来': {
                        'date1': datetime.datetime(year=2016,month=2,day=29),
                        'date2': last_friday
                    }
                }
                for key in d:
                    d[key]['res'] = func(d[key]['date1'], date2=d[key]['date2'])

                for s in [u'2017年1月1日至4月12日',u'2017年4月13日至5月10日',u'2017年4月13日至6月1日',u'2017年5月12日至11月22日',u'2017年6月2日至9月12日',
                          u'2017年9月13日至12月15日',u'2017年9月30日至11月22日',u'2017年11月14日至12月15日',u'2017年11月23日至12月15日']:
                    data_type = u'收益率'
                    df.loc[fund_code, s + data_type] = d[s]['res'][data_type] if d[s]['res'] is not None and not d[s]['res'].empty else None

                # 近三个月的月收益率
                for date1 in [last_friday - relativedelta(months=i) for i in [0,1,2]]:
                    date2 = date1.replace(day=calendar.monthrange(date1.year,month=date1.month)[1])
                    ser = func(date1, date2)
                    df.loc[fund_code, u'%s年%s月收益' %(date1.year,date1.month)] = ser[u'收益率'] if ser is not None and not ser.empty else None

                # 今年的季度数据
                for month1 in [10,7,4,1]:
                    if month1 > last_friday.month:
                        break
                    month2 = month1 + 2
                    date1 = datetime.datetime(year=last_friday.year,month=month1,day=1)
                    date2 = datetime.datetime(year=last_friday.year,month=month2,day=calendar.monthrange(last_friday.year,month=month2)[1])
                    ser = func(date1, date2)
                    df.loc[fund_code, u'%s年%s月-%s月收益' % (date1.year, date1.month, date2.month)] = ser[u'收益率'] if ser is not None and not ser.empty else None

                # 去年最后三个月的收益率
                for month0 in [12,11,10]:
                    date1 = datetime.datetime(year=last_friday.year-1, month=month0, day=1)
                    date2 = datetime.datetime(year=last_friday.year-1, month=month0, day=calendar.monthrange(last_friday.year-1, month=month0)[1])
                    ser = func(date1, date2)
                    df.loc[fund_code, u'%s年%s月收益' % (date1.year, date1.month)] = ser[u'收益率'] if ser is not None and not ser.empty else None

                for s in [u'近一周',u'近一个月',u'近三个月',u'近六个月',u'今年前三月',u'近一年',
                            u'近两年', u'今年以来',u'成立以来',u'2016年2月29日以来']:
                    data_type = u'收益率'
                    df.loc[fund_code, s + data_type] = d[s]['res'][data_type] if d[s]['res'] is not None and not d[s]['res'].empty else None

                for s in [u'近一个月',u'近三个月',u'近六个月',u'近一年', u'近两年', u'今年以来',u'成立以来',u'2016年2月29日以来']:
                    data_type = u'最大回撤'
                    df.loc[fund_code, s + data_type] = d[s]['res'][data_type] if d[s]['res'] is not None and not d[s]['res'].empty else None

                for s in [u'近一年', u'近两年', u'今年以来',u'成立以来',u'2016年2月29日以来']:
                    data_type = u'收益回撤比'
                    df.loc[fund_code, s + data_type] = d[s]['res'][data_type] if d[s]['res'] is not None and not d[s]['res'].empty else None

                # # 今天日收益
                # ser = self.simple_cal(fund_code, last_friday, last_friday,u'收益')
                # df.loc[fund_code, u'%s收益率' %last_friday.strftime(u'%m月%d日')] = ser[u'收益'] if ser is not None and not ser.empty else None
                #
                # # 近三个月的月收益率
                # for date1 in [datetime.datetime(year=last_friday.year,month=(last_friday - relativedelta(months=i)).month,day=1) for i in [0,1,2]]:
                #     date2 = datetime.datetime(year=date1.year, month=date1.month, day=calendar.monthrange(date1.year,month=date1.month)[1])
                #     ser = self.simple_cal(fund_code, date1, date2,u'收益')
                #     df.loc[fund_code, u'%s年%s月收益' %(date1.year,date1.month)] = ser[u'收益'] if ser is not None and not ser.empty else None
                #
                # # 今年的季度数据
                # for month1 in [10,7,4,1]:
                #     month2 = month1 + 2
                #     date1 = datetime.datetime(year=last_friday.year,month=month1,day=1)
                #     date2 = datetime.datetime(year=last_friday.year,month=month2,day=calendar.monthrange(last_friday.year,month=month2)[1])
                #     ser = self.simple_cal(fund_code, date1, date2, u'收益')
                #     df.loc[fund_code, u'%s年%s月-%s月收益' % (date1.year, date1.month, date2.month)] = ser[u'收益'] if ser is not None and not ser.empty else None
                #
                # # 去年最后三个月的收益率
                # for month0 in [12,11,10]:
                #     date1 = datetime.datetime(year=last_friday.year-1, month=month0, day=1)
                #     date2 = datetime.datetime(year=last_friday.year-1, month=month0, day=calendar.monthrange(last_friday.year-1, month=month0)[1])
                #     ser = self.simple_cal(fund_code, date1, date2, u'收益')
                #     df.loc[fund_code, u'%s年%s月收益' % (date1.year, date1.month)] = ser[u'收益'] if ser is not None and not ser.empty else None
                #
                # # 近一周收益率
                # date1 = last_friday - datetime.timedelta(7)
                # date2 = last_friday
                # ser_1w = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 近一个月 收益率，回撤，收益回撤比
                # date1 = last_friday + relativedelta(months=-1)
                # date2 = last_friday
                # ser_1m = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 近三个月 收益率，回撤，收益回撤比
                # date1 = last_friday + relativedelta(months=-3)
                # date2 = last_friday
                # ser_3m = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 近六个月 收益率，回撤，收益回撤比
                # date1 = last_friday + relativedelta(months=-6)
                # date2 = last_friday
                # ser_6m = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 今年前三个月 收益率，回撤，收益回撤比
                # date1 = datetime.datetime(year=last_friday.year, month=1, day=1)
                # date2 = datetime.datetime(year=last_friday.year, month=3, day=31)
                # ser_3m_first = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 近一年 收益率，回撤，收益回撤比
                # date1 = last_friday + relativedelta(years=-1)
                # date2 = last_friday
                # ser_1y = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 近两年 收益率，回撤，收益回撤比
                # date1 = last_friday + relativedelta(years=-2)
                # date2 = last_friday
                # ser_2y = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 今年以来 收益率，回撤，收益回撤比
                # date1 = datetime.datetime(year=last_friday.year, month=1, day=1)
                # date2 = last_friday
                # ser_this_year = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 成立以来 收益率，回撤，收益回撤比
                # date1 = datetime.datetime(year=1900, month=1, day=1)
                # date2 = last_friday
                # ser_so_far = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 2016年2月29日以来 收益率，回撤，收益回撤比
                # date1 = datetime.datetime(year=2016, month=2, day=29)
                # date2 = last_friday
                # ser_20160229 = self.simple_cal(fund_code, date1, date2, u'收益')
                #
                # df.loc[fund_code, u'近一周收益率'] = ser_1w[u'收益'] if ser_1w is not None and not ser_1w.empty else None
                # df.loc[fund_code, u'近一个月收益率'] = ser_1m[u'收益'] if ser_1m is not None and not ser_1m.empty else None
                # df.loc[fund_code, u'近三个月收益率'] = ser_3m[u'收益'] if ser_3m is not None and not ser_3m.empty else None
                # df.loc[fund_code, u'近六个月收益率'] = ser_6m[u'收益'] if ser_6m is not None and not ser_6m.empty else None
                # df.loc[fund_code, u'今年前三月收益率'] = ser_3m_first[u'收益'] if ser_3m_first is not None and not ser_3m_first.empty else None
                # df.loc[fund_code, u'近一年收益率'] = ser_1y[u'收益'] if ser_1y is not None and not ser_1y.empty else None
                # df.loc[fund_code, u'近两年收益率'] = ser_2y[u'收益'] if ser_2y is not None and not ser_2y.empty else None
                # df.loc[fund_code, u'今年以来收益率'] = ser_this_year[u'收益'] if ser_this_year is not None and not ser_this_year.empty else None
                # df.loc[fund_code, u'成立以来收益率'] = ser_so_far[u'收益'] if ser_so_far is not None and not ser_so_far.empty else None
                # df.loc[fund_code, u'2016年2月29日以来收益率'] = ser_20160229[u'收益'] if ser_20160229 is not None and not ser_20160229.empty else None
                #
                # df.loc[fund_code, u'近一个月回撤'] = ser_1m[u'回撤'] if ser_1m is not None and not ser_1m.empty else None
                # df.loc[fund_code, u'近三个月回撤'] = ser_3m[u'回撤'] if ser_3m is not None and not ser_3m.empty else None
                # df.loc[fund_code, u'近六个月回撤'] = ser_6m[u'回撤'] if ser_6m is not None and not ser_6m.empty else None
                # df.loc[fund_code, u'近一年回撤'] = ser_1y[u'回撤'] if ser_1y is not None and not ser_1y.empty else None
                # df.loc[fund_code, u'近两年回撤'] = ser_2y[u'回撤'] if ser_2y is not None and not ser_2y.empty else None
                # df.loc[fund_code, u'今年以来回撤'] = ser_this_year[u'回撤'] if ser_this_year is not None and not ser_this_year.empty else None
                # df.loc[fund_code, u'成立以来回撤'] = ser_so_far[u'回撤'] if ser_so_far is not None and not ser_so_far.empty else None
                # df.loc[fund_code, u'2016年2月29日以来回撤'] = ser_20160229[u'回撤'] if ser_20160229 is not None and not ser_20160229.empty else None
                #
                #
                # df.loc[fund_code, u'近一年收益回撤比'] = ser_1y[u'收益回撤比'] if ser_1y is not None and not ser_1y.empty else None
                # df.loc[fund_code, u'近两年收益回撤比'] = ser_2y[u'收益回撤比'] if ser_2y is not None and not ser_2y.empty else None
                # df.loc[fund_code, u'今年以来收益回撤比'] = ser_this_year[u'收益回撤比'] if ser_this_year is not None and not ser_this_year.empty else None
                # df.loc[fund_code, u'成立以来收益回撤比'] = ser_so_far[u'收益回撤比'] if ser_so_far is not None and not ser_so_far.empty else None
                # df.loc[fund_code, u'2016年2月29日以来收益回撤比'] = ser_20160229[u'收益回撤比'] if ser_20160229 is not None and not ser_20160229.empty else None

                sql = """
                SELECT `fund_code`, `data_type`, `json_data` FROM `fund_mixed_data` WHERE `fund_code` = '%s' AND `data_type` = '%s'
                """ %(fund_code, u'资产配置')
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                df0['value_date'] = df0['json_data'].apply(lambda s: json.loads(s)['value_date'])
                df0[u'股票占净比'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'股票占净比'])
                df0[u'债券占净比'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'债券占净比'])
                df0[u'现金占净比'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'现金占净比'])
                df0 = df0.sort_values(['value_date', ], ascending=False)

                func = lambda s:datetime.datetime.strptime(s, '%Y-%m-%d').strftime('%m月%d日')
                for i in range(df0.shape[0])[:2]:
                    df.loc[fund_code, u'%s股票占净比' %func(df0['value_date'].iloc[i])] = df0[u'股票占净比'].iloc[i]
                    df.loc[fund_code, u'%s债券占净比' %func(df0['value_date'].iloc[i])] = df0[u'债券占净比'].iloc[i]
                    df.loc[fund_code, u'%s现金占净比' %func(df0['value_date'].iloc[i])] = df0[u'债券占净比'].iloc[i]

                #date1 = datetime.datetime(1900, 1, 1)
                #date2 = datetime.datetime(2017, 10, 31)
                #summary_listing = self.simple_cal(fund_code, date1, date2, u'年化收益')

                df.to_excel(u'周报.xls')
                print u"耗时:", time.time() - start_time
            except:
                log_obj.error('%s计算时出错' %fund_code)
                log_obj.error(traceback.format_exc())
            # df = pd.read_csv('net_value_cal_display.csv', encoding='utf8')
            # print df.head(3)
            # df.to_excel('net_value_cal_display.xls', encoding='utf8', index=None)
            #
            # oldWb = xlrd.open_workbook('net_value_cal_display.xls')
            # monitor_col = oldWb.sheet_by_index(0).col_values(9)
            # monitor_col = [s for s in monitor_col if s][1:]
            # print monitor_col
            # bool_col = [float(s.split('/')[0]) / float(s.split('/')[-1])>1 for s in monitor_col]
            # print bool_col
            # newWb = copy(oldWb)
            # newWs = newWb.get_sheet(0)
            #
            #
            # pattern = xlwt.Pattern()  # 创建一个模式
            # pattern.pattern = pattern.SOLID_PATTERN  # 设置其模式为实型
            # pattern.pattern_fore_colour = 3
            # # 设置单元格背景颜色 0 = Black, 1 = White, 2 = Red, 3 = Green, 4 = Blue, 5 = Yellow, 6 = Magenta,  the list goes on...
            # style = xlwt.XFStyle()
            # style.pattern = pattern  # 将赋值好的模式参数导入Style
            #
            # # 机构持有/个人持有位于第9列
            # col = 9
            # for r in range(1, len(bool_col)):
            #     if bool_col[r]:
            #         print r, col, monitor_col[r]
            #         newWs.write(r, col, monitor_col[r], style)
            # newWb.save('net_value_cal_display.xls')

    def daily_report(self, fund_file='important_fund.txt'):
        with open(fund_file, 'r') as f:
            code_list = f.read().split('\n')

        output_file = u'%s净值日报.xls' %datetime.datetime.now().strftime(u'%Y-%m-%d')
        if os.path.exists(output_file):
            os.remove(output_file)

        df = pd.DataFrame([])
        # df.index = df['fund_code'].apply(lambda s:s.decode('gbk'))

        date_today = date_ser.iloc[-1]

        start_time = time.time()
        for fund_code in code_list:
            print 'fund_code = ', fund_code
            try:
                if not fund_code:
                    df.loc[fund_code,:] = None
                    print u'空白行，略过'
                    continue

                if re.search(r'[^\d]+', fund_code):
                    df.loc[fund_code.decode('gbk'), :] = ''
                    continue

                # 基金类型,基金名称，基金分类
                sql = """
                SELECT `fund_code`, `fund_name`, `fund_type`, `1st_class`, `2nd_class`, `3rd_class` FROM `fund_info` WHERE `fund_code` = '%s'
                """ % fund_code
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    fund_info = pd.read_sql(sql, conn)

                df.loc[fund_code, u'基金名称'] = fund_info['fund_name'].iloc[0]


                # 近期净值和收益率
                date_str = date_today.strftime('%Y-%m-%d')
                sql = """
                SELECT `value_date`,`estimate_net_value`,`estimate_daily_growth_rate`,`net_asset_value`,`daily_growth_rate` FROM `eastmoney_daily_data`
                WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                ORDER BY `value_date` DESC
                LIMIT 3
                """ %(date_str,fund_code)
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    net_value_df = pd.read_sql(sql, conn)

                for i in range(1):
                    datex = net_value_df.loc[i, 'value_date'].strftime(u'%m月%d日')
                    df.loc[fund_code, datex + u"预估净值"] = net_value_df.loc[i, 'estimate_net_value'] if not net_value_df.empty else None
                    # df.loc[fund_code, datex + u"结算净值"] = net_value_df.loc[i, 'net_asset_value'] if not net_value_df.empty else None

                for i in range(1,3):
                    datex = net_value_df.loc[i, 'value_date'].strftime(u'%m月%d日')
                    df.loc[fund_code, datex + u"结算净值"] = net_value_df.loc[i, 'net_asset_value'] if not net_value_df.empty else None



                # 昨天与上个月底的股票仓位预测
                date_1 = date_ser[date_ser < datetime.datetime(year=date_today.year, month=date_today.month,day=1)].max()
                for date0 in [date_ser.iloc[-2], date_1]:
                    sql = """
                    SELECT `value_date`, `fund_shares_positions` FROM `eastmoney_daily_data`
                    WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                    ORDER BY `value_date` DESC
                    LIMIT 1
                    """ % (date0.strftime('%Y-%m-%d'), fund_code)
                    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                        df0 = pd.read_sql(sql, conn)

                    datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                    df.loc[fund_code, datex + u"股票仓位"] = df0.loc[0, 'fund_shares_positions'] if not df0.empty else None


                df.loc[fund_code, u"日收益率"] = net_value_df.loc[0, 'estimate_daily_growth_rate'] if not net_value_df.empty else None #

                d = self.daily_report_date.copy()

                date1 = min([d[key]['date1'] for key in d])
                date2 = max([d[key]['date2'] for key in d])
                sql = """
                SELECT `crawler_key`, `fund_code`, `value_date`,`accumulative_net_value` FROM `eastmoney_daily_data`
                WHERE `fund_code` = %s AND (`value_date` BETWEEN "%s" AND "%s")""" %(fund_code,date1,date2)
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                for key in d:
                    d[key]['res'] = calculation.earnings_cal(fund_code, df0, date1=d[key]['date1'], date2=d[key]['date2'],
                                                             date_col='value_date', value_col='accumulative_net_value', data_type='fund')

                for s in [u'5天', u'本周', u'本月', u'前月', u'本季']:
                    data_type = u'收益率'
                    df.loc[fund_code, s + data_type] = "{:.2%}".format(d[s]['res'][data_type]) if d[s]['res'] is not None and not d[s]['res'].empty else None

                for s in [u'5天', u'本月', u'前月', u'本季']:
                    data_type = u'最大回撤'
                    df.loc[fund_code, s + data_type] = "{:.2%}".format(d[s]['res'][data_type]) if d[s]['res'] is not None and not d[s]['res'].empty else None

                for s in [u'本月', u'前月']:
                    data_type = u'收益回撤比'
                    df.loc[fund_code, s + data_type] = "{:.4f}".format(d[s]['res'][data_type]) if d[s]['res'] is not None and not d[s]['res'].empty and d[s]['res'][data_type] else None

                # 补齐基金分类
                df.loc[fund_code, u'基金类型'] = fund_info['fund_type'].iloc[0]
                df.loc[fund_code, u'一级分类'] = fund_info['1st_class'].iloc[0]
                df.loc[fund_code, u'二级分类'] = fund_info['2nd_class'].iloc[0]
                df.loc[fund_code, u'三级分类'] = fund_info['3rd_class'].iloc[0]
                # # 5天
                # date1 = date_ser.iloc[-5]
                # date2 = date_today
                # #print u'5天', date1, date2
                # ser_5d = calculation.earnings_cal(fund_code, date1, date2, u'收益')
                # # 本周
                # date1 = date_today - datetime.timedelta(date_today.weekday())
                # date2 = date_today
                # #print u'本周', date1, date2
                # ser_1w = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 本月
                # date1 = datetime.datetime(year=date_today.year, month=date_today.month, day=1)
                # date2 = date_today
                # #print u'本月', date1, date2
                # ser_1m = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 上月
                # date1 = datetime.datetime(year=date_today.year, month=date_today.month-1, day=1)
                # date2 = datetime.datetime(year=date_today.year, month=date_today.month-1, day=calendar.monthrange(date1.year,month=date1.month)[1])
                # #print u'上月', date1, date2
                # ser_1m_before = self.simple_cal(fund_code, date1, date2, u'收益')
                # # 当季
                # date1 = datetime.datetime(year=date_today.year, month= int((date_today.month - 1) / 3) * 3 + 1, day=1)
                # date2 = date_today
                # #print u'本季', date1, date2
                # ser_1s = self.simple_cal(fund_code, date1, date2, u'收益')

                # df.loc[fund_code, u'5天收益率'] = ser_5d[u'收益'] if ser_5d is not None and not ser_5d.empty else None
                # df.loc[fund_code, u'本周收益率'] = ser_1w[u'收益'] if ser_1w is not None and not ser_1w.empty else None
                # df.loc[fund_code, u'本月收益率'] = ser_1m[u'收益'] if ser_1m is not None and not ser_1m.empty else None
                # df.loc[fund_code, u'前月收益率'] = ser_1m_before[u'收益'] if ser_1m_before is not None and not ser_1m_before.empty else None
                # df.loc[fund_code, u'本季收益率'] = ser_1s[u'收益'] if ser_1s is not None and not ser_1s.empty else None
                #
                # df.loc[fund_code, u'5天回撤'] = ser_5d[u'回撤'] if ser_5d is not None and not ser_5d.empty else None
                # df.loc[fund_code, u'本月回撤'] = ser_1m[u'回撤'] if ser_1m is not None and not ser_1m.empty else None
                # df.loc[fund_code, u'前月回撤'] = ser_1m_before[u'回撤'] if ser_1m_before is not None and not ser_1m_before.empty else None
                # df.loc[fund_code, u'本季回撤'] = ser_1s[u'回撤'] if ser_1s is not None and not ser_1s.empty else None

                # # 计算预估偏离
                # for i in range(3):
                #     datex = df0.loc[i, 'value_date'].strftime(u'%m月%d日')
                #     num1 = df.loc[fund_code, datex + u"预估净值"].iloc[0] if isinstance(df.loc[fund_code, datex + u"预估净值"], pd.core.series.Series) \
                #                                                      else df.loc[fund_code, datex + u"预估净值"]
                #     num2 = df.loc[fund_code, datex + u"结算净值"].iloc[0] if isinstance(df.loc[fund_code, datex + u"结算净值"], pd.core.series.Series) \
                #                                                      else df.loc[fund_code, datex + u"结算净值"]
                #
                #     if num1 and num2:
                #         df.loc[fund_code, datex + u"预估偏离"] = float(num1) - float(num2)
                #     else:
                #         # 保持格式
                #         df.loc[fund_code, datex + u"预估偏离"] = ''

                # df.loc[fund_code, u'本月收益回撤比'] = ser_1m[u'收益回撤比'] if ser_1m is not None and not ser_1m.empty else None
                # df.loc[fund_code, u'前月收益回撤比'] = ser_1m_before[u'收益回撤比'] if ser_1m_before is not None and not ser_1m_before.empty else None

                df.index.name = u'基金代号'
                # df.rename({'fund_code':u'基金代号'}, axis=1).to_csv(output_file, index=None, encoding='ascii')
                df.rename({'fund_code': u'基金代号'}, axis=1).to_excel(output_file)
                print u"耗时:", time.time() - start_time
            except:
                log_obj.error(u'%s计算时出错' %fund_code.decode('gbk'))
                log_obj.error(traceback.format_exc())

        return output_file

    def index_calculation(self):
        index_dict = {u'000016.SH':u'上证50',
                      u'399300.SZ':u'沪深300',
                      u'399905.SZ':u'中证500'}

        output_file = 'index_calculation.xls'

        index_df = pd.DataFrame([])
        for key in index_dict:
            index_code = key
            index_name = index_dict[key]
            d = {
                'prod_code': index_code,
                'candle_mode': '0',
                'data_count': '1000',
                'get_type': 'offset',
                'search_direction': '1',
                'candle_period': '6',
            }
            json_data = api51.connect(user_key, d)
            df = pd.DataFrame(json_data['data']['candle'][index_code], columns=json_data['data']['candle'][u'fields'])
            df['min_time'] = pd.to_datetime(df['min_time'], format='%Y%m%d')

            index_df.loc[index_code, u'指数名称'] = index_name

            d = self.daily_report_date.copy()
            d[u'今日'] = {}
            d[u'今日']['date1'] = date_ser.iloc[-2]
            d[u'今日']['date2'] = date_ser.iloc[-1]

            for key in d:
                # print key
                d[key]['res'] = calculation.earnings_cal(index_df, df, date1=d[key]['date1'], date2=d[key]['date2'],
                                                         date_col='min_time', value_col='close_px')


            for s in [u'今日', u'5天', u'本周', u'本月', u'前月', u'本季']:
                data_type = u'收益率'
                index_df.loc[index_code, s + data_type] = "{:.2%}".format(d[s]['res'][data_type]) if d[s]['res'] is not None and not d[s]['res'].empty else None

            for s in [u'5天', u'本月', u'前月', u'本季']:
                data_type = u'最大回撤'
                index_df.loc[index_code, s + data_type] = "{:.2f}".format(d[s]['res'][data_type]) if d[s]['res'] is not None and not d[s]['res'].empty else None

            for s in [u'本月', u'前月']:
                data_type = u'收益回撤比'
                # print d[s]['res'], type(d[s]['res'])
                index_df.loc[index_code, s + data_type] = "{:.2f}".format(d[s]['res'][data_type]) if d[s]['res'] is not None and not d[s]['res'].empty and d[s]['res'][data_type] else None

            index_df.to_excel(output_file)

        return output_file

    def date_range_display(self):
        with open('weekly_fund.txt', 'r') as f:
            code_list = f.read().split('\n')

        date_dict = {
            u'2017年1月1日至4月12日': {
                'date1': datetime.datetime(year=2017, month=1, day=1),
                'date2': datetime.datetime(year=2017, month=4, day=12)
            },
            u'2017年4月13日至6月1日': {
                'date1': datetime.datetime(year=2017, month=4, day=13),
                'date2': datetime.datetime(year=2017, month=6, day=1)
            },
            u'2017年6月2日至10月13日': {
                'date1': datetime.datetime(year=2017, month=6, day=2),
                'date2': datetime.datetime(year=2017, month=10, day=13)
            },
            u'2017年10月13日至11月17日': {
                'date1': datetime.datetime(year=2017, month=10, day=13),
                'date2': datetime.datetime(year=2017, month=11, day=17)
            },
            u'2017年11月17日至12月7日': {
                'date1': datetime.datetime(year=2017, month=11, day=17),
                'date2': datetime.datetime(year=2017, month=12, day=7)
            },
            u'2017年12月8日至12月29日': {
                'date1': datetime.datetime(year=2017, month=12, day=8),
                'date2': datetime.datetime(year=2017, month=12, day=30)
            }
        }

        sql = """
        SELECT `crawler_key`,a.`fund_code`, `value_date`,`accumulative_net_value`, b.`fund_name` FROM `eastmoney_daily_data` a
        LEFT JOIN `fund_info` b ON a.`fund_code` = b.`fund_code`
        WHERE a.`fund_code` IN (%s)""" % (','.join(['"%s"' %code for code in code_list]))
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            fund_data = pd.read_sql(sql, conn)
        print fund_data

        d = {
            'prod_code': '000300.SS',
            'candle_mode': '0',
            'data_count': '1000',
            'get_type': 'offset',
            'search_direction': '1',
            'candle_period': '6',
        }
        json_data = api51.connect(user_key, d)
        hs300 = pd.DataFrame(json_data['data']['candle']['000300.SS'], columns=json_data['data']['candle']['fields'])
        hs300['min_time'] = pd.to_datetime(hs300['min_time'], format='%Y%m%d')
        # print 'hs300'
        # print hs300

        d = {
            'prod_code': '000016.SH',
            'candle_mode': '0',
            'data_count': '1000',
            'get_type': 'offset',
            'search_direction': '1',
            'candle_period': '6',
        }
        json_data = api51.connect(user_key, d)
        sz50 = pd.DataFrame(json_data['data']['candle']['000016.SH'], columns=json_data['data']['candle']['fields'])
        sz50['min_time'] = pd.to_datetime(sz50['min_time'], format='%Y%m%d')

        d = {
            'prod_code': '399905.SZ',
            'candle_mode': '0',
            'data_count': '1000',
            'get_type': 'offset',
            'search_direction': '1',
            'candle_period': '6',
        }
        json_data = api51.connect(user_key, d)
        zz500 = pd.DataFrame(json_data['data']['candle']['399905.SZ'], columns=json_data['data']['candle']['fields'])
        zz500['min_time'] = pd.to_datetime(zz500['min_time'], format='%Y%m%d')

        file_name = u'区间.xlsx'
        if os.path.exists(file_name):
            os.remove(file_name)
            wb = xlwt.Workbook()
            wb.add_sheet('sheet1')
            wb.save(file_name)

        writer = pd.ExcelWriter(file_name)
        for key in date_dict:
            df = pd.DataFrame([])
            date1 = date_dict[key]['date1']
            date2 = date_dict[key]['date2']

            func = lambda date1, date2: calculation.earnings_cal(u'沪深300', hs300, date1=date1, date2=date2,
                                                                 date_col='min_time', value_col='close_px', data_type='fund')
            df.loc[u'沪深300', u'收益率'] = func(date1, date2)[u'收益率']
            df.loc[u'沪深300', u'最大回撤'] = func(date1, date2)[u'最大回撤']

            func = lambda date1, date2: calculation.earnings_cal(u'上证50', sz50, date1=date1, date2=date2,
                                                                 date_col='min_time', value_col='close_px', data_type='fund')
            df.loc[u'上证50', u'收益率'] = func(date1, date2)[u'收益率']
            df.loc[u'上证50', u'最大回撤'] = func(date1, date2)[u'最大回撤']

            func = lambda date1, date2: calculation.earnings_cal(u'中证500', zz500, date1=date1, date2=date2,
                                                                 date_col='min_time', value_col='close_px', data_type='fund')
            df.loc[u'中证500', u'收益率'] = func(date1, date2)[u'收益率']
            df.loc[u'中证500', u'最大回撤'] = func(date1, date2)[u'最大回撤']

            for fund_code in code_list:
                print 'fund_code = ', fund_code
                try:
                    if not fund_code:
                        df.loc[fund_code, :] = None
                        print u'空白行，略过'
                        continue

                    if re.search(r'[^\d]+', fund_code):
                        df.loc[fund_code.decode('gbk'), :] = ''
                        continue
                    data = fund_data[fund_data['fund_code'] == fund_code].copy()
                    func = lambda date1,date2: calculation.earnings_cal(fund_code, data, date1=date1, date2=date2,
                                                                 date_col='value_date', value_col='accumulative_net_value', data_type='fund')
                    df.loc[fund_code, u'基金名称'] = data['fund_name'].iloc[0]
                    df.loc[fund_code, u'收益率'] = func(date1, date2)[u'收益率']
                    df.loc[fund_code, u'最大回撤'] = func(date1, date2)[u'最大回撤']
                    print df
                    df.to_excel(writer, key)

                except:
                    print traceback.format_exc()
        writer.save()

    def special_fund(self):
        with open('special_fund.json', 'r') as f:
            special_fund = json.load(f, encoding='utf_8_sig')

        d = {
            'prod_code': '000300.SS',
            'candle_mode': '0',
            'data_count': '1000',
            'get_type': 'offset',
            'search_direction': '1',
            'candle_period': '6',
        }
        json_data = api51.connect(user_key, d)
        hs300 = pd.DataFrame(json_data['data']['candle']['000300.SS'], columns=json_data['data']['candle']['fields'])
        hs300['min_time'] = pd.to_datetime(hs300['min_time'], format='%Y%m%d')
        # print 'hs300'
        # print hs300

        d = {
            'prod_code': '000016.SH',
            'candle_mode': '0',
            'data_count': '1000',
            'get_type': 'offset',
            'search_direction': '1',
            'candle_period': '6',
        }
        json_data = api51.connect(user_key, d)
        sz50 = pd.DataFrame(json_data['data']['candle']['000016.SH'], columns=json_data['data']['candle']['fields'])
        sz50['min_time'] = pd.to_datetime(sz50['min_time'], format='%Y%m%d')

        d = {
            'prod_code': '399905.SZ',
            'candle_mode': '0',
            'data_count': '1000',
            'get_type': 'offset',
            'search_direction': '1',
            'candle_period': '6',
        }
        json_data = api51.connect(user_key, d)
        zz500 = pd.DataFrame(json_data['data']['candle']['399905.SZ'], columns=json_data['data']['candle']['fields'])
        zz500['min_time'] = pd.to_datetime(zz500['min_time'], format='%Y%m%d')

        index_data ={
            u'沪深300': hs300,
            u'上证50': sz50,
            u'中证500': zz500
        }

        sql = """
        SELECT `fund_code`, `fund_name` FROM `fund_info`
        """
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            fund_name_df = pd.read_sql(sql, conn)
        fund_name_d = fund_name_df.set_index('fund_code').to_dict()['fund_name']

        title = [
            u"基金名称",
            u"拟合净值收益率",
            u"实际净值收益率",
            u"收益率比较",
            u"拟合净值最大回撤",
            u"实际净值最大回撤",
            u"拟合净值收益回撤比",
            u"实际净值收益回撤比",
        ]
        table_df = pd.DataFrame([], columns=title)
        output_data = pd.DataFrame([])
        for fund_info in special_fund:
            print fund_info
            date1 = datetime.datetime.strptime(fund_info['date1'],"%Y.%m.%d")
            date2 = datetime.datetime.strptime(fund_info['date2'],"%Y.%m.%d")
            date_str = date1.strftime("%Y.%m.%d") + "-" + date2.strftime("%m.%d")
            # print date_str
            # print table_df
            table_df = table_df.append(pd.Series([]), ignore_index=True)
            table_df.iloc[table_df.shape[0]-1, 0] = date_str
            table_df.iloc[table_df.shape[0]-1, 1] = u'收益率'
            table_df.iloc[table_df.shape[0]-1, 2] = u'最大回撤'
            table_df.iloc[table_df.shape[0]-1, 3] = u'收益回撤比'

            for s in [u'沪深300', u'上证50', u'中证500']:
                table_df = table_df.append(pd.Series([]), ignore_index=True)
                table_df.iloc[table_df.shape[0] - 1, 0] = s
                func = lambda date1, date2: calculation.earnings_cal(s, index_data[s], date1=date1, date2=date2,
                                                                     date_col='min_time', value_col='close_px', data_type='fund')
                table_df.iloc[table_df.shape[0] - 1, 1] = func(date1, date2)[u'收益率']
                table_df.iloc[table_df.shape[0] - 1, 2] = func(date1, date2)[u'最大回撤']
                table_df.iloc[table_df.shape[0] - 1, 3] = func(date1, date2)[u'收益回撤比']

            cal_fund_df = pd.DataFrame([])
            cut_off_date_list = []
            for fund_type in fund_info['fund']:
                for fund_code in fund_info['fund'][fund_type]:
                    print u"正在运行：", fund_code
                    sql = """
                    SELECT `fund_code`, `cut_off_date`, `net_value_ratio`, `stock_code` FROM `fund_holdings`
                    WHERE `fund_code` = %s
                    ORDER BY `cut_off_date`
                    """ %fund_code
                    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                        fund_holding = pd.read_sql(sql, conn)

                    if fund_holding.empty:
                        continue

                    fund_holding['cut_off_date'] = pd.to_datetime(fund_holding['cut_off_date'])
                    # fund_holding
                    if fund_holding[fund_holding['cut_off_date'].between(date1, date2)].empty and not fund_holding[fund_holding['cut_off_date'] < date1].empty:
                        frist_date = fund_holding[fund_holding['cut_off_date'] < date1]['cut_off_date'].iloc[-1]
                    else:
                        frist_date = fund_holding[fund_holding['cut_off_date'].between(date1, date2)]['cut_off_date'].iloc[0]

                    if date1 < frist_date and not fund_holding[fund_holding['cut_off_date'] < date1].empty:
                        frist_date = fund_holding[fund_holding['cut_off_date'] < date1]['cut_off_date'].iloc[-1]
                    fund_holding = fund_holding[fund_holding['cut_off_date'].between(frist_date, date2)]

                    # 获取股票数据
                    stock_data = {}
                    for stock_code in set(fund_holding['stock_code'].tolist()):
                        sql = 'SELECT `stock_code`, `stock_type` FROM `stock_belonging` WHERE `stock_code`=\'%s\'' % stock_code
                        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                            stock_belonging = pd.read_sql(sql, conn)
                        if stock_belonging.empty:
                            continue
                        stock_type = stock_belonging['stock_type'].iat[0]

                        d = {
                            'prod_code': stock_code + '.' + stock_type,
                            'candle_mode': '0',
                            'data_count': '1000',
                            'get_type': 'offset',
                            'search_direction': '1',
                            'candle_period': '6',
                        }
                        stock_price = api51.connect(user_key, d)[u'data'][u'candle']
                        df = pd.DataFrame(stock_price[stock_code + '.' + stock_type], columns=stock_price['fields'])
                        df['min_time'] = pd.to_datetime(df['min_time'], format="%Y%m%d")
                        df = df.set_index('min_time')
                        stock_data[stock_code] = df['close_px']
                        # print df

                    stock_data = pd.DataFrame(stock_data)
                    stock_data = stock_data.fillna(method='ffill').fillna(0)
                    stock_data = stock_data.reset_index()
                    stock_data = stock_data[stock_data['min_time'].between(date1, date2)]
                    stock_data = stock_data.set_index('min_time')

                    fund_holding_d = {}
                    for date in fund_holding['cut_off_date'].drop_duplicates().tolist():
                        df = fund_holding[fund_holding['cut_off_date'] == date].copy()
                        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda s: re.search(r'\d+\.*\d*', s).group() if re.search(r'\d+\.*\d*', s) else 0).astype(np.float64)
                        df = df.sort_values(['net_value_ratio',], ascending=False)

                        df = df.iloc[:10,:].copy()
                        # print df
                        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda num: num/df['net_value_ratio'].sum())
                        # print df
                        df = df.set_index('stock_code')
                        fund_holding_d[date] = df['net_value_ratio']

                    for date in stock_data.index:
                        ser = stock_data.loc[date,:]
                        # print date
                        # print 'cut_off_date month:', int((date.month-1)/3) * 3 + 1 , type(int(date.month/3) * 3 + 1)
                        cut_off_date = datetime.datetime(year=2017, month=int((date.month-1)/3) * 3 + 1, day=1) - datetime.timedelta(days=1)
                        if cut_off_date in fund_holding_d:
                            cut_off_date_list.append(cut_off_date)
                            stock_data.loc[date, u'拟合净值'] = (ser * fund_holding_d[cut_off_date]).sum()

                    base_price = stock_data[u'拟合净值'].iloc[0]
                    stock_data[u'拟合净值'] = stock_data[u'拟合净值'].apply(lambda num: num / base_price)

                    ser = pd.Series(stock_data[u'拟合净值'], name=fund_code)
                    if not ser.empty:
                        cal_fund_df = cal_fund_df.append(ser)
                        cal_fund_df = cal_fund_df.drop_duplicates()



            # 修正数据
            df = cal_fund_df.T
            # df = pd.read_excel('temp.xlsx')
            df.to_excel('temp.xlsx')
            df = df.sort_index()
            df = df[df.index.isin(date_ser)]
            cut_off_date_list.extend([
                datetime.datetime(year=1950,month=2,day=10),
                datetime.datetime(year=2100,month=1,day=1),
                datetime.datetime(year=2017, month=3, day=31)])
            cut_off_date_list = sorted(list(set(cut_off_date_list)))
            print cut_off_date_list
            base_price = 1
            cal_fund_df = pd.DataFrame([])
            for i in range(len(cut_off_date_list)-1):
                df0 = df[cut_off_date_list[i]+datetime.timedelta(days=1):cut_off_date_list[i+1]].copy()
                if df0.empty:
                    continue
                rate = base_price / df0.iloc[0,:]
                # print 'rate', rate
                # print df0
                df0 = df0 * rate
                base_price = df0.iloc[-1,:]
                cal_fund_df = cal_fund_df.append(df0)
            # print cal_fund_df

            cal_fund_df = cal_fund_df.T
            output_data = output_data.append(cal_fund_df)
            # 计算收益率
            fund_df = pd.DataFrame([])
            for fund_code in cal_fund_df.index:
                sql = """
                SELECT `fund_code`, `value_date`, `accumulative_net_value` FROM `eastmoney_daily_data` WHERE `fund_code` = "%s"
                """ %fund_code
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    fund_value = pd.read_sql(sql, conn)
                func1 = lambda date1, date2: calculation.earnings_cal(fund_code, fund_value, date1=date1, date2=date2,
                                                                     date_col='value_date', value_col='accumulative_net_value', data_type='fund')
                ser1 = func1(date1, date2)

                fund_value = pd.DataFrame(cal_fund_df.loc[fund_code,:])
                fund_value['min_time'] = fund_value.index
                func2 = lambda date1, date2: calculation.earnings_cal(fund_code, fund_value, date1=date1, date2=date2,
                                                                     date_col='min_time', value_col=fund_code, data_type='fund')
                ser2 = func2(date1, date2)

                fund_df.loc[fund_code, u"拟合净值收益率"] = ser2[u'收益率'] if not ser2.empty else None
                fund_df.loc[fund_code, u"实际净值收益率"] = ser1[u'收益率'] if not ser1.empty else None
                fund_df.loc[fund_code, u"收益率比较"] = fund_df.loc[fund_code, u"拟合净值收益率"] - fund_df.loc[fund_code, u"实际净值收益率"]

                fund_df.loc[fund_code, u"拟合净值最大回撤"] = ser2[u'最大回撤'] if not ser2.empty else None
                fund_df.loc[fund_code, u"实际净值最大回撤"] = ser1[u'最大回撤'] if not ser1.empty else None

                fund_df.loc[fund_code, u"拟合净值收益回撤比"] = ser2[u'收益回撤比'] if not ser2.empty else None
                fund_df.loc[fund_code, u"实际净值收益回撤比"] = ser1[u'收益回撤比'] if not ser1.empty else None


            fund_df.index.name = u"基金名称"
            fund_df = fund_df.reset_index()
            table_df = table_df.append(fund_df, ignore_index=True)
            print fund_df
            print table_df
            table_df[u"基金名称"] = table_df[u"基金名称"].replace(fund_name_d)
            table_df.to_excel(u'special_fund.xlsx', index=None)
            output_data.T.to_excel(u'special_fund计算过程.xlsx')


    def main(self):
        if self.method == 'weekly':
            self.weekly_report()
        elif self.method == 'daily':
            self.daily_report()

    def privately_offered_fund(self):
        file_path = u"C:\\Users\\Administrator\\Desktop\\私募基金数据\\"
        title = [u"主策略", u"子策略", u"产品名称", u"管理人", u"成立日期", u"最新日期", u"最新净值",
                 u"成立以来年化收益率", u"成立以来最大回撤", u"成立以来年化收益回撤比",
                 u"2017年收益率", u"2017年回撤", u"2017年年化收益回撤比", u"来源", u"尽调进程", u"备注"]
        output_df = pd.DataFrame([], columns=title)
        res = {}
        for dir_name in os.listdir(file_path):
            data_type = dir_name.split('_')[-1]

            # 计算汇总表
            for file_name in os.listdir(file_path+dir_name):
                df = pd.read_excel(file_path+dir_name+'\\'+file_name)
                df[u'日期'] = pd.to_datetime(df[u'日期']).apply(lambda date:date.date())
                df = df.sort_values([u'日期',]) # , ascending=False)
                newest_date = df[u'日期'].max()

                product_name = os.path.splitext(file_name)[0]

                func = lambda date1, date2: calculation.earnings_cal(file_name, df, date1=date1, date2=date2,
                                                                     date_col=u'日期', value_col=u'累计净值(分红不投资)', data_type='fund')

                ser1 = func(datetime.datetime(year=1990, month=1, day=1), datetime.datetime.now())
                ser2 = func(datetime.datetime(year=2017, month=1, day=1), datetime.datetime(year=2017, month=12, day=31))
                output_df = output_df.append({
                    u"主策略":data_type,
                    u"产品名称": product_name,
                    u"最新日期": newest_date,
                    u"最新净值": df[df[u'日期']==newest_date][u'累计净值(分红不投资)'].iloc[0],
                    u"成立以来年化收益率":ser1.loc[u'年化收益率'],
                    u"成立以来最大回撤":ser1.loc[u'最大回撤'],
                    u"成立以来年化收益回撤比":ser1.loc[u'年化收益回撤比'],
                    u"2017年收益率":ser2.loc[u'年化收益率'],
                    u"2017年回撤":ser2.loc[u'最大回撤'],
                    u"2017年年化收益回撤比":ser2.loc[u'年化收益回撤比']
                }, ignore_index=True)

                detail_d = {
                    u"data":df,
                    u"成立以来":ser1,
                    u"2017年":ser2,
                    u"区间统计":{
                        u"2015年":func(datetime.datetime(year=2015, month=1, day=1), datetime.datetime(year=2015, month=12, day=31)),
                        u"2016年": func(datetime.datetime(year=2016, month=1, day=1), datetime.datetime(year=2016, month=12, day=31)),
                        u"2017年1月": func(datetime.datetime(year=2017, month=1, day=1), datetime.datetime(year=2017, month=2, day=1) - datetime.timedelta(days=1)),
                        u"2017年2月": func(datetime.datetime(year=2017, month=2, day=1), datetime.datetime(year=2017, month=3, day=1) - datetime.timedelta(days=1)),
                        u"2017年3月": func(datetime.datetime(year=2017, month=3, day=1), datetime.datetime(year=2017, month=4, day=1) - datetime.timedelta(days=1)),
                        u"2017年4月": func(datetime.datetime(year=2017, month=4, day=1), datetime.datetime(year=2017, month=5, day=1) - datetime.timedelta(days=1)),
                        u"2017年5月": func(datetime.datetime(year=2017, month=5, day=1), datetime.datetime(year=2017, month=6, day=1) - datetime.timedelta(days=1)),
                        u"2017年6月": func(datetime.datetime(year=2017, month=6, day=1), datetime.datetime(year=2017, month=7, day=1) - datetime.timedelta(days=1)),
                        u"2017年7月": func(datetime.datetime(year=2017, month=7, day=1), datetime.datetime(year=2017, month=8, day=1) - datetime.timedelta(days=1)),
                        u"2017年8月": func(datetime.datetime(year=2017, month=8, day=1), datetime.datetime(year=2017, month=9, day=1) - datetime.timedelta(days=1)),
                        u"2017年9月": func(datetime.datetime(year=2017, month=9, day=1), datetime.datetime(year=2017, month=10, day=1) - datetime.timedelta(days=1)),
                        u"2017年10月": func(datetime.datetime(year=2017, month=10, day=1), datetime.datetime(year=2017, month=11, day=1) - datetime.timedelta(days=1)),
                        u"2017年11月": func(datetime.datetime(year=2017, month=11, day=1), datetime.datetime(year=2017, month=12, day=1) - datetime.timedelta(days=1)),
                        u"2017年12月": func(datetime.datetime(year=2017, month=12, day=1), datetime.datetime(year=2018, month=1, day=1) - datetime.timedelta(days=1)),
                    }
                }
                res[product_name] = detail_d


        output_df = output_df.sort_values([u"主策略", u"2017年年化收益回撤比",], ascending=False)
        for s in [u"成立以来年化收益率", u"成立以来最大回撤", u"2017年收益率", u"2017年回撤"]:
            output_df[s] = output_df[s].apply(lambda num: "{:0.2%}".format(num))

        writer = pd.ExcelWriter(u"私募排排网数据.xlsx")

        output_df.to_excel(writer, u"汇总表", index=None)
        # df0 = pd.DataFrame([])
        # for data_type in output_df[u"主策略"].drop_duplicates():
        #     df0 = df0.append(output_df[output_df[u"主策略"] == data_type].head(10))
        # df0.to_excel(writer, u"汇总表", index=None)

        for data_type in output_df[u"主策略"].drop_duplicates():
            data_list = output_df[output_df[u"主策略"] == data_type].head(10)[u"产品名称"].tolist() # 每个策略的前十个产品
            for product in data_list:
                df = res[product][u"data"]
                # print res[product]

                df = df.drop([u'基金名称',], axis=1)
                df = df.reindex(columns=[u'日期', u'单位净值', u'累计净值(分红再投资)', u'累计净值(分红不投资)', u'净值变动'])

                # 插入统计数据
                func = lambda l:df.append(pd.Series(dict(zip(list(df.columns), l))), ignore_index=True)
                df = func(['',])
                df = func([u"成立以来",u"年化收益率",u"最大回撤",u"收益回撤比"])
                ser = res[product][u"成立以来"]
                if ser.empty:
                    df = func(['', ])
                else:
                    df = func([u"", "{:.2%}".format(ser[u"年化收益率"]) if ser[u"年化收益率"] else None,
                                    "{:.2%}".format(ser[u"最大回撤"]) if ser[u"最大回撤"] else None,
                                    "{:.4f}".format(ser[u"收益回撤比"]) if ser[u"收益回撤比"] else None
                               ])

                df = func(['',])
                df = func([u"2017年", u"收益率", u"最大回撤", u"收益回撤比"])
                ser = res[product][u"2017年"]
                if ser.empty:
                    df = func(['', ])
                else:
                    df = func([u"", "{:.2%}".format(ser[u"收益率"]) if ser[u"收益率"] else None,
                                    "{:.2%}".format(ser[u"最大回撤"]) if ser[u"最大回撤"] else None,
                                    "{:.4f}".format(ser[u"收益回撤比"]) if ser[u"收益回撤比"] else None
                               ])
                df = func(['', ])
                df = func([u"区间统计", u"收益率", u"最大回撤", u"收益回撤比"])

                for key in [u"2015年",u"2016年",u"2017年1月",u"2017年2月",u"2017年3月",u"2017年4月",u"2017年5月",u"2017年6月",u"2017年7月",
                           u"2017年8月",u"2017年9月",u"2017年10月",u"2017年11月",u"2017年12月"]:
                    ser = res[product][u"区间统计"][key]
                    if ser.empty:
                        df = func([key,])
                    else:
                        # print ser[u"收益回撤比"]
                        # print type(ser[u"收益回撤比"])
                        df = func([key, "{:.2%}".format(ser[u"收益率"]) if ser[u"收益率"] else None,
                                        "{:.2%}".format(ser[u"最大回撤"]) if ser[u"最大回撤"] else None,
                                        "{:.4f}".format(ser[u"收益回撤比"]) if ser[u"收益回撤比"] else None
                                   ])



                df.to_excel(writer, "%s_%s" %(data_type, product), index=None)

        writer.save()



if __name__ == '__main__':
    if len(sys.argv)>1:
        net_value_cal_display = net_value_cal_display(method=sys.argv[1])
    else:
        net_value_cal_display = net_value_cal_display()
    # net_value_cal_display = net_value_cal_display.weekly_report()
    # net_value_cal_display.date_range_display()
    # net_value_cal_display.special_fund()

    net_value_cal_display.privately_offered_fund()
# http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?type=HSGTHDSTA&token=70f12f2f4f091e459a279469fe49eca5&filter=(SCODE=%27000063%27)&st=HDDATE&sr=-1&p=1&ps=50&js=var%20ANuhNwTP={pages:(tp),data:(x)}&rt=50437829
# http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?type=HSGTHDSTA&token=1942f5da9b46b069953c873404aad4b5&filter=(SCODE=000063)&st=HDDATE&sr=-1&p=1&ps=1000