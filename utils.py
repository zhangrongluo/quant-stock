import os
import re
import sqlite3
import pandas as pd
import datetime
import time
import random
from typing import List, Tuple, Dict, Union
import tushare as ts
import swindustry as sw
from path import (INDICATOR_ROE_FROM_1991, CURVE_SQLITE3, CURVE_TABLE, ROE_TABLE, TRADE_RECORD_PATH)

def calculate_MOS_7_from_2006(code: str, date: str) -> float:
    """
    使用INDICATOR_ROE_FROM_1991等数据库计算7年MOS值,需要获取7年roe平均值 10年期国债收益率和PB.
    7年roe值取自于INDICATOR_ROE_FROM_1991数据库,10年期国债收益率取自于curve数据库,pb值取自CSV文件.
    date参数指定国债收益率和PB的日期,为yyyy-mm-dd型字符串.
    curve数据库自2006-03-01开始,故7年roe序列最早为2006年-2012年序列.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2019-01-01'
    :return: 返回MOS_7值
    NOTE:如果参数date的月份数在1-4月,ROE年份数取date参数年份-2前推7年,否者取date参数年份-1前推7年.
    """
    # 检查参数
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not date_regex.match(date):
        raise ValueError('参数date应为yyyy-mm-dd型字符串')
    this_year = datetime.datetime.now().year
    year_month_list = date.split('-')
    if not this_year >= int(year_month_list[0]) >= 2006:
        raise ValueError('参数date的年份应在2006至当前年份之间')
    
    # 获取7年平均ROE值
    end_year = int(year_month_list[0]) - 1 if int(year_month_list[1]) >= 5 else int(year_month_list[0]) - 2
    average_roe_7 = 0.00
    stock_code = f'{code}.SH' if code.startswith('6') else f'{code}.SZ'
    date_range = [f'Y{year}' for year in range(end_year, end_year-7, -1)]
    suffix = """stockcode, stockname, stockclass, """
    for index, item in enumerate(date_range):
        if index == len(date_range)-1:
            suffix += item
        else:
            suffix += f"{item}, "
    con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
    with con:
        sql = f"SELECT {suffix} FROM '{ROE_TABLE}' WHERE stockcode=?"
        tmp = con.execute(sql, (stock_code, )).fetchone()
        num_zero = list(tmp).count(0.00)
        if num_zero == 7:
            raise ValueError(f'{stock_code}7年roe值均为0.00')
        try:
            average_roe_7 = sum(list(tmp)[3:10])/(7-num_zero)  # 剔除0
        except:
            raise ValueError(f'未能获取{stock_code}7年roe均值')

    row = find_closest_row_in_curve_table(date=date)  # 获取date参数指定的日期及附近的10年期国债收益率
    try:
        yield_value = row['value1'].values[0]
    except:
        raise ValueError(f'未能获取{date}10年期国债收益率')
    row = find_closest_row_in_trade_record(code=code, date=date)  # 获取date参数指定的日期及附近的PB值
    try:
        pb = row['pb'].values[0]
    except:
        raise ValueError(f'未能获取{stock_code}PB值')
    inner_pb = average_roe_7/yield_value  # 计算mos_7
    mos_7 = 1 -pb/inner_pb
    return round(mos_7, 4)

def calculate_index_rising_value(code: str, start_date: str, end_date: str) -> float:
    """
    计算000300 399006 000905指数期间涨幅
    :param code: 指数代码, 例如: '000300'
    :param start_date: 开始日期, 例如: '2019-01-01'
    :param end_date: 结束日期, 例如: '2019-01-01'
    :return: 指数的涨幅
    NOTE:
    调用index_daily时日期格式如果是yyyy-mm-dd,则返回的结果是错误的,必须转换为yyyymmdd格式.
    而calculate_stock_rising_value函数则没有这个问题.
    为保持参数输入一致性,统一在两个函数转换日期格式.
    """
    if code not in ['000300', '399006', '000905']:
        raise ValueError('请检查指数代码是否正确(000300, 399006, 000905)')
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 日期格式转换
    if date_regex.match(start_date):
        start_date = start_date.replace('-', '')
    if date_regex.match(end_date):
        end_date = end_date.replace('-', '')
    full_code = f'{code}.SH' if code.startswith('000') else f'{code}.SZ'
    pro = ts.pro_api()
    df = pro.index_daily(ts_code=full_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        rate = 0.00
    else:
        df['rate'] = df['pct_chg'].apply(lambda x: x/100 + 1)
        rate = df['rate'].prod() - 1
    return rate

def calculate_stock_rising_value(code: str, start_date: str, end_date: str) -> float:
    """
    计算单只股票期间涨幅涨幅
    :param code: 股票代码, 例如: '600000' or '000001'
    :param start_date: 开始日期, 例如: '2019-01-01'
    :param end_date: 结束日期, 例如: '2019-01-01'
    :return: 组合的涨幅
    """
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 日期格式转换
    if date_regex.match(start_date):
        start_date = start_date.replace('-', '')
    if date_regex.match(end_date):
        end_date = end_date.replace('-', '')
    full_code = f'{code}.SH' if code.startswith('6') else f'{code}.SZ'
    df = ts.pro_bar(ts_code=full_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        rate = 0.00
    else:
        df['rate'] = df['pct_chg'].apply(lambda x: x/100 + 1)
        rate = df['rate'].prod() - 1
    return rate

def calculate_portfolio_rising_value(code_list: List[str], start_date: str, end_date: str) -> float:
    """
    计算股票组合的涨幅,采用等资金权重模式
    :param code_list: 股票代码列表, 例如: ['600000', '000001']
    :param start_date: 开始日期, 例如: '2019-01-01'
    :param end_date: 结束日期, 例如: '2019-01-01'
    :return: 组合的涨幅
    """
    if not code_list:
        return 0.00
    rate = 0.00
    for code in code_list:
        tmp = calculate_stock_rising_value(code, start_date, end_date)
        rate += tmp
    return rate/len(code_list)

def find_closest_row_in_trade_record(code: str, date: str):
    """
    在CSV中查找指定日期所在或者最接近的行.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2019-01-01'
    :return: 查找到的行,如果没有精确匹配,则返回最接近的行
    """
    sw_industry = sw.get_name_and_class_by_code(code)[1]
    csv_file = os.path.join(TRADE_RECORD_PATH, sw_industry, f"{code}.csv")
    df = pd.read_csv(csv_file)
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    date0 = datetime.datetime.strptime(date, '%Y-%m-%d')
    match_row = df.loc[df['trade_date'] == date0]  # 精确匹配        

    if match_row is None or match_row.empty:  # 查找最接近的行
        df['diff'] = df['trade_date'].apply(lambda x: abs((x - date0).days))
        df = df.sort_values(by='diff', ascending=True)
        match_row = df.iloc[0:1, :]
    return match_row

def find_closest_row_in_curve_table(date: str):
    """
    在curve数据库中查找指定日期所在或者最接近的行.
    :param date: 日期, 例如: '2019-01-01'
    :return: 查找到的行,如果没有精确匹配,则返回最接近的行
    """
    con = sqlite3.connect(CURVE_SQLITE3)
    with con:
        df = pd.read_sql(f"SELECT * FROM '{CURVE_TABLE}'", con)
    df['date1'] = pd.to_datetime(df['date1'], format='%Y-%m-%d')
    date0 = datetime.datetime.strptime(date, '%Y-%m-%d')
    match_row = df.loc[df['date1'] == date0]  # 精确匹配

    if match_row is None or match_row.empty:  # 查找最接近的行
        df['diff'] = df['date1'].apply(lambda x: abs((x - date0).days))
        df = df.sort_values(by='diff', ascending=True)
        match_row = df.iloc[0:1, :]
    return match_row

def get_indicator_in_trade_record(code: str, date: str, fields: str) -> float:
    """
    获取指定股票指定日期的指定字段值
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2019-01-01'
    :param fileds: 字段名称, 可选范围为: ['pe_ttm', 'pb', 'ps_ttm', 'dv_ttm', 'total_mv', 'circ_mv']
    :return: 指定字段的值
    """
    if fields not in ['pe_ttm', 'pb', 'ps_ttm', 'dv_ttm', 'total_mv', 'circ_mv']:
        raise ValueError('参数fields应为pe_ttm, pb, ps_ttm, dv_ttm, total_mv, circ_mv之一')
    row = find_closest_row_in_trade_record(code, date)
    return row[fields].values[0]

if __name__ == "__main__":
    res = calculate_stock_rising_value('000333', '2022-06-01', '2023-06-01')
    print(f"2022-06-01至2023-06-01,000333涨幅为{res:.2%}")
    res = calculate_index_rising_value('000300', '2022-06-01', '2023-06-01')
    print(f"2023-06-01至2024-06-01,000300涨幅为{res:.2%}")