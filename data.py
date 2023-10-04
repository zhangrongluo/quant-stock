"""
以get_开头的函数为获取原始数据的函数
以create_开头的函数为系统初始化创建数据表的函数
以update_开头的函数为运行过程中更新数据表的函数
Tushare下载速度比较慢
"""
import os
import time
import datetime
import sqlite3
import requests
from io import StringIO
from functools import partial
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import tushare as ts
import swindustry as sw
from path import TRADE_RECORD_PATH, INDICATOR_ROE_FROM_1991, CURVE_SQLITE3, ROE_TABLE, CURVE_TABLE

def get_IPO_date(code: str) -> str:
    """
    使用tushare获取股票上市日期
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: 股票上市日期, 例如: '1991-04-03'
    """
    ipo_data = "1991-01-01"
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    pro = ts.pro_api()
    df = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    try:
        ipo_data = df[df['ts_code'] == full_code]['list_date'].values[0]
        ipo_data = IPO_date[0:4] + '-' + IPO_date[4:6] + '-' + IPO_date[6:8]
    except:
        ...
    return ipo_data

def get_whole_trade_record_data(code: str) -> pd.DataFrame:
    """
    使用tushare获取股票历史交易记录文件,从上市日至今.包括ts_code,trade_date,pe_ttm,pb,ps_ttm,dv_ttm,total_mv,circ_mv列
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: 股票历史交易记录文件
    """
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    pro = ts.pro_api()
    result = pro.daily_basic(ts_code=full_code,fields='ts_code,trade_date,pe_ttm,pb,ps_ttm,dv_ttm,total_mv,circ_mv')
    tmp = sw.get_name_and_class_by_code(code=code)  # 插入公司简称和行业分类
    result.insert(2, 'company', tmp[0])
    result.insert(3, 'industry', tmp[1])
    return result

def get_ROE_indicators_from_Tushare(code: str) -> Dict:
    """
    获取公司1991至上年度年度ROE值,用于初始化indicator_roe_from_1991.sqlite3文件
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: 从1991年上年的字典序列,键值对为年度:ROE
    """
    start_year = '19911231'
    end_year = str(time.localtime().tm_year-1)+'1231'
    periods = pd.date_range(start=start_year, end=end_year, freq='y').strftime("%Y%m%d")
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'

    result = {}  # 定义返回值
    pro = ts.pro_api()
    for period in periods:
        res = pro.fina_indicator(fields="roe", ts_code=full_code, period=period)
        if res is None:
            result[period] = None
        elif isinstance(res, pd.DataFrame):
            if res.empty:
                result[period] = None
            else:
                result[period] = res.loc[0, 'roe']
    result = dict(sorted(result.items(), key=lambda x: x[0], reverse=True))  # 按照键降序排序
    return result

def get_ROE_indicators_from_xueqiu(code: str, count: int, type: str):
    """
    从雪球网站上获取的财务指标信息
    :param code: 股票代码, 例如: 600600 或 000001
    :param count: 财务指标的数量, 例如: 30
    :param type: 财务指标类型Q1、Q2、Q3、Q4、all,分别代表第一二三四季度及全部期间
    :return: 返回一个字典, 键为年度名称, 值为年度ROE值
    """
    url = 'https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json'
    params = {
        'symbol': f'sh{code}' if code.startswith('6') else f'sz{code}',
        'type': f'{type}',
        'is_detail': 'true',
        'count': f'{count}',
        'timestamp': ''
    }
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62',
    }
    session = requests.Session()
    session.get(url='https://xueqiu.com/', headers=headers)
    response = session.get(url=url, headers=headers, params=params).json()  # 获取数据

    result = {}  # 定义返回值
    for indicator in response['data']['list']:
        result[indicator['report_name']] = indicator['avg_roe'][0]
    return result

def get_yield_data_from_china_bond(date_str: str) -> float:
    """ 
    从chinabond中债信息网获取指定日期10年期国债到期收益率表格
    :param date_str: 日期字符串,例如: '2021-10-29'
    :return: 10年期国债到期收益率
    """
    curve_value = 0
    url = "https://yield.chinabond.com.cn/cbweb-cbrc-web/cbrc/queryGjqxInfo"
    data = {
        'workTime': date_str,
        'locale': 'cn_ZH',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15'}   
    time.sleep(0.05)
    response = requests.post(url=url, headers=headers, data=data)
    try:
        df_list = pd.read_html(io=StringIO(response.text))
        curve_value = float(df_list[0].loc[0, '10年'])
    except KeyError:
        ...
    print(f"{date_str} 10年期国债到期收益率为{curve_value}." + '\r', end='', flush=True)
    return curve_value

def create_curve_value_table(days: int):
    """
    创建10年期国债到期收益率插入curve表中,数据从2006-03-01开始.
    :param days: 从昨天起向前推days天数
    :return: None
    """
    yesterday = datetime.date.today() + datetime.timedelta(days=-1)
    begin = yesterday + datetime.timedelta(days=-days)
    date_list = pd.date_range(begin, yesterday)
    date_str = [str(date)[0:10] for date in date_list]  # 生成日期序列
    with ThreadPoolExecutor() as pool:
        value_list = pool.map(get_yield_data_from_china_bond, date_str)

    con = sqlite3.connect(CURVE_SQLITE3)
    with con:
        sql = f"""
            CREATE TABLE IF NOT EXISTS '{CURVE_TABLE}' (
            date1 TEXT PRIMARY KEY NOT NULL,
            value1 REAL DEFAULT 0
        )"""
        con.executescript(sql)  # 创建curve表格

        sql = f"""select * from '{CURVE_TABLE}' """
        df = pd.read_sql_query(sql, con)
        for date, value in zip(date_str, value_list):  # 在第一行插入日期和值，遇重复日期，略过
            df.loc[-1] = [date, value]
            df.index += 1
            df = df.sort_index()

        df = df[df['value1'] != 0]  # 去除value1为0的行
        df.drop_duplicates(subset=['date1'], keep='last', inplace=True)
        df.to_sql(name=CURVE_TABLE, con=con, index=False, if_exists='replace')

def create_trade_record_csv_table(code: str, rm_empty_rows: bool = False) -> None:
    """
    创建股票历史交易记录文件,从上市日至今.保存在TRADE_RECORD_PATH目录下.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param rm_empty_rows: 是否删除空行
    :return: None
    """
    df = get_whole_trade_record_data(code=code)
    if rm_empty_rows:
        df = df.dropna(axis=0, how='any')
    df.sort_values(by='trade_date', ascending=False, inplace=True)  # 按日期降序排列
    df['trade_date'] = df['trade_date'].astype('object')  # 将trade_date列转换为object类型
    
    # 创建保存目录
    swindustry = sw.get_name_and_class_by_code(code=code)[1]
    dest_path = os.path.join(TRADE_RECORD_PATH, swindustry)
    if not os.path.exists(dest_path):
        os.mkdir(dest_path)
    file_name = code + '.csv'
    file_path = os.path.join(TRADE_RECORD_PATH, swindustry, file_name)
    df.to_csv(file_path, index=False)
    print(f'{code}历史交易记录文件下载成功,保存在{dest_path}目录下.'+ '\r', end='', flush=True)

def create_specific_class_trade_record_csv_table(stock_class: str, rm_empty_rows: bool = False):
    """
    创建stock_class指定的行业下所有股票的历史交易记录文件,从上市日至今.保存在TRADE_RECORD_PATH目录下.
    :param stock_class: 行业分类
    :param rm_empty_rows: 是否删除空行
    :return: None
    """
    stocks = [item[0][0:6] for item in sw.get_stocks_of_specific_class(stock_class=stock_class)]
    part_func = partial(create_trade_record_csv_table, rm_empty_rows=rm_empty_rows)
    with ThreadPoolExecutor() as pool:
        pool.map(part_func, stocks)

def create_all_stocks_trade_record_csv_table(rm_empty_rows: bool = False):
    """
    创建所有股票的历史交易记录文件,从上市日至今.保存在TRADE_RECORD_PATH目录下.
    :param rm_empty_rows: 是否删除空行
    :return: None
    """
    stocks = [item[0][0:6] for item in sw.get_all_stocks()]
    part_func = partial(create_trade_record_csv_table, rm_empty_rows=rm_empty_rows)
    with ThreadPoolExecutor() as pool:
        pool.map(part_func, stocks)

def create_ROE_indicators_table_from_1991(code: str):
    """ 
    创建1991至上年年度ROE至indicator_roe_from_1991.sqlite3中
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: None
    NOTE: 被注释的第一行为使用Tushare接口的代码,速度仅为Xueqiu接口20%
    """
    # roe_dict = get_ROE_indicators_from_Tushare(code=code)  # 使用tushare接口
    start_year = '19911231'
    end_year = str(time.localtime().tm_year-1)+'1231'
    count = int(end_year[0:4]) - 1991 + 1
    roe_dict = get_ROE_indicators_from_xueqiu(code=code, count=count, type='Q4')
    periods = pd.date_range(start=start_year, end=end_year, freq='y').strftime("%Y%年报")  # 补齐缺失的年度
    for period in periods:
        if period not in roe_dict.keys():
            roe_dict[period] = None
    for key in list(roe_dict.keys()):  # 删除1991以前的键值对
        if int(key[0:4]) < 1991:
            del roe_dict[key]
    roe_dict = dict(sorted(roe_dict.items(), key=lambda x: x[0], reverse=True))  # 按照键降序排序
    con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
    with con:
        full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        fields = list(roe_dict.keys())

        sql = f"""CREATE TABLE IF NOT EXISTS '{ROE_TABLE}' (""" +  "\n" + \
        "stockcode TEXT PRIMARY KEY," + "\n" + "stockname TEXT," + "\n" + "stockclass TEXT," + "\n" 
        for index, item in enumerate(fields):
            if index == len(fields) - 1:  
                sql += f"Y{item[0:4]} REAL" + "\n"
            else:  
                sql += f"Y{item[0:4]} REAL," + "\n"
        sql += ")"
        con.execute(sql)  # 创建表

        tmp = sw.get_name_and_class_by_code(code=code)
        insert_value =[full_code] + tmp + list(roe_dict.values())
        sql = f"""INSERT OR IGNORE INTO '{ROE_TABLE}' VALUES (""" + "\n" 
        for index, item in enumerate(insert_value):
            if index == len(insert_value) - 1:
                sql += "?" + "\n"
            else:
                sql += "?," + "\n"
        sql += ")"
        con.execute(sql, insert_value)  # 插入数据
    print(f"{full_code}历史ROE数据下载成功." + '\r', end='', flush=True)

def update_ROE_indicators_table_from_1991(code: str):
    """ 
    更新最新的年度ROE至INDICATOR_ROE_FROM_1991数据库.
    :params code: 股票代码, 例如: '600000' or '000001'
    :return: None
    NOTE:当然也可以每年5月份之后重新执行一遍create_ROE_indicators_table_from_1991函数,只是比较耗时.
    """
    # 根据日历时间获得需要插入的最新字段名,按照以下规则确定(假设现年份为2023年):
    today = datetime.date.today()
    if today.month in [1, 2, 3, 4]:
        last_filed = 'Y'+str(today.year-2)
    else:
        last_filed = 'Y'+str(today.year-1)

    # 获取表字段中最新的时间
    con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
    with con:
        sql = f""" SELECT * FROM '{ROE_TABLE}' """
        df = pd.read_sql_query(sql, con)

        # 如果表中未包含最近一期的年度数据,则插入新列
        contain_last_report = False if (last_filed not in df.columns) else True
        if not contain_last_report:
            df.insert(loc=3, column=last_filed, value=0.00)  # 在第四列插入
            df.to_sql(name=ROE_TABLE, con=con, if_exists='replace', index=False)

        # 使用Tushare下载最新ROE数据
        full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
        period = last_filed[1:5] + '1231'
        pro = ts.pro_api()
        tmp = pro.fina_indicator(ts_code=full_code, period=period, fields='roe')
        if tmp is None:
            last_roe = 0.00
        elif isinstance(tmp, pd.DataFrame):
            if tmp.empty:
                last_roe = 0.00
            else:
                last_roe = tmp.loc[0, 'roe']
        sql = f""" UPDATE '{ROE_TABLE}' SET {last_filed}=? WHERE stockcode=? """
        try:
            con.execute(sql, (last_roe, stock_code))
        except sqlite3.IntegrityError:
            ...

def update_trade_record_csv(code: str):
    """
    更新股票历史交易记录文件至今日最新数据.
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: None
    NOTE:当然也可以重新一遍create_trade_record_csv_table函数,但是太耗时了.
    """
    # 获取日期
    csv_file = os.path.join(TRADE_RECORD_PATH, sw.get_name_and_class_by_code(code=code)[1], code+'.csv')
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"未发现{csv_file}历史交易记录文件,请检查.")
    df_old = pd.read_csv(csv_file)
    if df_old.empty:
        raise Exception(f"{csv_file}文件内容为空,请先使用create_...函数创建表格.")
    last_date = df_old.loc[0, 'trade_date']  # int64型
    last_date = datetime.datetime.strptime(str(last_date), '%Y%m%d')
    start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y%m%d')  # 获取last_date第二天的日期, str型
    end_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    
    # 获取数据
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    pro = ts.pro_api()
    df1 = pro.daily_basic(ts_code=full_code, start_date=start_date, end_date=end_date, 
    fields=["ts_code","trade_date","pe_ttm","pb","ps_ttm","circ_mv","dv_ttm","total_mv"])
    if df1.empty:
        print(f"{full_code}无可更新数据." + '\r', end='', flush=True)
        return  # 如果df1为空,则无可更新数据,直接返回
    tmp = sw.get_name_and_class_by_code(code=code)  # 插入公司简称和行业分类
    df1.insert(2, 'company', tmp[0])
    df1.insert(3, 'industry', tmp[1])
    df_new = pd.concat([df1, df_old], axis=0)  # 数据合并
    df_new['trade_date'] = df_new['trade_date'].astype('object')
    df_new.to_csv(csv_file, index=False)  # 保存文件
    print(f"{full_code}历史交易记录文件更新成功." + '\r', end='', flush=True)

def update_curve_value_table():
    """
    刷新国债收率表至昨日数据, 须每日定期执行, 避免在计算MOS时出错
    :return: None
    """
    con = sqlite3.connect(CURVE_SQLITE3)
    with con:
        sql = f""" SELECT date1, value1 FROM '{CURVE_TABLE}' ORDER BY date1 DESC """
        latest_value = con.execute(sql).fetchone()

    yesterday = datetime.date.today() + datetime.timedelta(days=-1)
    latest_day = datetime.datetime.strptime(latest_value[0], '%Y-%m-%d').date()
    delta_day = (yesterday - latest_day).days  # 计算日期差

    if delta_day > 0:  # 如果日期差大于0,则更新数据
        create_curve_value_table(days=delta_day)


if __name__ == '__main__':
    while True:
        print('-------------------------操作提示-------------------------')
        print('Create-Trade-CSV      Create-Curve       Create-Roe-Table')
        print('Update-Trade-CSV      Update-Curve       Update-ROE-Table')
        print('Quit                                                     ')
        print('---------------------------------------------------------')
        msg = input('>>>> 请选择操作提示 >>>>  ')
        if msg.upper()  == 'QUIT':
            break
        elif msg.upper() == 'CREATE-TRADE-CSV':
            print('正在创建trade-record csv文件,请稍等...')
            create_all_stocks_trade_record_csv_table()
            print('trade-record csv文件创建成功.'+ ''*20)
        elif msg.upper() == 'CREATE-CURVE':
            print('正在创建curve表格,请稍等...')
            begin = datetime.date(2006, 3, 1)
            yesterday = datetime.date.today() + datetime.timedelta(days=-1)
            days = (yesterday - begin).days
            create_curve_value_table(days=days)
            print('curve表格创建成功.'+ ''*20)
        elif msg.upper() == 'CREATE-ROE-TABLE':
            print('正在创建indicators表格,请稍等...')
            stocks = [item[0][0:6] for item in sw.get_all_stocks()]
            with ThreadPoolExecutor() as pool:
                pool.map(create_ROE_indicators_table_from_1991, stocks)
            print('indicators表格创建成功.'+ ''*20)
        elif msg.upper() == 'UPDATE-TRADE-CSV':
            print('正在更新trade-record csv文件,请稍等...')
            stocks = [item[0][0:6] for item in sw.get_all_stocks()]
            with ThreadPoolExecutor() as pool:
                pool.map(update_trade_record_csv, stocks)
            print('trade-record csv文件更新成功.'+ ''*20)
        elif msg.upper() == 'UPDATE-CURVE':
            print('正在更新curve表格,请稍等...')
            update_curve_value_table()
            print('curve表格更新成功.'+ ''*20)
        elif msg.upper() == 'UPDATE-ROE-TABLE':
            print('正在更新indicators表格,请稍等...')
            stocks = [item[0][0:6] for item in sw.get_all_stocks()]
            with ThreadPoolExecutor() as pool:
                pool.map(update_ROE_indicators_table_from_1991, stocks)
            print('indicators表格更新成功.'+ ''*20)
        else:
            continue
