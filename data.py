"""
以get_开头的函数为获取原始数据的函数
以create_开头的函数为系统初始化创建数据表的函数
以update_开头的函数为运行过程中更新数据表的函数
Tushare下载速度比较慢
"""
import os
import re
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
import tsswindustry as sw
from path import (TRADE_RECORD_PATH, INDICATOR_ROE_FROM_1991, CURVE_SQLITE3, ROE_TABLE, 
                CURVE_TABLE, INDEX_VALUE, TEST_CONDITION_SQLITE3, TEST_CONDITION_PATH)

def get_IPO_date(code: str) -> str:
    """
    使用tushare获取股票上市日期
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: 股票上市日期, 例如: '1991-04-03'
    """
    ipo_data = "1991-01-01"
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    pro = ts.pro_api()
    df = pro.query(
        'stock_basic', exchange='', list_status='L', 
        fields='ts_code,symbol,name,area,industry,list_date'
        )
    try:
        ipo_data = df[df['ts_code'] == full_code]['list_date'].values[0]
        ipo_data = ipo_data[0:4] + '-' + ipo_data[4:6] + '-' + ipo_data[6:8]
    except:
        ...
    return ipo_data

def get_whole_trade_record_data(code: str) -> pd.DataFrame:
    """
    使用tushare获取股票历史交易记录文件,从上市日至今.包括ts_code,trade_date,close,pe_ttm,
    pb,ps_ttm,dv_ttm,total_mv,circ_mv,pct_chg和dv_est
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: 股票历史交易记录文件
    """
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    pro = ts.pro_api()
    fields = ["ts_code", "trade_date", "close", "pe_ttm", "pb", "ps_ttm",
        "dv_ratio", "dv_ttm", "turnover_rate", "turnover_rate_f", "volume_ratio",
        "total_share", "float_share", "free_share", "total_mv", "circ_mv", "pe", "ps"]
    result = pro.daily_basic(ts_code=full_code, fields=fields)
    tmp = sw.get_name_and_class_by_code(code=code)  # 插入公司简称和行业分类
    result.insert(2, 'company', tmp[0])
    result.insert(3, 'industry', tmp[1])
    # 获取历史涨幅，以trade_date为索引，合并到result中
    tmp = pro.daily(ts_code=full_code, fields='trade_date,pct_chg')
    tmp = tmp.set_index('trade_date')
    result = result.set_index('trade_date')
    result = result.join(tmp, how='inner')
    result.reset_index(inplace=True)
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

def check_stockcodes_integrity(
    trade_record_path = TRADE_RECORD_PATH,
    roe_sqlite = INDICATOR_ROE_FROM_1991,
    roe_table = ROE_TABLE
) -> Dict:
    """
    检查股票代码的完整性,以当前申万行业分类下全部股票为基准, 检查TRADE_RECORD_PATH目录下
    每个行业目录下股票的交易记录文件是否完整, 检查ROE_TABLE数据表中的股票代码是否完整.
    :param trade_record_path: str, TRADE_RECORD_PATH目录
    :param roe_sqlite: str, INDICATOR_ROE_FROM_1991文件
    :param roe_table: str, ROE_TABLE表名
    :return: Dict,返回缺失的股票和文件代码信息
    NOTE:
    返回一个字典,包含三个键值对:
    1. trade_record_path: {"农林牧渔":[...],...}, 表示每个行业目录下缺失的股票代码,
    2. roe_table: [...], 表示roe_table数据表中不存在而申万行业中存在的股票代码,需要添加.
    3. to_remove: [...], 表示roe_table数据表中存在而申万行业中不存在的股票代码,需要删除.
    """
    result = {}  # 返回结果
    result['trade_record_path'] = {}
    sw_classes = sw.get_stock_classes()
    # 遍历sw_classes,获取每个行业目录下的全部股票代码
    # 获取trade_record_path对应行业目录下的全部文件名称(以股票代码.csv命名)
    # 比较两者的差异,储存缺失的股票代码
    for stock_class in sw_classes:
        dest_dir = os.path.join(trade_record_path, stock_class)
        if not os.path.exists(dest_dir):
            os.mkdir(dest_dir)
        trade_files = os.listdir(dest_dir)
        trade_codes = [f.split('.')[0] for f in trade_files if f.endswith(".csv")]
        dest_stocks = sw.get_stocks_of_specific_class(stock_class=stock_class)
        dest_codes = [item[0][0:6] for item in dest_stocks]
        diff_codes = [code for code in dest_codes if code not in trade_codes]
        if diff_codes:
            result['trade_record_path'][stock_class] = diff_codes
    sw_stocks = sw.get_all_stocks()
    sw_codes = [item[0][0:6] for item in sw_stocks]  # 不含后缀的全部股票代码
    con = sqlite3.connect(roe_sqlite)  # 获取roe_table数据表中的全部股票代码
    with con:
        sql = f""" SELECT stockcode FROM "{roe_table}" """
        df = pd.read_sql(sql, con)
        roe_codes = df['stockcode'].values.tolist()
        roe_codes = [code[0:6] for code in roe_codes]  # 不含后缀的全部股票代码
    result['roe_table'] = [code for code in sw_codes if code not in roe_codes]
    result['to_remove'] = [code for code in roe_codes if code not in sw_codes]
    return result

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

def create_index_indicator_table(index: str='000300'):
    """
    创建指数估值数据库,用以计算指数MOS_7
    :param index: '000300' or '000905' or '399006'
    NOTE:
    数据期间从20040101开始至今日(tushare接口限制)
    数据库名称为INDEX_VALUE,表名为000300.SH, 000905.SH, 399006.SH
    指标包括ts_code,trade_date,pb,pe,turnover_rate,pe_ttm,turnover_rate_f,pct_chg,close,vol,amount
    NOTE:
    此外再加一个roe预估数=pb/pe
    """
    if index not in ['000300', '000905', '399006']:
        raise ValueError('请检查指数代码是否正确[000300, 000905, 399006]')
    full_code = index + '.SH' if index.startswith('000') else index + '.SZ'
    con = sqlite3.connect(INDEX_VALUE)
    with con:
        sql = f"""
            DROP TABLE IF EXISTS '{full_code}'
        """
        con.executescript(sql)  # 首先删除表格清空

        sql = f"""
            CREATE TABLE IF NOT EXISTS '{full_code}' (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            pb REAL DEFAULT 0,
            pe REAL DEFAULT 0,
            pe_ttm REAL DEFAULT 0,
            turnover_rate REAL DEFAULT 0,
            turnover_rate_f REAL DEFAULT 0,
            roe_est REAL DEFAULT 0,
            pct_chg REAL DEFAULT 0
        )"""
        con.executescript(sql)  # 创建表格
        # 每次下载3000条数据
        pro = ts.pro_api()
        df_list = []
        today = pd.Timestamp.today()
        for i in range(100):
            if i == 0:
                start_date = (today - pd.Timedelta(days=3000)).strftime("%Y%m%d")
                end_date = today.strftime("%Y%m%d")
            else:
                start_date = (today - pd.Timedelta(days=3000*(i+1))).strftime("%Y%m%d")
                end_date = (today - pd.Timedelta(days=3000*i)).strftime("%Y%m%d")
            df = pro.index_dailybasic(
                **{"ts_code": full_code,"start_date": start_date, "end_date": end_date,},
                fields='ts_code,trade_date,pb,pe,pe_ttm,turnover_rate,turnover_rate_f'
            )
            if not df.empty:
                df_list.append(df)
            else:
                break
            time.sleep(1)
        df = pd.concat(df_list)
        df = df.sort_values(by='trade_date', ascending=False)
        try:
            df['roe_est'] = (df['pb'] / df['pe']).apply(lambda x: round(x, 4))  # 保留四位小数
        except ZeroDivisionError:
            df['roe_est'] = 0.0000
        df_1 = pro.index_daily(
            ts_code=full_code, fields="trade_date, pct_chg, close, vol, amount"
        )
        df_1 = df_1.sort_values(by='trade_date', ascending=False)
        df = df.set_index('trade_date')
        df_1 = df_1.set_index('trade_date')
        df = df.join(df_1, how='inner')
        df.reset_index(inplace=True)
        df = df.sort_values(by='trade_date', ascending=False)
        df.to_sql(name=full_code, con=con, index=False, if_exists='replace')

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
    print(f'{code}历史交易记录文件下载成功,保存在{swindustry}目录下.'+ ' '*50 + '\r', end=" ", flush=True)

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
    NOTE: 
    被注释的第一行为使用Tushare接口的代码,速度仅为Xueqiu接口20%
    """
    roe_dict = get_ROE_indicators_from_Tushare(code=code)  # 使用tushare接口
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

def invert_trade_record_to_win_stock_format(code: str, des_root_path: str):
    """
    将股票历史交易记录文件转换为WinStock格式,保存在des_path目录下.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param des_root_path: 转换后的文件保存根目录
    :return: None
    NOTE:
    转换后,使用win-stock系统data.init_trade_record_form_IPO函数,增加PC列完成最后的转换.
    """
    industry = sw.get_name_and_class_by_code(code=code)[1]
    src_file = os.path.join(TRADE_RECORD_PATH, industry, code+'.csv')
    des_file = os.path.join(des_root_path, industry, code+'.csv')
    if not os.path.exists(src_file):
        raise FileNotFoundError(f"未发现{src_file}历史交易记录文件,请检查.")
    if not os.path.exists(os.path.dirname(des_file)):
        os.makedirs(os.path.dirname(des_file))

    df = pd.read_csv(src_file)
    df.columns = ['股票代码', '日期', '名称', '行业', 'PE', 'PB', 'PS', 'DIVIDEND', '总市值', '流通市值']
    df['日期'] = df['日期'].astype('object')
    df['日期'] = df['日期'].apply(lambda x: str(x)[0:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8])
    df['股票代码'] = df['股票代码'].apply(lambda x: "'"+str(x)[0:6])
    df = df[['日期', '股票代码', '名称', '总市值', 'PB', 'PE', 'PS', 'DIVIDEND']]
    df['总市值'] = df['总市值'].fillna(0)
    df['PB'] = df['PB'].fillna(0)
    df['PE'] = df['PE'].fillna(0)
    df['PS'] = df['PS'].fillna(0)
    df['DIVIDEND'] = df['DIVIDEND'].fillna(0)
    df.to_csv(des_file, index=False)
    print(f"{code}历史交易记录文件转换成功." + '\r', end='', flush=True)

def update_ROE_indicators_table_from_1991(code: str):
    """ 
    更新最新的年度ROE至INDICATOR_ROE_FROM_1991数据库.
    :params code: 股票代码, 例如: '600000' or '000001'
    :return: None
    NOTE:
    当然也可以每年5月份之后重新执行一遍create_ROE_indicators_table_from_1991函数,只是比较耗时.
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
            con.execute(sql, (last_roe, full_code))
        except sqlite3.IntegrityError:
            ...
    print(f"{full_code} {last_filed} ROE数据更新成功." + " "*20 + '\r', end='', flush=True)

def update_trade_record_csv(code: str):
    """
    更新股票历史交易记录文件至今日最新数据.
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: None
    NOTE:
    当然也可以重新一遍create_trade_record_csv_table函数,但是太耗时了.
    """
    # 获取日期
    csv_file = os.path.join(TRADE_RECORD_PATH, sw.get_name_and_class_by_code(code=code)[1], code+'.csv')
    if not os.path.exists(csv_file):
        create_trade_record_csv_table(code)
    df_old = pd.read_csv(csv_file, dtype={'trade_date': str})
    last_date = df_old.loc[0, 'trade_date']  # int64型
    last_date = datetime.datetime.strptime(str(last_date), '%Y%m%d')
    start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y%m%d')  # 获取last_date第二天的日期, str型
    end_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    # 获取数据
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    pro = ts.pro_api()
    fields = ["ts_code", "trade_date", "close", "pe_ttm", "pb", "ps_ttm",
    "dv_ratio", "dv_ttm", "turnover_rate", "turnover_rate_f", "volume_ratio",
    "total_share", "float_share", "free_share", "total_mv", "circ_mv", "pe", "ps"]
    df1 = pro.daily_basic(ts_code=full_code, start_date=start_date, end_date=end_date, fields=fields)
    # 获取历史涨幅，以trade_date为索引，合并到df1中
    tmp = pro.daily(
        ts_code=full_code, start_date=start_date, end_date=end_date, 
        fields='trade_date,pct_chg'
        )
    tmp = tmp.set_index('trade_date')
    df1 = df1.set_index('trade_date')
    df1 = df1.join(tmp, how='inner')
    df1.reset_index(inplace=True)
    if df1.empty:
        print(f"{full_code}无可更新数据." + ' '*20 + '\r', end='', flush=True)
        return  # 如果df1为空,则无可更新数据,直接返回
    tmp = sw.get_name_and_class_by_code(code=code)  # 插入公司简称和行业分类
    df1.insert(2, 'company', tmp[0])
    df1.insert(3, 'industry', tmp[1])
    df1.fillna(0, inplace=True)  # 填充空值
    df_old.fillna(0, inplace=True)  # 填充空值
    df_new = pd.concat([df1, df_old], axis=0)  # 数据合并
    df_new['trade_date'] = df_new['trade_date'].astype('object')
    df_new.to_csv(csv_file, index=False)  # 保存文件
    print(f"{full_code}历史交易记录文件更新成功." + " "*20 + '\r', end='', flush=True)

def update_index_indicator_table(index: str='000300'):
    """ 
    更新指数估值数据库,用以计算指数MOS
    :param index: '000300' or '000905' or '399006'
    NOTE:
    数据期间从20040101开始至今日(tushare接口限制)
    数据库名称为INDEX_VALUE,表名为000300.SH, 000905.SH, 399006.SH
    指标包括ts_code,trade_date,pb,pe,turnover_rate,pe_ttm,turnover_rate_f,pct_chg,close,vol,amount
    """
    full_code = index + '.SH' if index.startswith('000') else index + '.SZ'
    pro = ts.pro_api()
    con = sqlite3.connect(INDEX_VALUE)
    with con:
        sql = f""" SELECT * FROM '{full_code}' """
        df = pd.read_sql_query(sql, con)
        df['trade_date'] = df['trade_date'].astype('object')
        last_date = df['trade_date'][0]
        today = pd.Timestamp.today().strftime("%Y%m%d")
        # 获取增量数据
        df1 = pro.index_dailybasic(
            ts_code=full_code, start_date=last_date, end_date=today,
            fields='ts_code,trade_date,pb,pe,pe_ttm,turnover_rate,turnover_rate_f'
        )
        try:
            df1['roe_est'] = (df1['pb'] / df1['pe']).apply(lambda x: round(x, 4))  # 保留四位小数
        except ZeroDivisionError:
            df1['roe_est'] = 0.0000
        df1 = df1.sort_values(by='trade_date', ascending=False)
        df2 = pro.index_daily(
            ts_code=full_code, start_date=last_date, end_date=today,
            fields="trade_date, pct_chg, close, vol, amount"
        )
        df2 = df2.sort_values(by='trade_date', ascending=False)
        df1 = df1.set_index('trade_date')
        df2 = df2.set_index('trade_date')
        df1 = df1.join(df2, how='inner')
        df1.reset_index(inplace=True)
        df1 = df1.sort_values(by='trade_date', ascending=False)
        # 数据合并
        df = pd.concat([df1, df], axis=0)
        df = df.drop_duplicates(subset=['trade_date'], keep='first')
        df.to_sql(name=full_code, con=con, index=False, if_exists='replace')
    print(f"{full_code}指数估值数据库更新成功." + ' '*20 + '\r', end='', flush=True)

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
    print(f"国债收率表更新成功." + ' '*20 + '\r', end='', flush=True)

if __name__ == '__main__':
    stocks = [item[0][0:6] for item in sw.get_all_stocks()]
    while True:
        print('-------------------------操作提示-------------------------')
        print('Create-Trade-CSV      Create-Curve       Create-Roe-Table')
        print('Update-Trade-CSV      Update-Curve       Update-ROE-Table')
        print('Create-Index-Value    Update-Index-Value Check-Integrity ')
        print('Sort-Conditions       Quit                               ')
        print('---------------------------------------------------------')
        msg = input('>>>> 请选择操作提示 >>>>  ')
        if msg.upper()  == 'QUIT':
            break
        elif msg.upper() == 'CREATE-TRADE-CSV':
            print('正在创建trade-record csv文件,请稍等...\r', end='', flush=True)
            for index in range(0, len(stocks), 20):
                codes = stocks[index:index+20]
                with ThreadPoolExecutor() as pool:
                    pool.map(create_trade_record_csv_table, codes)
                time.sleep(1.5)
            print('trade-record csv文件创建成功.'+ ' '*50)
        elif msg.upper() == 'CREATE-CURVE':
            print('正在创建curve表格,请稍等...\r', end='', flush=True)
            begin = datetime.date(2006, 3, 1)
            yesterday = datetime.date.today() + datetime.timedelta(days=-1)
            days = (yesterday - begin).days
            create_curve_value_table(days=days)
            print('curve表格创建成功.'+ ' '*50)
        elif msg.upper() == 'CREATE-ROE-TABLE':
            print('正在创建indicators表格,请稍等...\r', end='', flush=True)
            for index in range(0, len(stocks), 20):
                codes = stocks[index:index+20]
                with ThreadPoolExecutor() as pool:
                    pool.map(create_ROE_indicators_table_from_1991, codes)
                time.sleep(1.5)
            print('indicators表格创建成功.'+ ' '*50)
        elif msg.upper() == 'UPDATE-TRADE-CSV':
            print('正在更新trade-record csv文件,请稍等...\r', end='', flush=True)
            for index in range(0, len(stocks), 20):
                codes = stocks[index:index+20]
                with ThreadPoolExecutor() as pool:
                    pool.map(update_trade_record_csv, codes)
                time.sleep(1.5)
            print('trade-record csv文件更新成功.'+ ' '*50)
        elif msg.upper() == 'UPDATE-CURVE':
            print('正在更新curve表格,请稍等...\r', end='', flush=True)
            update_curve_value_table()
            print('curve表格更新成功.'+ ' '*50)
        elif msg.upper() == 'UPDATE-ROE-TABLE':
            print('正在更新indicators表格,请稍等...\r', end='', flush=True)
            con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
            with con:
                sql = f""" SELECT * FROM '{ROE_TABLE}' """
                df = pd.read_sql_query(sql, con)
                df = df[df.columns[:4]]  # 只取前四列
                df = df[df.iloc[:, 3].isnull()]  # 取出最新年度ROE字段为空的股票代码
                null_stocks = df['stockcode'].values.tolist()
                null_stocks = [code[0:6] for code in null_stocks]
            for index in range(0, len(null_stocks), 20):
                null_codes = null_stocks[index:index+20]
                with ThreadPoolExecutor() as pool:
                    pool.map(update_ROE_indicators_table_from_1991, null_codes)
                time.sleep(1.5)
            print('indicators表格更新成功.'+ ' '*50)
        elif msg.upper() == 'CREATE-INDEX-VALUE':
            print('正在创建指数估值数据库,请稍等...\r', end='', flush=True)
            for index in ["000300", "000905", "399006"]:
                create_index_indicator_table(index)
            print('指数估值数据库创建成功.'+ ' '*20)
        elif msg.upper() == 'UPDATE-INDEX-VALUE':
            print('正在更新指数估值数据库,请稍等...\r', end='', flush=True)
            for index in ["000300", "000905", "399006"]:
                update_index_indicator_table(index)
            print('指数估值数据库更新成功.'+ ' '*20)
        elif msg.upper() == 'SORT-CONDITIONS':
            print('正在排序条件表格,请稍等...\r', end='', flush=True)
            if not os.path.exists(TEST_CONDITION_SQLITE3):
                print(f"{TEST_CONDITION_SQLITE3}文件不存在,请检查.")
                continue
            from strategy import Strategy
            stra = Strategy()
            now = time.localtime()
            table_name = f'condition-{now.tm_year}' if now.tm_mon >= 5 else f'condition-{now.tm_year-1}'
            for mode in [0, 1, 2]:
                df = stra.comprehensive_sorting_test_condition_sqlite3(
                    table_name=table_name, riskmode=mode,
                    sqlite_name=TEST_CONDITION_SQLITE3
                )
                file_name = os.path.join(TEST_CONDITION_PATH, f"conditions-by-mode{mode}.xlsx")
                df.to_excel(file_name, index=False)
            print('条件表格排序成功.'+ ' '*50)
        elif msg.upper() == 'CHECK-INTEGRITY':
            res = check_stockcodes_integrity()
            if not res["roe_table"] and not res["trade_record_path"]:
                print("股票代码完整性检查通过,共有股票代码:", len(stocks))
            if res["roe_table"]:
                print("indicator_roe_from_1991.sqlite3文件中缺失的股票代码:")
                print(res["roe_table"])
                print("开始补齐缺失的数据...")
                with ThreadPoolExecutor() as pool:
                    pool.map(create_ROE_indicators_table_from_1991, res["roe_table"])
                print("indicator_roe_from_1991.sqlite3文件中缺失的数据已补齐."+" "*50)
            if res["trade_record_path"]:
                print("TRADE_RECORD_PATH目录中缺失的股票交易信息代码:")
                print(res["trade_record_path"])
                print("开始补齐缺失的交易信息文件...")
                diff_codes = res["trade_record_path"].values()
                for codes in diff_codes:
                    with ThreadPoolExecutor() as pool:
                        pool.map(create_trade_record_csv_table, codes)
                print("TRADE_RECORD_PATH目录中缺失的交易信息文件已补齐."+" "*50)
            if res["to_remove"]:
                print("indicator_roe_from_1991.sqlite3文件中多余的股票代码:")
                print(res["to_remove"])
                print("开始删除ROE_TABLE中非申万行业的股票清单...")
                con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
                with con:
                    for code in res["to_remove"]:
                        code = code + '.SH' if code.startswith('6') else code + '.SZ'
                        sql = f""" DELETE FROM '{ROE_TABLE}' WHERE stockcode=? """
                        con.execute(sql, (code,))
                print("ROE_TABLE中多余的股票代码已删除."+" "*50)
        else:
            continue
