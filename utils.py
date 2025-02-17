import os
import re
import sqlite3
import pandas as pd
import datetime
import time
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from typing import List, Tuple, Literal
import tushare as ts
import data
import tsswindustry as sw
from path import (INDICATOR_ROE_FROM_1991, CURVE_SQLITE3, CURVE_TABLE, 
                ROE_TABLE, TRADE_RECORD_PATH, INDEX_VALUE, STOCK_MOS_IMG, 
                INDEX_MOS_IMG, INDEX_UP_DOWN_IMG, STOCK_UP_DOWN_IMG)

def calculate_MOS_7_from_2006(code: str, date: str) -> float:
    """
    使用INDICATOR_ROE_FROM_1991等数据库计算7年MOS值,需要获取7年roe平均值 10年期国债收益率和PB.
    7年roe值取自于INDICATOR_ROE_FROM_1991数据库,10年期国债收益率取自于curve数据库,pb值取自CSV文件.
    date参数指定国债收益率和PB的日期,为yyyy-mm-dd型字符串.
    curve数据库自2006-03-01开始,故date参数不应早于2006-03-01.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2019-01-01'
    :return: 返回MOS_7值
    NOTE:
    如果参数date的月份数在1-4月,ROE年份数取date参数年份-2前推7年,否者取date参数年份-1前推7年.
    """
    # 检查参数
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not date_regex.match(date):
        raise ValueError('参数date应为yyyy-mm-dd型字符串')
    if date < '2006-03-01':
        date = '2006-03-01'
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    if date > today_str:
        date = today_str
    
    # 获取7年平均ROE值
    this_year = datetime.datetime.now().year
    year_month_list = date.split('-')
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
        tmp = [0 if item is None else item for item in tmp]  # None替换成0
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

def calculate_index_MOS_from_2006(
    index: Literal["000300", "399006", "000905"], 
    date: str
) -> float:
    """ 
    计算000300 399006 000905指数的MOS值
    :param index: 指数代码
    :param date: 日期, 例如: '2019-01-01',不早于2006-03-01和记录最早日期的较大者
    :return: 返回MOS值(1年)
    NOTE:
    roe值取自INDEX_VALUE数据库,以最接近当年4月30日roe_est值为准.
    国债收益率取自curve数据库,pb值取自INDEX_VALUE数据库pb列.
    """
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not date_regex.match(date):
        raise ValueError('参数date应为yyyy-mm-dd型字符串')
    if index not in ['000300', '399006', '000905']:
        raise ValueError('请检查指数代码是否正确(000300, 399006, 000905)')
    full_code = f'{index}.SH' if index.startswith('000') else f'{index}.SZ'

    con = sqlite3.connect(INDEX_VALUE)
    with con:
        sql = f"SELECT trade_date FROM '{full_code}' ORDER BY trade_date ASC LIMIT 1"
        start_date = con.execute(sql).fetchone()[0]
        start_date = datetime.datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
        if date < max([start_date, "2006-03-01"]):
            date = max([start_date, "2006-03-01"])

        # 获取和当年4月30日最接近的roe_est值
        sql = f"SELECT * FROM '{full_code}' WHERE trade_date LIKE '{date[0:4]}%'"
        df = pd.read_sql(sql, con)
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        target_date = f"{date[0:4]}0430"
        target_date = datetime.datetime.strptime(target_date, '%Y%m%d')
        df['diff'] = df['trade_date'].apply(lambda x: abs(x-target_date).days)
        df = df.sort_values(by='diff', ascending=True)
        roe_est = df.iloc[0]['roe_est']

        # 获取date日的指数pb值
        date1 = "".join(date.split('-'))
        sql = f"SELECT pb FROM '{full_code}' WHERE trade_date=?"
        try:
            pb = con.execute(sql, (date1, )).fetchone()[0]
        except:
            raise ValueError(f'未能获取{index}指数{date}pb值')

    # 获取date参数指定的日期及附近的10年期国债收益率
    row = find_closest_row_in_curve_table(date=date)  
    yield_value = row['value1'].values[0]

    inner_pb = roe_est*100/yield_value
    mos = 1 - pb/inner_pb
    return round(mos, 4)

def calculate_index_rising_value(
    index: Literal["000300", "399006", "000905"], 
    start_date: str, 
    end_date: str
) -> float:
    """
    计算000300 399006 000905指数期间涨幅
    :param index: 指数代码
    :param start_date: 开始日期, 例如: '2019-01-01'
    :param end_date: 结束日期, 例如: '2019-01-01'
    :return: 指数的涨幅
    NOTE:
    调用index_daily时日期格式如果是yyyy-mm-dd,则返回的结果是错误的,必须转换为yyyymmdd格式.
    而calculate_stock_rising_value函数则没有这个问题.
    为保持参数输入一致性,统一在两个函数转换日期格式.
    """
    if index not in ['000300', '399006', '000905']:
        raise ValueError('请检查指数代码是否正确(000300, 399006, 000905)')
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 日期格式转换
    if date_regex.match(start_date):
        start_date = start_date.replace('-', '')
    if date_regex.match(end_date):
        end_date = end_date.replace('-', '')
    full_code = f'{index}.SH' if index.startswith('000') else f'{index}.SZ'
    if not os.path.exists(INDEX_VALUE):
        data.create_index_indicator_table(index=index)
    con = sqlite3.connect(INDEX_VALUE)
    with con:
        sql = f"""
            SELECT trade_date, pct_chg FROM '{full_code}' WHERE 
            trade_date>=? AND trade_date<=?
        """
        df = pd.read_sql(sql, con, params=(start_date, end_date))
    if df is None or df.empty:
        rate = 0.00
    else:
        df['rate'] = df['pct_chg'].apply(lambda x: x/100 + 1)
        rate = df['rate'].prod() - 1
    return rate

def calculate_index_up_and_down_value_by_MOS(
    index: Literal["000300", "399006", "000905"], 
    date: str, 
    mos: float,
    mos_high: float, 
    mos_low: float,
) -> Tuple[float, float]:
    """
    根据MOS值计算指定日期潜在上涨幅度和下跌幅度.
    :param index: 指数代码, 例如: '000300', '399006', '000905'
    :param date: 日期, 例如: '2019-01-01'
    :param mos: MOS值, 例如: 0.50
    :param mos_high: 高点MOS值, 例如: 0.75
    :param mos_low: 低点MOS值, 例如: 0.10
    :return: 返回潜在上涨幅度和下跌幅度
    NOTE:
    potential_up = ((1-mos_low)-(1-mos)) / (1-mos)=(mos-mos_low) / (1-mos)  # 潜在上涨幅度
    potential_down = ((1-mos_high)-(1-mos)) / (1-mos)=(mos-mos_high) / (1-mos)  # 潜在下跌幅度
    该指标用于判断指数估值风险收益对比情况
    """
    if mos < mos_low or mos > mos_high:
        raise ValueError("MOS值上下限设置错误")
    potential_up = (mos - mos_low) / (1 - mos)
    potential_down = (mos - mos_high) / (1 - mos)
    return round(potential_up, 2), round(potential_down, 2)

def calculate_stock_up_and_down_value_by_MOS(
    code: str,
    date: str,
    mos: float,
    mos_high: float,
    mos_low: float,
) -> Tuple[float, float]:
    """
    计算股票潜在上涨幅度和下跌幅度.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2019-01-01'
    :param mos: MOS值, 例如: 0.50
    :param mos_high: 高点MOS值, 例如: 0.75
    :param mos_low: 低点MOS值, 例如: 0.10
    :return: 返回潜在上涨幅度和下跌幅度
    NOTE:
    potential_up = ((1-mos_low)-(1-mos)) / (1-mos)=(mos-mos_low) / (1-mos)  # 潜在上涨幅度
    potential_down = ((1-mos_high)-(1-mos)) / (1-mos)=(mos-mos_high) / (1-mos)  # 潜在下跌幅度
    该指标用于判断股票估值风险收益对比情况
    """
    if mos < mos_low or mos > mos_high:
        raise ValueError("MOS值上下限设置错误")
    potential_up = (mos - mos_low) / (1 - mos)
    potential_down = (mos - mos_high) / (1 - mos)
    return round(potential_up, 2), round(potential_down, 2)

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
    swindustry = sw.get_name_and_class_by_code(code)[1]
    csv_file = os.path.join(TRADE_RECORD_PATH, swindustry, f"{code}.csv")
    if not os.path.exists(csv_file):
        data.create_trade_record_csv_table(code)
    df = pd.read_csv(csv_file, dtype={'trade_date': str}, usecols=['trade_date', 'pct_chg'])
    df = df[(df['trade_date'] >= start_date)]
    df = df[(df['trade_date'] <= end_date)]
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


def calculate_portfolio_holding_percent(
        last_year_rate, inner_rate,
        last_series_rate, avg_rate, std_rate
) -> float:
    """
    根据最新一年年化收益化和最新一期序列收益率,计算持仓比例
    :param last_year_rate: 最新一年年化收益率
    :param inner_rate: 内在收益率
    :param last_series_rate: 最新一期序列收益率
    :param avg_rate: 平均收益率
    :param std_rate: 收益率标准差
    NOTE:计算方法如下:
    1.比较last_year_rate和inner_rate,如果last_year_rate位于(0.8, 1.2(含))inner_rate,
    则持仓比例为80%,如果位于(1.2, 1.4(含))inner_rate,则持仓比例为60%,如果位于(1.4, 1.6(含))
    inner_rate,则持仓比例为50%,如果位于(1.6, 2.0(含))inner_rate,则持仓比例为30%,如果位于2.0
    inner_rate,则持仓比例为20%.如果位于(0.6, 0.8(含))inner_rate,则持仓比例为90%,如果小于0.6,
    则持仓比例为100%.
    2.比较last_series_rate位于avg_rate正负1倍标准差之间,则持仓比例为90%,位于avg_rate正2倍标准差
    以内,则持仓比例为70%,位于avg_rate正3倍标准差以内,则持仓比例为40%,超过avg_rate正3倍标准差,持仓20%.
    位于avg_rate位于avg_rate负1倍标准差以外,则持仓比例为100%。
    3.最终持仓比例为上述两个持仓比例的最小值。
    """
    # 计算最新一年年化收益率的持仓比例
    if 0.8*inner_rate <= last_year_rate <= 1.2*inner_rate:
        holding_percent = 0.8
    elif 1.2*inner_rate < last_year_rate <= 1.4*inner_rate:
        holding_percent = 0.6
    elif 1.4*inner_rate < last_year_rate <= 1.6*inner_rate:
        holding_percent = 0.5
    elif 1.6*inner_rate < last_year_rate <= 2.0*inner_rate:
        holding_percent = 0.3
    elif last_year_rate > 2.0*inner_rate:
        holding_percent = 0.2
    elif 0.6*inner_rate <= last_year_rate < 0.8*inner_rate:
        holding_percent = 0.9
    else:
        holding_percent = 1.0
    # 计算最新一期序列收益率的持仓比例
    if avg_rate - std_rate <= last_series_rate <= avg_rate + std_rate:
        holding_percent_series = 0.9
    elif avg_rate + std_rate < last_series_rate <= avg_rate + 2*std_rate:
        holding_percent_series = 0.7
    elif avg_rate + 2*std_rate < last_series_rate <= avg_rate + 3*std_rate:
        holding_percent_series = 0.4
    elif last_series_rate > avg_rate + 3*std_rate:
        holding_percent_series = 0.2
    else:
        holding_percent_series = 1.0
    # 计算最终持仓比例
    holding_percent = min(holding_percent, holding_percent_series)
    return holding_percent

def date_to_stamp(date: str) -> int:
    """
    把yyyy-mm-dd格式的日期转换为13位时间戳
    :param date: 日期, 例如: '2022-01-01'
    :return: 13位时间戳
    """
    begin = date + " 00:00:00"
    timeArray = time.strptime(begin, "%Y-%m-%d %H:%M:%S")
    stamp = int(time.mktime(timeArray) * 1000)
    return stamp

def stamp_to_date(stamp: int) -> str:
    """
    把13位时间戳转换为yyyy-mm-dd格式的日期
    :param stamp: 13位时间戳
    :return: 日期, 例如: '2022-01-01'
    """
    timeArray = time.localtime(stamp // 1000)
    date = time.strftime("%Y-%m-%d", timeArray)
    return date

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

def get_indicator_in_trade_record(code: str, date: str, indicator: str) -> float:
    """
    获取指定股票指定日期的指定字段值
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2019-01-01'
    :param indicator: 指标字段, 例如: 'pb', 'pe', 'dv_ttm', 'dv_ratio', 'circ_mv'
    :return: 指定字段的值
    """
    row = find_closest_row_in_trade_record(code, date)
    return row[indicator].values[0]

def plot_10y_yield_curve_figure():
    """
    绘制10年期国债到期收益率曲线图.
    """
    con = sqlite3.connect(CURVE_SQLITE3)
    with con:
        df = pd.read_sql(f"SELECT * FROM '{CURVE_TABLE}'", con)
    date = df['date1'].tolist()[::-1]
    value = df['value1'].tolist()[::-1]
    plt.plot(date, value)
    plt.rcParams['font.sans-serif'] = ['Songti SC'] # 设置中文显示
    plt.fill_between(date, value, color='grey', alpha=0.1)
    plt.title(f"10年期国债到期收益率曲线图(自 {date[0]} 到 {date[-1]})")
    # xticks['loc']在10个以内全部显示,11-20个显示总数的一半,21-30个显示总数的1/3,依次类推
    xticks = {'labels': [], 'loc': []}
    for item in date:
        if item[:4] not in xticks['labels']:
            xticks['labels'].append(item[:4])
            xticks['loc'].append(item)
    if len(xticks['loc']) > 10:
        step = len(xticks['loc']) // 10 + 1
        xticks['loc'] = xticks['loc'][::step]
        xticks['labels'] = xticks['labels'][::step]
    plt.xticks(ticks=xticks['loc'], labels=xticks['labels'])
    plt.gca().yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, loc: "{:,}%".format(round(x, 2)))
    )  # 设置y轴刻度
    max_value = max(value)    # 设置最高点和最低点
    min_value = min(value)
    max_date = date[value.index(max_value)]
    min_date = date[value.index(min_value)]
    plt.text(
        max_date, max_value, f"最高点: {max_value:.4}%", ha='center', va='bottom', fontsize=12
    )
    plt.text(
        min_date, min_value, f"最低点: {min_value:.4}%", ha='center', va='top', fontsize=12
    )
    plt.gca().yaxis.grid(True)  # 显示网格
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    plt.show()

def save_whole_MOS_7_figure(code: str, dest: str = STOCK_MOS_IMG, show_figure: bool = False):
    """ 
    绘制完整的MOS_7图形保存到指定目录.
    开始日期为交易记录最早日期,如果最早日期早于2006-03-01,
    则以2006-03-01为最早日期.结束日期为交易记录的最晚日期.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param dest: 图形保存目录
    :param show_figure: 是否显示图形
    """
    sw_class = sw.get_name_and_class_by_code(code)[1]
    csv_file = os.path.join(TRADE_RECORD_PATH, sw_class, f"{code}.csv")
    df = pd.read_csv(csv_file, dtype={'trade_date': str})
    dates = df['trade_date'].tolist()
    dates = [date for date in dates if date >= '20060301']
    start_date = dates[-1]
    end_date = dates[0]
    dates = [date[0:4] + '-' + date[4:6] + '-' + date[6:8] for date in dates]
    mos_list = []
    for date in dates:
        tmp = calculate_MOS_7_from_2006(code=code, date=date)
        mos_list.append(tmp)
    # 画图
    dates = dates[::-1]
    mos_list = mos_list[::-1]
    plt.rcParams['font.sans-serif'] = ['Songti SC'] # 设置中文显示
    plt.plot(dates, mos_list)
    plt.fill_between(dates, mos_list, color='grey', alpha=0.1)
    name = sw.get_name_and_class_by_code(code)[0]  # 设置标题
    title = f"{code} {name} MOS-7 曲线图 (自 {start_date} 到 {end_date})"
    plt.title(title)
    # xticks['loc']在10个以内全部显示,11-20个显示总数的一半,21-30个显示总数的1/3,依次类推
    xticks = {'labels': [], 'loc': []}
    for item in dates:
        if item[:4] not in xticks['labels']:
            xticks['labels'].append(item[:4])
            xticks['loc'].append(item)
    if len(xticks['loc']) > 10:
        step = len(xticks['loc']) // 10 + 1
        xticks['loc'] = xticks['loc'][::step]
        xticks['labels'] = xticks['labels'][::step]
    plt.xticks(ticks=xticks['loc'], labels=xticks['labels'])  # 设置x轴刻度
    plt.gca().yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, loc: "{:,}%".format(round(x*100, 2)))
    )  # 设置y轴刻度
    max_mos = max(mos_list)  # 设置最高点和最低点
    min_mos = min(mos_list)
    max_date = dates[mos_list.index(max_mos)]
    min_date = dates[mos_list.index(min_mos)]
    plt.text(
        max_date, max_mos, f"最高点: {max_mos:.2%}", ha='center', va='bottom', fontsize=12
    )
    plt.text(
        min_date, min_mos, f"最低点: {min_mos:.2%}", ha='center', va='top', fontsize=12
    )
    plt.gca().yaxis.grid(True)  # 显示网格
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    
    if not os.path.exists(dest):
        os.mkdir(dest)
    existed_files = [file for file in os.listdir(dest) if file.startswith(code)]
    for file in existed_files:
        os.remove(os.path.join(dest, file))
    file_name = f"{code}-{name}-{start_date}-{end_date}.pdf"
    dest_file = os.path.join(dest, file_name)
    plt.savefig(dest_file)
    print(f"已保存{dest_file}")
    if show_figure:
        plt.show()
    plt.close()

def save_whole_index_MOS_figure(
    index: Literal["000300", "399006", "000905"],
    dest: str = INDEX_MOS_IMG, 
    show_figure: bool = False
):
    """ 
    绘制完整的MOS图形保存到指定目录.
    开始日期为交易记录最早日期,如果最早日期早于2006-03-01,
    则以2006-03-01为最早日期.结束日期为交易记录的最晚日期.
    :param index: 指数代码
    :param dest: 图形保存目录
    :param show_figure: 是否显示图形
    """
    full_code = f'{index}.SH' if index.startswith('000') else f'{index}.SZ'
    con = sqlite3.connect(INDEX_VALUE)
    with con:
        sql = f"SELECT * FROM '{full_code}' "
        df = pd.read_sql(sql, con)
        date_range = df['trade_date'].tolist()
        date_range = [item for item in date_range if item >= '20060301']
        date_range = [f"{item[0:4]}-{item[4:6]}-{item[6:8]}" for item in date_range][::-1]
        start = date_range[0]
        end = date_range[-1]
    mos_list = []
    for date in date_range:
        tmp = calculate_index_MOS_from_2006(index=index, date=date)
        mos_list.append(tmp)
    plt.rcParams['font.sans-serif'] = ['Songti SC']  # 设置中文显示
    plt.plot(date_range, mos_list)
    plt.fill_between(date_range, mos_list, color='grey', alpha=0.1)
    title = f"{full_code} MOS 曲线图(自 {start} 到 {end}) "
    plt.title(title)
    # xticks['loc']在10个以内全部显示,11-20个显示总数的一半,21-30个显示总数的1/3,依次类推
    xticks = {'labels': [], 'loc': []}
    for item in date_range:
        if item[:4] not in xticks['labels']:
            xticks['labels'].append(item[:4])
            xticks['loc'].append(item)
    if len(xticks['loc']) > 10:
        step = len(xticks['loc']) // 10 + 1
        xticks['loc'] = xticks['loc'][::step]
        xticks['labels'] = xticks['labels'][::step]
    plt.xticks(ticks=xticks['loc'], labels=xticks['labels'])  # 设置x轴刻度
    plt.gca().yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, loc: "{:,}%".format(round(x*100, 2)))
    )  # 设置y轴刻度
    max_mos = max(mos_list)  # 设置最高点和最低点
    min_mos = min(mos_list)
    max_date = date_range[mos_list.index(max_mos)]
    min_date = date_range[mos_list.index(min_mos)]
    plt.text(
        max_date, max_mos, f"最高点: {max_mos:.2%}", ha='center', va='bottom', fontsize=12
    )
    plt.text(
        min_date, min_mos, f"最低点: {min_mos:.2%}", ha='center', va='top', fontsize=12
    )
    plt.gca().yaxis.grid(True)
    fig = plt.gcf()
    fig.set_size_inches(16, 10)

    if not os.path.exists(dest):
        os.mkdir(dest)
    existed_files = [file for file in os.listdir(dest) if file.startswith(index)]
    for file in existed_files:
        os.remove(os.path.join(dest, file))
    s = start.replace('-', '')
    e = end.replace('-', '')
    file_name = f"{index}-{s}-{e}.pdf"
    dest_file = os.path.join(dest, file_name)
    plt.savefig(dest_file)
    print(f"已保存{dest_file}")
    if show_figure:
        plt.show()
    plt.close()

def save_index_up_and_down_value_figure(
    index: Literal["000300", "399006", "000905"],
    end_date : str = "",
    years_offset: int = 7,
    months_offset: int = 0,
    show_notions: bool = True,
    notion_nums: int = 3,
    down_value_target: float = -0.25,
    dset: str = INDEX_UP_DOWN_IMG,
    remove_existed_img: bool = True,
    show_figure: bool = False
):
    """
    绘制完整的潜在上涨幅度和下跌幅度图形保存到指定目录.
    开始日期为交易记录最早日期,如果最早日期早于2006-03-01,
    则以2006-03-01为最早日期.结束日期为交易记录的最晚日期.
    :param index: 指数代码
    :param end_date: 结束日期, 例如: '2019-01-01',默认为空字符串取值为today
    :param years_offset: 绘制的年份向前偏移量
    :param months_offset: 绘制的月份向前偏移量
    :param show_notions: 是否显示notions
    :param notion_nums: 显示的最低down_value的数量
    :param down_value_target: 指定的下跌幅度目标
    :param dset: 图形保存目录
    :param remove_existed_img: 是否删除已存在的图形文件
    :param show_figure: 是否显示图形
    """
    full_code = f'{index}.SH' if index.startswith('000') else f'{index}.SZ'
    con = sqlite3.connect(INDEX_VALUE)
    # 推算开始日期
    today = pd.Timestamp.today()
    end_date = today if not end_date else pd.Timestamp(end_date)
    start_date = end_date - pd.DateOffset(years=years_offset, months=months_offset)
    end_date = end_date.strftime("%Y%m%d")
    start_date = start_date.strftime("%Y%m%d")
    if start_date < '20060301':
        start_date = '20060301'
    with con:
        sql = f"""
        SELECT ts_code, trade_date, close FROM '{full_code}' 
        WHERE trade_date>="{start_date}" AND trade_date<="{end_date}"
        """
        df = pd.read_sql(sql, con)
        df["trade_date"] = df["trade_date"].astype(str)
        dates = df['trade_date'].tolist()
        dates = [date[:4] + '-' + date[4:6] + '-' + date[6:] for date in dates]
        mos_list = []
        for date in dates:
            mos = calculate_index_MOS_from_2006(index, date)
            mos_list.append(mos)
        df["mos"] = mos_list
        # 计算潜在上涨幅度和下跌幅度比例
        up_list = []
        down_list = []
        for date in dates:
            df_ratio = df[df['trade_date'] <= date.replace('-', '')]
            mos = df_ratio['mos'].tolist()[0]
            mos_high = df_ratio['mos'].max()
            mos_low = df_ratio['mos'].min()
            up_value, down_value = calculate_index_up_and_down_value_by_MOS(
                index=index, date=date, mos_high=mos_high, mos_low=mos_low, mos=mos
            )
            up_list.append(up_value)
            down_list.append(down_value)
        df["up_value"] = up_list
        df["down_value"] = down_list
    # 双轴图,左轴为close折线图，右轴为潜在上涨幅度和下跌幅度柱状图
    plt.rcParams['font.sans-serif'] = ['Songti SC']
    ax1: Axes
    ax2: Axes
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    dates = [date.replace('-', '') for date in dates][::-1]
    close = df['close'].tolist()[::-1]
    up_value = df['up_value'].tolist()[::-1]
    down_value = df['down_value'].tolist()[::-1]
    ax1.plot(dates, close, 'g-')
    ax2.bar(dates, up_value, color='b', alpha=0.5, label='潜在上涨幅度')
    ax2.bar(dates, down_value, color='r', alpha=0.5, label='潜在下跌幅度')
    ax1.set_xlabel('日期')
    ax1.set_ylabel(f'{index}指数收盘价', color='g')
    ax2.set_ylabel('潜在上涨幅度和下跌幅度', color='b')
    ax1.set_title(f"{full_code}(指数) 潜在上涨幅度和下跌幅度图(自 {dates[0]} 至 {dates[-1]})")
    # 设置x轴刻度
    xticks = {'labels': [], 'loc': []}
    for item in dates:
        if item[:4] not in xticks['labels']:
            xticks['labels'].append(item[:4])
            xticks['loc'].append(item)
    if len(xticks['loc']) > 10:
        step = len(xticks['loc']) // 10 + 1
        xticks['loc'] = xticks['loc'][::step]
        xticks['labels'] = xticks['labels'][::step]
    ax1.set_xticks(xticks['loc'], xticks['labels'])
    ax2.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, loc: "{:,}%".format(round(x*100, 2)))
    )
    # 显示ax1最后一个位置的数值
    notion = f"""C:{close[-1]:.2f}
    D:{down_value[-1]:.2%} U:{up_value[-1]:.2%}
    """
    ax1.text(
        dates[-1], close[-1], notion, ha='center', va='bottom', fontsize=10
    )
    # 以down_value_target画一条红色水平线,在水平线中间位置标注金额
    ax2.axhline(y=down_value_target, color='r', linestyle='--')
    ax2.text(
        dates[len(dates)//2], down_value_target, f"目标:{down_value_target:.2%}", ha='center', va='top', fontsize=10
    )
    # 以down_value中最小值画一条红色水平线,在水平线中间位置标注金额
    min_down_value = min(down_value)
    ax2.axhline(y=min_down_value, color='r', linestyle='--')
    ax2.text(
        dates[len(dates)//2], min_down_value, f"最低:{min_down_value:.2%}", ha='center', va='top', fontsize=10
    )
    ax1.grid(True)
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    plt.legend(loc='upper left')
    if show_notions:
        # 显示down_value最低值(不同年份的最低值)
        tmp_df = df.sort_values(by='down_value', ascending=True)
        tmp_res = {
            "year":[], "date":[], "down_value":[], "close":[], "up_value":[], "mos":[]
        }
        for idx, row in tmp_df.iterrows():
            if len(tmp_res["date"]) >= notion_nums:
                break
            year = row['trade_date'][0:4]
            if year not in tmp_res["year"]:  # 不同年份的最低值
                tmp_res["year"].append(year)
                tmp_res["date"].append(row['trade_date'])
                tmp_res["down_value"].append(row['down_value'])
                tmp_res["close"].append(row['close'])
                tmp_res["up_value"].append(row['up_value'])
                tmp_res["mos"].append(row['mos'])
                notion = f"""{row['trade_date']} C:{row['close']:.2f} 
                D:{row['down_value']:.2%} U:{row['up_value']:.2%}
                """
                ax1.text(
                    row['trade_date'], row['close'], notion, ha='center', va='bottom', fontsize=8
                )
        # 显示up_value最高值(不同年份的最高值)
        tmp_df = df.sort_values(by='up_value', ascending=False)
        tmp_res = {
            "year":[], "date":[], "down_value":[], "close":[], "up_value":[], "mos":[]
        }
        for idx, row in tmp_df.iterrows():
            if len(tmp_res["date"]) >= notion_nums:
                break
            year = row['trade_date'][0:4]
            if year not in tmp_res["year"]:
                tmp_res["year"].append(year)
                tmp_res["date"].append(row['trade_date'])
                tmp_res["down_value"].append(row['down_value'])
                tmp_res["close"].append(row['close'])
                tmp_res["up_value"].append(row['up_value'])
                tmp_res["mos"].append(row['mos'])
                notion = f"""{row['trade_date']} C:{row['close']:.2f} 
                D:{row['down_value']:.2%} U:{row['up_value']:.2%}
                """
                ax1.text(
                    row['trade_date'], row['close'], notion, ha='center', va='top', fontsize=8
                )
    # 保存图形
    if not os.path.exists(dset):
        os.mkdir(dset)
    if remove_existed_img:
        existed_files = [file for file in os.listdir(dset) if file.startswith(index)]
        for file in existed_files:
            os.remove(os.path.join(dset, file))
    file_name = f"{full_code}(指数)-{dates[0].replace('-', '')}-{dates[-1].replace('-', '')}.pdf"
    dest_file = os.path.join(dset, file_name)
    plt.savefig(dest_file)
    print(f"已保存{dest_file}")
    if show_figure:
        plt.show()
    plt.close()

def save_stock_up_and_down_value_figure(
    code: str,
    end_date: str = "",
    years_offset: int = 7,
    months_offset: int = 0,
    show_notions: bool = True,
    notion_nums: int = 3,
    down_value_target: float = -0.30,
    dest: str = STOCK_UP_DOWN_IMG,
    remove_existed_img: bool = True,
    show_figure: bool = False
):
    """
    绘制股票潜在上涨幅度和下跌幅度图形.
    开始日期为交易记录最早日期,如果最早日期早于2006-03-01,
    则以2006-03-01为最早日期.结束日期为交易记录的最晚日期.
    :param code: 股票代码, 例如: '600000' or '000001'
    :param end_date: 结束日期, 例如: '2019-01-01',默认为空字符串取值为today
    :param years_offset: 绘制的年份向前偏移量
    :param months_offset: 绘制的月份向前偏移量
    :param show_notions: 是否显示notions
    :param notion_nums: 显示的最低down_value的数量
    :param down_value_target: 指定的下跌幅度目标
    :param dest: 图形保存目录
    :param remove_existed_img: 是否删除已存在的图形文件
    :param show_figure: 是否显示图形
    """
    name = sw.get_name_and_class_by_code(code)[0]
    sw_class = sw.get_name_and_class_by_code(code)[1]
    csv_file = os.path.join(TRADE_RECORD_PATH, sw_class, f"{code}.csv")
    df = pd.read_csv(csv_file, dtype={'trade_date': str})
    # 推算开始日期
    today = pd.Timestamp.today()
    end_date = today if not end_date else pd.Timestamp(end_date)
    start_date = today - pd.DateOffset(years=years_offset, months=months_offset)
    end_date = end_date.strftime("%Y%m%d")
    start_date = start_date.strftime("%Y%m%d")
    if start_date < '20060301':
        start_date = '20060301'
    df = df[df['trade_date'] >= start_date]
    df = df[df['trade_date'] <= end_date]
    dates = df['trade_date'].tolist()
    dates = [date[:4] + '-' + date[4:6] + '-' + date[6:] for date in dates]
    mos_list = []
    for date in dates:
        mos = calculate_MOS_7_from_2006(code=code, date=date)
        mos_list.append(mos)
    df["mos"] = mos_list
    # 计算潜在上涨幅度和下跌幅度比例
    up_list = []
    down_list = []
    for date in dates:
        df_ratio = df[df['trade_date'] <= date.replace('-', '')]
        mos_high = df_ratio['mos'].max()
        mos_low = df_ratio['mos'].min()
        mos = df_ratio['mos'].tolist()[0]
        up_value, down_value = calculate_stock_up_and_down_value_by_MOS(
            code=code, date=date, mos_high=mos_high, mos_low=mos_low, mos=mos
        )
        up_list.append(up_value)
        down_list.append(down_value)
    df["up_value"] = up_list
    df["down_value"] = down_list
    # 绘双轴图,左轴为close折线图，右轴为潜在上涨幅度和下跌幅度柱状图
    plt.rcParams['font.sans-serif'] = ['Songti SC']  # 设置中文显示
    ax1: Axes
    ax2: Axes
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    dates = [date.replace('-', '') for date in dates][::-1]
    close = df['close'].tolist()[::-1]
    up_value = df['up_value'].tolist()[::-1]
    down_value = df['down_value'].tolist()[::-1]
    ax1.plot(dates, close, 'g-')
    ax2.bar(dates, up_value, color='b', alpha=0.5, label='潜在上涨幅度')
    ax2.bar(dates, down_value, color='r', alpha=0.5, label='潜在下跌幅度')
    ax1.set_xlabel('日期')
    ax1.set_ylabel(f'{code}股票收盘价', color='g')
    ax2.set_ylabel('潜在上涨幅度和下跌幅度', color='b')
    ax1.set_title(f"{code} {name} 潜在上涨幅度和下跌幅度图(自 {dates[0]} 至 {dates[-1]})")
    # 设置x轴刻度
    xticks = {'labels': [], 'loc': []}
    for item in dates:
        if item[:4] not in xticks['labels']:
            xticks['labels'].append(item[:4])
            xticks['loc'].append(item)
    if len(xticks['loc']) > 10:
        step = len(xticks['loc']) // 10 + 1
        xticks['loc'] = xticks['loc'][::step]
        xticks['labels'] = xticks['labels'][::step]
    ax1.set_xticks(xticks['loc'], xticks['labels'])
    ax2.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, loc: "{:,}%".format(round(x*100, 2)))
    )
    # 显示ax1最后一个位置的数值
    notion = f"""C:{close[-1]:.2f}
    D:{down_value[-1]:.2%} U:{up_value[-1]:.2%}
    """
    ax1.text(
        dates[-1], close[-1], notion, ha='center', va='bottom', fontsize=10
    )
    # 以down_value_target画一条红色水平线,在水平线中间位置标注金额
    ax2.axhline(y=down_value_target, color='r', linestyle='--')
    ax2.text(
        dates[len(dates)//2], down_value_target, f"目标:{down_value_target:.2%}", ha='center', va='bottom', fontsize=10
    )
    # 以down_value中最小值画一条红色水平线,在水平线最右边位置标注金额
    min_down_value = min(down_value)
    ax2.axhline(y=min_down_value, color='r', linestyle='--')
    ax2.text(
        dates[len(dates)//2], min_down_value, f"最低:{min_down_value:.2%}", ha='center', va='top', fontsize=10
    )
    ax1.grid(True)
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    plt.legend(loc='upper left')
    if show_notions:
        # 显示down_value最低值(不同年份的最低值)
        tmp_df = df.sort_values(by='down_value', ascending=True)
        tmp_res = {
            "year":[], "date":[], "down_value":[], "close":[], "up_value":[], "mos":[]
        }
        for idx, row in tmp_df.iterrows():
            if len(tmp_res["date"]) >= notion_nums:
                break
            year = row['trade_date'][0:4]
            if year not in tmp_res["year"]:
                tmp_res["year"].append(year)
                tmp_res["date"].append(row['trade_date'])
                tmp_res["down_value"].append(row['down_value'])
                tmp_res["close"].append(row['close'])
                tmp_res["up_value"].append(row['up_value'])
                tmp_res["mos"].append(row['mos'])
                notion = f"""{row['trade_date']} C:{row['close']:.2f}
                D:{row['down_value']:.2%} U:{row['up_value']:.2%}
                """
                ax1.text(
                    row['trade_date'], row['close'], notion, ha='center', va='bottom', fontsize=8
                )
        # 显示up_value最高值(不同年份的最高值)
        tmp_df = df.sort_values(by='up_value', ascending=False)
        tmp_res = {
            "year":[], "date":[], "down_value":[], "close":[], "up_value":[], "mos":[]
        }
        for idx, row in tmp_df.iterrows():
            if len(tmp_res["date"]) >= notion_nums:
                break
            year = row['trade_date'][0:4]
            if year not in tmp_res["year"]:
                tmp_res["year"].append(year)
                tmp_res["date"].append(row['trade_date'])
                tmp_res["down_value"].append(row['down_value'])
                tmp_res["close"].append(row['close'])
                tmp_res["up_value"].append(row['up_value'])
                tmp_res["mos"].append(row['mos'])
                notion = f"""{row['trade_date']} C:{row['close']:.2f} 
                D:{row['down_value']:.2%} U:{row['up_value']:.2%}
                """
                ax1.text(
                    row['trade_date'], row['close'], notion, ha='center', va='top', fontsize=8
                )
    # 保存图形
    if not os.path.exists(dest):
        os.mkdir(dest)
    if remove_existed_img:
        existed_files = [file for file in os.listdir(dest) if file.startswith(code)]
        for file in existed_files:
            os.remove(os.path.join(dest, file))
    file_name = f"{code}-{name}-{dates[0].replace('-', '')}-{dates[-1].replace('-', '')}.pdf"
    dest_file = os.path.join(dest, file_name)
    plt.savefig(dest_file)
    print(f"已保存{dest_file}")
    if show_figure:
        plt.show()
    plt.close()

def get_gaps_statistic_data(code: str, is_index: bool = False) -> pd.DataFrame:
    """ 
    获取股票和指数缺口及回补情况
    :param code: 股票代码, 例如: '600000' or '000001'
    :param is_index: 是否为指数
    :return: 缺口及回补情况.键为缺口日期,值为[缺口类型, 回补日期, 回补天数]
    NOTE:
    缺口类型: up(向上跳空)和down(向下跳空)
    """
    pro = ts.pro_api()
    if not is_index:
        ts_code = f'{code}.SH' if code.startswith('6') else f'{code}.SZ'
        df = pro.daily(ts_code=ts_code)
    else:
        ts_code = f'{code}.SH' if code.startswith('000') else f'{code}.SZ'
        df = pro.index_daily(ts_code=ts_code)
    # 向上缺口:今日最低价>昨日最高价
    # 向下缺口:今日最高价<昨日最低价
    # 逐行检查,判断是否存在缺口,如有则记录前一日的日期和缺口类型
    result = {}  # 返回值
    for index, row in df.iterrows():
        if index == len(df) - 1:
            break
        elif row['low'] > df.iloc[index+1]['high']:
            date = df.iloc[index+1]['trade_date']
            result[date] = ['up', ]
        elif row['high'] < df.iloc[index+1]['low']:
            date = df.iloc[index+1]['trade_date']
            result[date] = ['down', ]
    # 计算回补日期,回补天数和回补幅度
    # 排序遍历缺口记录result,如果为up类型缺口,则搜索后续日期,
    # 如果出现最低价<缺口日最高价,则记录该日期为回补日期.
    # 如果为down类型缺口,则搜索后续日期,如果出现最高价>
    # 缺口日最低价,则记录该日期为回补日期.
    result = dict(sorted(result.items(), key=lambda x:x[0], reverse=False))
    for date, item in result.items():
        if item[0]  == "up":
            high = df[df['trade_date'] == date]['high'].values[0]
            tmp_df = df[(df['low'] < high)&(df['trade_date'] > date)]
        elif item[0] == "down":
            low = df[df['trade_date'] == date]['low'].values[0]
            tmp_df = df[(df['high'] > low)&(df['trade_date'] > date)]
        if not tmp_df.empty:
            tmp_df = tmp_df.sort_values(by='trade_date', ascending=True)
            item.append(tmp_df.iloc[0]['trade_date'])
            item.append(
                (datetime.datetime.strptime(tmp_df.iloc[0]['trade_date'], '%Y%m%d')
                 - datetime.datetime.strptime(date, '%Y%m%d')).days
            )
        else:
            item.append(None)
            item.append(None)
    df = pd.DataFrame(result).T
    df.columns = ['gap_type', 'recovery_date', 'recovery_days']
    return df

if __name__ == "__main__":
    res = calculate_stock_rising_value('000333', '2022-06-01', '2023-06-01')
    print(f"2022-06-01至2023-06-01,000333涨幅为{res:.2%}")
    res = calculate_index_rising_value('000300', '2022-06-01', '2023-06-01')
    print(f"2023-06-01至2024-06-01,000300涨幅为{res:.2%}")