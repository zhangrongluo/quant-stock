import os
import sqlite3
import time
import random
import datetime
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from typing import List, Dict, Union, Literal
import utils
import tsswindustry as sw
from path import (INDICATOR_ROE_FROM_1991, ROE_TABLE, TEST_CONDITION_SQLITE3, STRATEGIES, 
                MOS_STEP, HOLDING_TIME, MAX_NUMBERS, ROE_LIST, MOS_RANGE, DV_LIST, TRADE_MONTH)

pd.set_option('display.colheader_justify', 'left')
pd.set_option('display.max_colwidth', 20)

class Strategy:
    def __init__(self):
        pass

    @staticmethod
    def generate_ROE_test_conditions(strategy: str, items: int = 10) -> List[Dict]:
        """
        生成测试条件列表,用于测试策略
        :param strategy: 策略名称, 例如: 'roe', 'roe-dividend', 'roe-mos', 'roe-mos-dividend', 'roe-mos-multi-yield'
        :param items: 生成的测试条件数量, 例如: 10
        :return: 测试条件列表
        NOTE:
        holding_time和trade_month键是后新增的,以前的测试条件没有这两个键,默认为12个月和6月.
        """
        if strategy.upper() not in STRATEGIES:
            raise ValueError(f'请检查策略名称是否在列表中({STRATEGIES})')

        condition = []  # 定义返回值
        if strategy.upper() == 'ROE':
            for item in range(items):
                roe_value = random.randint(*ROE_LIST)
                period = random.randint(5, 10)
                tmp =  {
                    'strategy': strategy.upper(), 
                    'test_condition': {
                        'roe_list': [roe_value,]*period,  
                        'roe_value': roe_value, 
                        'period': period,
                        'holding_time': random.choice(HOLDING_TIME),
                        'trade_month': random.choice(TRADE_MONTH),
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == 'ROE-MOS':
            for item in range(items):
                roe_value = random.randint(*ROE_LIST)
                mos_range = [round(random.uniform(*MOS_RANGE), 4) for _ in range(2)]
                mos_range.sort()
                if mos_range[1] - mos_range[0] > MOS_STEP:
                    mos_range[1] = round(mos_range[0] + MOS_STEP, 4)  # 限制mos_range的最大步长
                tmp =  {
                    'strategy': strategy.upper(), 
                    'test_condition': {
                        'roe_list': [roe_value]*7, 
                        'mos_range': mos_range,
                        'holding_time': random.choice(HOLDING_TIME),
                        'trade_month': random.choice(TRADE_MONTH),
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == 'ROE-DIVIDEND':
            for item in range(items):
                roe_value = random.randint(*ROE_LIST)
                period = random.randint(5, 10)
                roe_list = [roe_value]*period
                dividend = random.randint(*DV_LIST)
                tmp =  {
                    'strategy': strategy.upper(),
                    'test_condition': {
                        'roe_list': roe_list,
                        'period': period,
                        'dividend': dividend,
                        'holding_time': random.choice(HOLDING_TIME),
                        'trade_month': random.choice(TRADE_MONTH),
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == 'ROE-MOS-DIVIDEND':
            for item in range(items):
                roe_value = random.randint(*ROE_LIST)
                mos_range = [round(random.uniform(*MOS_RANGE), 4) for _ in range(2)]
                mos_range.sort()
                if mos_range[1] - mos_range[0] > MOS_STEP:
                    mos_range[1] = round(mos_range[0] + MOS_STEP, 4)  # 限制mos_range的最大步长
                dividend = random.randint(*DV_LIST)
                tmp =  {
                    'strategy': strategy.upper(),
                    'test_condition': {
                        'roe_list': [roe_value]*7,
                        'mos_range': mos_range,
                        'dividend': dividend,
                        'holding_time': random.choice(HOLDING_TIME),
                        'trade_month': random.choice(TRADE_MONTH),
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == "ROE-MOS-MULTI-YIELD":
            # 该策略和ROE-MOS-DIVIDEND策略类似,只是用每个交易日10年国债利率的倍数替代固定股息率.
            for item in range(items):
                roe_value = random.randint(*ROE_LIST)
                mos_range = [round(random.uniform(*MOS_RANGE), 4) for _ in range(2)]
                mos_range.sort()
                if mos_range[1] - mos_range[0] > MOS_STEP:
                    mos_range[1] = round(mos_range[0] + MOS_STEP, 4)  # 限制mos_range的最大步长
                multi_list = np.arange(0.5, 3.5, 0.1)  # 倍数列表
                multi_value = round(np.random.choice(multi_list, 1)[0], 2)
                tmp = {
                    'strategy': strategy.upper(),
                    'test_condition': {
                        'roe_list': [roe_value]*7,
                        'mos_range': mos_range,
                        'multi_value': multi_value,
                        'holding_time': random.choice(HOLDING_TIME),
                        'trade_month': random.choice(TRADE_MONTH),
                    }
                }
                condition.append(tmp)
        else:
            ...
        return condition

    def display_result_of_strategy(self, strategy: Dict):
        """
        显示选股策略的具体结果.策略使用self.get_conditions_from_sqlite3获取.
        结构如下:{'strategy': '...', 'test_condition': {...}}
        :param strategy: roe、roe-dividend、roe-mos、roe-mos-dividend、roe-mos-multi-yield.
        """
        name = strategy['strategy']
        condition = strategy['test_condition']
        print(f"正在执行{name}选股策略,请稍等......")
        print('++'*50)
        if name.upper() == 'ROE':
            res = self.ROE_only_strategy_backtest_from_1991(**condition)
        elif name.upper() == 'ROE-MOS':
            res = self.ROE_MOS_strategy_backtest_from_1991(**condition)
        elif name.upper() == 'ROE-DIVIDEND':
            res = self.ROE_DIVIDEND_strategy_backtest_from_1991(**condition)
        elif name.upper() == 'ROE-MOS-DIVIDEND':
            res = self.ROE_MOS_DIVIDEND_strategy_backtest_from_1991(**condition)
        elif name.upper() == 'ROE-MOS-MULTI-YIELD':
            res = self.ROE_MOS_MULTI_YIELD_strategy_backtest_from_1991(**condition)
        for key, value in sorted(res.items(), key=lambda x: x[0]):
            print(key, '投资组合', f'共{len(value)}', '只股票')
            start_end = key.split(':')[0]  # 选股时间段
            start_year = int(start_end.split('-')[0][1:5])  # 选股起始年份
            end_year = int(start_end.split('-')[1][1:5])  # 选股结束年份
            columns = list(range(start_year, end_year-1, -1))
            columns = [f"Y{item}" for item in columns]
            columns = ["股票代码", "股票名称", "申万行业"] + columns
            if msg.upper() == "ROE-DIVIDEND":
                columns.append("DV_ttm")
                columns.append("DV_ratio")
            elif msg.upper() == "ROE-MOS":
                columns.append("MOS7")
            elif msg.upper() == "ROE-MOS-DIVIDEND":
                columns.append("MOS7")
                columns.append("DV_ttm")
                columns.append("DV_ratio")
            elif msg.upper() == "ROE-MOS-MULTI-YIELD":
                columns.append("MOS7")
                columns.append("DV_ttm")
                columns.append("DV_ratio")
                columns.append("M_Yield")
            else:
                pass
            df = pd.DataFrame(value, columns=columns)
            if not df.empty:
                print(df)
            stock_codes = df['股票代码'].tolist()
            stock_codes = [item[0:6] for item in stock_codes]
            start_date = key.split(":")[1]
            end_date = key.split(":")[2]
            res = utils.calculate_portfolio_rising_value(stock_codes, start_date, end_date)
            print('该组合在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            res = utils.calculate_index_rising_value('000300', start_date, end_date)
            print('沪深300在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            print('--'*50)

    def test_strategy_portfolio(
        self, 
        strategy: str, 
        result: Dict, 
        index: Literal['000300', '399006', '000905'] = '000300',
        max_numbers: int = MAX_NUMBERS
    ) -> Union[str, Dict]:
        """
        对选股策略的测试结果进行初步测试,生成该测试结果每个时间组股票组合的收益率和指定指数的收益率,即测试结果和指数的收益对比.
        :param strategy:选股策略名称,支持'ROE-DIVIDEND','ROE-MOS', 'ROE-MOS-DIVIDEND', 'ROE-MOS-MULTI-YIELD'.
        :param result:策略类方法的返回值,即测试条件相对应的测试结果.
        :param index:指定测试的指数,沪深300(000300),创业板指(399006),中证500(000905).
        :param max_numbers:时间组最大平均选股数量,默认为15.
        :return:返回值为字典,键为时间组(和result参数时间组相同),值为该时间组的选股组合和指定指数的收益率.
        NOTE:
        如果result参数时间组平均持股数量大于15,直接返回定制的测试结果
        """
        if strategy.upper() not in STRATEGIES:
            raise ValueError(f'请检查策略名称是否在列表中({STRATEGIES})')
        test_result = {date: [] for date in result.keys()}  # 定义返回值

        if sum([len(item) for item in result.values()])/len(result) > max_numbers:
            # print('测试结果股票数量过多,为减轻计算压力,返回定制的结果')
            return {date: [0, 0] for date in result.keys()}
        
        for date, stocks in sorted(result.items(), key=lambda x: x[0]):  # 对每个时间组的选股结果进行回测
            code_list = [item[0][0:6] for item in stocks]  # 不含后缀
            start_date = date.split(":")[1]
            end_date = date.split(":")[2]
            daily_return = utils.calculate_portfolio_rising_value(code_list, start_date, end_date)  # 获取组合的收益率
            test_result[date].append(daily_return)
            index_return = utils.calculate_index_rising_value(index, start_date, end_date)
            test_result[date].append(index_return)
        return test_result

    def evaluate_portfolio_effect(
        self, 
        test_condition: Dict, 
        test_result: Dict, 
        portfolio_test_result: Dict
    ) -> Dict:
        """
        对测试结果和指数的收益对比进行综合评估,根据评估结果对测试条件加以储存和动态调整.
        评估方法: 对某个测试结果中每个时间组的组合和指数的收益率对比,并计算该测试结果的内在收益率.
        某个测试结果战胜000300指数的比例为0则basic_ration基本比率为0.如某个结果共10个有效时间组(valid_groups),
        战胜000300指数的次数为8次,则basic_ration为0.8.选股组合的inner_rate内在收益率为各有效时间组组合总收益的复合收益率.
        down_max为所有有效时间组中最大回测.
        计算basic_ratio,inner_rate,down_max,highest_rate,avg_rate,std_rate和score,均使用有效时间组(valid_groups),
        即该时间组包含的股票数在5至25之间.
        :param test_condition: 测试条件,结构为{'strategy': 'ROE', 'test_condition': {...}}.
        :param test_result: 测试结果,策略类方法的返回值.
        :param portfolio_test_result: 测试结果和指数的收益对比,test_strategy_portfolio的返回值.
        :return: 为该测试条件的评估结果,结构为{'basic_ratio': 0.8, 'inner_rate': 0.1,...}.
        """
        evaluate_result = {}  # 定义返回值

        evaluate_result['strategy'] = test_condition['strategy']
        evaluate_result['test_condition'] = test_condition['test_condition']
        # total_groups = len([date for date, stocks in test_result.items() if stocks])  # 获取总时间组数,不含空代码集合
        total_groups = len(test_result.keys())  # 获取总时间组数目,包含空代码集合
        evaluate_result['total_groups'] = total_groups

        # 获取有效时间组
        valid_groups = {date: stocks for date, stocks in test_result.items() if 5 <= len(stocks) <= 25}
        valid_groups = dict(sorted(valid_groups.items(), key=lambda x: x[0]))
        evaluate_result['valid_groups'] = len(valid_groups)
        evaluate_result['valid_percent'] = round(len(valid_groups) / total_groups, 4)

        # 获取有效时间组的键名
        valid_groups_keys = list(valid_groups.keys())
        evaluate_result['valid_groups_keys'] = valid_groups_keys

        # 计算basic_ratio和组合收益差
        win_count = 0
        delta_rate = []
        for date, stocks in sorted(valid_groups.items(), key=lambda x: x[0]):
            if portfolio_test_result[date][0] > portfolio_test_result[date][1]:
                win_count += 1
            tmp = round(portfolio_test_result[date][0] - portfolio_test_result[date][1], 4)
            delta_rate.append(tmp)
        basic_ratio = win_count / len(valid_groups) if valid_groups else 0
        evaluate_result['basic_ratio'] = round(basic_ratio, 4)
        evaluate_result['delta_rate'] = delta_rate

        # 计算inner_rate 和 down_max,inner_rate为有效时间组内在年化收益率,down_max为有效时间组单期最大回撤
        rate_list = []
        total_return = 1
        # 获取持股时间。20230523以前的测试条件没有持股时间键名,默认持股时间为12个月
        holding_time = test_condition['test_condition'].get('holding_time', 12) 
        for date, stocks in valid_groups.items():
            total_return *= (1 + portfolio_test_result[date][0])
            rate_list.append(portfolio_test_result[date][0])
        evaluate_result['rate_list'] = rate_list
        # 将有效时间组的总时长转换为年数，用于计算内在年化收益率
        years = evaluate_result['valid_groups'] * holding_time / 12
        inner_rate = total_return ** (1 / years) - 1 if valid_groups else 0
        evaluate_result['inner_rate'] = round(inner_rate, 4)
        evaluate_result['down_max'] = round(min(rate_list), 4) if valid_groups else 0
        evaluate_result['highest_rate'] = round(max(rate_list), 4) if valid_groups else 0
        evaluate_result['avg_rate'] = round(np.mean(rate_list), 4) if valid_groups else 0
        evaluate_result['std_rate'] = round(np.std(rate_list), 4) if valid_groups else 0

        # 调用calculate_score_of_test_condition计算综合评分score
        score = self.calculate_score_of_test_condition(
            evaluate_result['inner_rate'], evaluate_result['valid_percent'], 
            evaluate_result['basic_ratio'], evaluate_result['down_max']
        )
        evaluate_result['score'] = score

        # 添加日期
        evaluate_result['date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
        return evaluate_result

    def save_strategy_to_sqlite3(
        self, 
        evaluate_result: Dict,
        table_name,
        sqlite_file: str = TEST_CONDITION_SQLITE3,
    ) -> None:
        """
        如某个结果综合得分超过85分且valid_percent大于35%,则储存该组合的测试条件和相关评估信息到数据库.
        数据库内容:strategy、test_condition、total_groups(总时间组数目)、valid_groups(有效时间组数目)、
        valid_percent(有效时间组占比)、valid_groups_keys(有效时间组清单)、basci_ratio(对000300的胜率)
        和inner_rate(内在收益率)、down_max(最大回撤)、score(综合得分)、date(保存日期).
        :param evaluate_result: 测试结果和指数收益对比的评估结果的返回值. 
        :param table_name: 目标数据库表名,默认为CONDITION_TABLE.
        :param sqlite_file: 目标数据库文件路径,默认为TEST_CONDITION_SQLITE3.
        :return: None
        NOTE:
        综合得分低于85分或者valid_percent小于0.35,不保存返回.
        """
        if evaluate_result['score'] < 85 or evaluate_result['valid_percent'] < 0.35:
            return
        conn = sqlite3.connect(sqlite_file)
        with conn:
            sql = f"""
                CREATE TABLE IF NOT EXISTS '{table_name}'
                (
                    strategy TEXT, 
                    test_condition TEXT, 
                    total_groups INTEGER, 
                    valid_groups INTEGER, 
                    valid_percent REAL, 
                    valid_groups_keys TEXT, 
                    basic_ratio REAL, 
                    inner_rate REAL,
                    down_max REAL, 
                    score REAL, 
                    date TEXT,
                    PRIMARY KEY(strategy, test_condition)
                )
            """
            conn.execute(sql)  # 创建表格

            sql = f"""
                INSERT OR REPLACE INTO '{table_name}'
                (
                    strategy,
                    test_condition,
                    total_groups,
                    valid_groups,
                    valid_percent,
                    valid_groups_keys,
                    basic_ratio,
                    inner_rate,
                    down_max,
                    score,
                    date
                )
                VALUES
                (
                    '{evaluate_result['strategy']}',
                    '{json.dumps(evaluate_result['test_condition'])}',
                    {evaluate_result['total_groups']},
                    {evaluate_result['valid_groups']},
                    {evaluate_result['valid_percent']},
                    '{json.dumps(evaluate_result['valid_groups_keys'])}',
                    {evaluate_result['basic_ratio']},
                    {evaluate_result['inner_rate']},
                    {evaluate_result['down_max']},
                    {evaluate_result['score']},
                    '{evaluate_result['date']}'
                )
            """
            conn.execute(sql)
            conn.commit()
            print('已保存测试条件到数据库!')

    @staticmethod
    def calculate_score_of_test_condition(
        inner_rate: float, valid_percent: float, basic_ratio: float, down_max: float
    ) -> float:
        """
        获取测试条件的评分,综合评分规则为:
        basic_ratio*0.3+inner_rate*0.5+valid_percent*0.05+down_max*0.15
        inner_rate valid_percent basic_ratio最大得分均为100分.单项评分如下:
        inner_rate: [0.25-]:100分, [0.2-0.25]:90分, [0.15-0.2]:80分, [0.1-0.15]:70分, [0.06-0.1]:60分
        valid_percent: [0.7-]:100分, [0.6-0.7]:90分, [0.5-0.6]:80分, [0.4-0.5]:70分, [0.3-0.4]:60分
        basic_ratio: [0.85-]:100分, [0.8-0.85]:90分, [0.75-0.8]:80分, [0.7-0.75]:70分, [0.6-0.7]:60分
        down_max: [0.00-]:100分, [-0.10-0.00]:90分, [-0.20--0.10]:80分, [-0.30--0.20]:70分, [--0.30]:60分
        :param inner_rate: 内在收益率
        :param valid_percent: 有效时间组占比
        :param basic_ratio: 对000300的胜率
        :param down_max: 有效时间组最大回撤
        :return: 测试条件的评分
        """
        # 计算inner_rate的评分
        if inner_rate >= 0.25:
            inner_rate_score = 100
        elif inner_rate >= 0.2:
            inner_rate_score = 90
        elif inner_rate >= 0.15:
            inner_rate_score = 80
        elif inner_rate >= 0.1:
            inner_rate_score = 70
        elif inner_rate >= 0.06:
            inner_rate_score = 60
        else:
            inner_rate_score = 0

        # 计算valid_percent的评分
        if valid_percent >= 0.7:
            valid_percent_score = 100
        elif valid_percent >= 0.6:
            valid_percent_score = 90
        elif valid_percent >= 0.5:
            valid_percent_score = 80
        elif valid_percent >= 0.4:
            valid_percent_score = 70
        elif valid_percent >= 0.3:
            valid_percent_score = 60
        else:
            valid_percent_score = 0
        
        # 计算basic_ratio的评分
        if basic_ratio >= 0.85:
            basic_ratio_score = 100
        elif basic_ratio >= 0.8:
            basic_ratio_score = 90
        elif basic_ratio >= 0.75:
            basic_ratio_score = 80
        elif basic_ratio >= 0.7:
            basic_ratio_score = 70
        elif basic_ratio >= 0.6:
            basic_ratio_score = 60
        else:
            basic_ratio_score = 0

        # 计算down_max的评分
        if down_max >= 0:
            down_max_score = 100
        elif down_max >= -0.1:
            down_max_score = 90
        elif down_max >= -0.2:
            down_max_score = 80
        elif down_max >= -0.3:
            down_max_score = 70
        else:
            down_max_score = 60

        # 计算综合评分
        score = inner_rate_score*0.5 + valid_percent_score*0.05 + basic_ratio_score*0.30 + down_max_score*0.15
        return score

    def test_strategy_specific_condition(
        self, 
        condition: Dict, 
        table_name,
        sqlite_file: str = TEST_CONDITION_SQLITE3,
        display: bool = False,
    ):
        """
        测试回测类的闭环效果,测试对象为特定的测试条件,测试结果将保存到数据库
        :param condition: 测试条件,字典类型,结构如下:{'strategy': 'ROE', 'test_condition': {...}}
        :param table_name: 保存测试结果的sqlite3数据库中的表名
        :param sqlite_file: 保存测试结果的sqlite3数据库文件
        :param display: 是否显示中间结果
        :return: None
        """
        strategy = condition['strategy']
        if strategy == 'ROE':
            result = self.ROE_only_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-MOS':
            result = self.ROE_MOS_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-DIVIDEND':
            result = self.ROE_DIVIDEND_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-MOS-DIVIDEND':
            result = self.ROE_MOS_DIVIDEND_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-MOS-MULTI-YIELD':
            result = self.ROE_MOS_MULTI_YIELD_strategy_backtest_from_1991(**condition['test_condition'])
        if display:
            print('+'*120)
            print(result)
        
        # 测试该测试结果和指数的收益对比
        portfolio_test_result = self.test_strategy_portfolio(
            strategy=strategy, result=result
        )
        if display:
            print('+'*120)
            print(portfolio_test_result)
        
        # 测试该测试结果的评估
        evaluate_result = self.evaluate_portfolio_effect(
            test_condition=condition, 
            test_result=result, 
            portfolio_test_result=portfolio_test_result
        )
        if display:
            print('+'*120)
            print(evaluate_result)
        
        # 将该测试结果保存到数据库
        self.save_strategy_to_sqlite3(
            evaluate_result=evaluate_result, 
            sqlite_file=sqlite_file, 
            table_name=table_name
        )

    def test_strategy_random_condition(
        self, 
        table_name,
        sqlite_file: str = TEST_CONDITION_SQLITE3,
        times: int = 10, 
        display: bool = False
    ):
        """
        测试回测类的闭环效果,测试对象为随机生成的测试条件
        :param table_name: 保存测试结果的sqlite3数据库中的表名
        :param sqlite_file: 保存测试结果的sqlite3数据库文件
        :param times: 测试次数
        :param display: 是否显示中间结果
        :return: None
        """
        start = time.time()
        number = 0
        for i in range(times):
            print(f'第{i+1}轮测试......'.ljust(120, ' '))
            strategy = random.choice(STRATEGIES)
            items = random.randint(1, 5)
            number += items
            condition_list = self.generate_ROE_test_conditions(strategy=strategy, items=items)
            if display:
                print('+'*120)
                print(condition_list)
            for condition in condition_list:  # 测试
                print(f'测试条件(From quant-stock):{condition}'.ljust(120, ' '))
                self.test_strategy_specific_condition(
                    condition=condition, display=display, 
                    sqlite_file=sqlite_file, table_name=table_name
                )
        end = time.time()
        print('+'*120)
        print(f'共测试{number}次，耗时{round(end-start, 4)}秒')
        print(f'平均每次测试耗时{round((end-start)/number, 4)}秒')

    def calculate_condition_total_retrun(
        self, 
        condition: Dict, 
        index: Literal["000300", "399006", "000905"] = "000300",
        draw_return_figure: bool = False
    ) -> pd.DataFrame:
        """
        计算测试条件的总收益率
        :param condition: 测试条件:{'strategy': 'ROE', 'test_condition': {...}}
        :param index: 指数代码,默认为'000300'
        :param draw_return_figure: 是否绘制收益率图
        """
        strategy = condition['strategy']
        if strategy == 'ROE':
            result = self.ROE_only_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-MOS':
            result = self.ROE_MOS_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-DIVIDEND':
            result = self.ROE_DIVIDEND_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-MOS-DIVIDEND':
            result = self.ROE_MOS_DIVIDEND_strategy_backtest_from_1991(**condition['test_condition'])
        elif strategy == 'ROE-MOS-MULTI-YIELD':
            result = self.ROE_MOS_MULTI_YIELD_strategy_backtest_from_1991(**condition['test_condition'])
        # 计算该测试条件的总收益率
        return_list = []
        index_return_list = []
        for date, stocks in result.items():
            code_list = [item[0][0:6] for item in stocks]
            start_date = date.split(":")[1]
            end_date = date.split(":")[2]
            if 25 >= len(stocks) >= 5:  # 有效时间组
                tmp = utils.calculate_portfolio_rising_value(
                    code_list=code_list, start_date=start_date, end_date=end_date
                )
                return_list.append(round(tmp, 4))
            else:  # 无效时间组,不买入,无收益
                return_list.append(0.0000)
            tmp = utils.calculate_index_rising_value(
                index=index, start_date=start_date, end_date=end_date
            )
            index_return_list.append(round(tmp, 4))
        df = pd.DataFrame(return_list, columns=['portfolio_return'], index=result.keys())
        df['index_return'] = index_return_list
        df = df.reset_index()
        df = df.rename(columns={'index': 'date'})
        df = df.sort_values(by='date', ascending=True)
        df['portfolio_total_return'] = (df['portfolio_return'] + 1).cumprod()
        df['index_total_return'] = (df['index_return'] + 1).cumprod()
        df['portfolio_total_return'] = df['portfolio_total_return'].map(lambda x: round(x, 4))
        df['index_total_return'] = df['index_total_return'].map(lambda x: round(x, 4))
        df['strategy'] = condition['strategy']
        df['test_condition'] = f"{condition['test_condition']}"
        # 绘制收益率图
        if draw_return_figure:
            ax: Axes
            fig, ax = plt.subplots(figsize=(12, 6))
            plt.rcParams['font.sans-serif'] = ['Songti SC']
            ax.plot(df['date'], df['portfolio_total_return'], label='组合总收益')
            ax.plot(df['date'], df['index_total_return'], label=f'{index}指数总收益')
            ax.fill_between(df['date'], df['portfolio_total_return'], 0, color='gray', alpha=0.5)
            ax.set_title(f'策略组合VS{index}总收益率对比')
            ax.set_xlabel(f'{condition["strategy"].lower()} : {condition["test_condition"]}')
            ax.set_ylabel('总收益率')
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: f"{round(x*100, 2):}%"))
            ax.legend()
            ax.text(
                df['date'].iloc[-1], df['portfolio_total_return'].iloc[-1],
                f'{df["portfolio_total_return"].iloc[-1]:.2%}', ha='center', va='bottom'
            )
            ax.text(
                df['date'].iloc[-1], df['index_total_return'].iloc[-1],
                f'{df["index_total_return"].iloc[-1]:.2%}', ha='center', va='bottom'
            )
            # xticks['loc']在10个以内全部显示,11-20个显示总数的一半,21-30个显示总数的1/3,依次类推
            xticks = {'labels': [], 'loc': [], 'year': []}
            for date in df['date']:
                if date[12:16] not in xticks['year']:
                    xticks['year'].append(date[12:16])
                    xticks['labels'].append(date[12:19].replace('-', ''))  # 显示yyyymm
                    xticks['loc'].append(date)
            if len(xticks['loc']) > 10:
                step = len(xticks['loc']) // 10 + 1
                xticks['loc'] = xticks['loc'][::step]
                xticks['labels'] = xticks['labels'][::step]
            ax.set_xticks(xticks['loc'], xticks['labels'])
            ax.grid(True)
            plt.show()
        return df

    def get_conditions_from_sqlite3(self, src_sqlite3: str, src_table: str) -> List[Dict]:
        """
        从指定的sqlite3数据库中获取测试条件集.
        :param src_sqlite3: 指定的sqlite3数据库文件
        :param src_table: 指定的sqlite3数据库中的表名
        :return: 测试条件集
        """
        conditions = []  # 定义返回值
        con = sqlite3.connect(src_sqlite3)
        with con:
            sql = f""" SELECT * FROM '{src_table}' """
            df = pd.read_sql_query(sql, con)
            df = df.drop_duplicates(subset=['strategy', 'test_condition'], keep='last')
            strategy_list = df['strategy'].tolist()
            condition_list = df['test_condition'].tolist()
            for strategy, condition in zip(strategy_list, condition_list):
                tmp = {
                    'strategy': strategy,
                    'test_condition': json.loads(condition)
                }
                conditions.append(tmp)
        return conditions

    def retest_conditions_from_sqlite3(
        self,
        src_sqlite3: str,
        src_table: str,
        dest_sqlite3: str,
        dest_table: str,
        from_pos: int = 0
    ):
        """
        从指定的sqlite3数据库中获取测试条件集,重新测试后保存至指定的数据库.
        :param src_sqlite3: 指定的sqlite3数据库文件
        :param src_table: 指定的sqlite3数据库中的表名
        :param dest_sqlite3: 保存测试结果的sqlite3数据库文件
        :param dest_table: 保存测试结果的sqlite3数据库中的表名
        :param from_pos: 从指定的位置开始获取测试条件
        :return: None
        """
        conditions = self.get_conditions_from_sqlite3(
            src_sqlite3=src_sqlite3, src_table=src_table
        )
        for condition in conditions[from_pos:]:  # 重新测试
            tmp_conditons = self.get_conditions_from_sqlite3(
                src_sqlite3=dest_sqlite3, src_table=dest_table
            )
            if condition not in tmp_conditons:
                print(f'正在重新测试条件(From quant-stock): {condition}'.ljust(120, ' '))
                self.test_strategy_specific_condition(
                    condition=condition, display=False, 
                    sqlite_file=dest_sqlite3, table_name=dest_table
                )
    ###################################################################################################
    # 用生产线比喻quant-stock系统的测试过程。以ROE-MOS-DIVIDEND测试流程为例。
    # 流水线传送带上一只空箱子缓慢移动,到了目标点停下,等待合适的产品(投资组合)装进来。
    # 生产线如何从原材料(数据库)中生产出产品呢(投资组合)？很简单,就是使用查询动作,找到数据库中Y2023-Y2017年每年ROE均大于
    # 目标值的记录即可。如果箱子里只有一个格子,查询到的记录放入箱子即可,如何箱子里有多个格子,每个格子里都要放入同样的查询结果。
    # 为什么有的时候箱子只有一个格子,有的时候箱子有多个格子呢?格子多少和投资组合的持有时间有关。投资组合持有时间默认为12个
    # 月,对应整个箱子。默认状态下,一个箱子只有一个格子,代表12个月份。如果将投资组合持有时间调整为6个月,那么整个车厢就有两
    # 个格子(12除以6),每个格子代表6个月,全部格子的持有时间和箱子的持有时间12个月相等。如果投资组合持有时间调整为4个月,那
    # 么整个车厢里面就有三个格子(12除以4),每个格子代表4个月,全部格子的持有时间和箱子的持有时间12个月相等。
    # 为什么初步查询后,每个格子里面要放入同样的查询结果呢?在ROE-MOD-DIVIDEND策略中,第一道查询过滤器为7年ROE均大于目标值,
    # 在使用年度ROE为原材料的情况下,查询的结果在未来的12个月是不会发生变化的,所以车厢内(覆盖12个月)全部格子里面初步查询后的
    # 投资组合都是一样的。如果我们只使用ROE策略来选股,那么每个格子(持股期间)的投资组合都是一样的,只不过每一个新的持股期开始
    # 时,需要将上期的投资组合调整成等资金权重。
    # 经过ROE过滤器筛选后,车厢每个格子(持股期间)里面都有了同样的投资组合,第二道过滤器MOS上场了。针对每一个格子,生产线会计算
    # 该格子(持股期间)起点时刻投资组合所有股票的MOS值,根据MOS的目标值过滤出符合条件的股票。
    # 经过MOS过滤器过滤后,每个格子里面的投资组合包含的股票就出现了差异。第三道过滤器DIVIDEND出场了。针对每一个格子,生产线会
    # 查询该格子(持股期间)起点时刻投资组合所有股票的DIVIDEND值,根据DIVIDEND的目标值过滤出符合条件的股票。
    # 到了这里,第一个车厢里面全部格子都放入了符合生产条件(三道过滤器)的投资组合了。在实际投资的时候,需要在每个格子(持股期间)
    # 起点时刻将该格子里面的投资组合调整成等资金权重模式。
    # 然后生产线继续缓慢移动,第二个空箱子出来了。生产线启动第一道ROE过滤器,查询Y2022-Y2016年每年ROE均大于目标值的记录,放入
    # 箱子的全部格子里。然后开始MOS过滤器、DIVIDEND过滤器过滤筛选。
    # 依次循环,直到原材料(数据库)时间轴走到尽头即可终止。
    ###################################################################################################
    @staticmethod
    def ROE_only_strategy_backtest_from_1991(
        roe_list:List=[20]*5, roe_value=None, period:int=5, holding_time:int=12, trade_month:int=6
    ) -> Dict:
        """
        带有回测功能的单一ROE选股策略, 从1991年开始回测.筛选过程中使用INDICATOR_ROE_FROM_1991数据库的年度roe数据.
        年度财务指标公布完成是4月30日,为避免信息误差的问题,以此构建组合的时间应该在第二年5月份以后,
        :param roe_list: roe筛选列表,函数按照提供的参数值对股票进行筛选.提供了这个参数,则忽略roe_value参数.列表长度等于period.
        :param roe_value: roe筛选值,函数将其转化为一个相同元素的列表,列表长度等于period.
        :param period: 筛选条件中roe数据包含的年份数. 
        :param holding_time: 持有时间,默认为12个月.
        :return:返回值为字典格式,键为时间组,标明ROE起止期间和持股, 值标明选出股票代码集合及期间内年度ROE值.
        比如'Y2023-Y2014:2024-06-01:2024-10-01': [...], 表示该时间组是以2014年-2023年ROE值为数据源,该组合的持股时间为
        2024-06-01:2024-10-01,列表内元素为选股结果,每个元素内容包括股票代码,股票名称,股票行业,以及每年的ROE值.
        """
        if roe_list and len(roe_list) != period:
            raise ValueError('roe_list列表长度应等于period')
        if not all(map(lambda x: isinstance(x, float) or isinstance(x, int), roe_list)):
            raise ValueError('roe_list列表元素应为数字')
        if not roe_list and not roe_value:
            raise ValueError('roe_list和roe_value不能同时为空')
        if not roe_list and roe_value:
            roe_list = [roe_value]*period
        if holding_time not in HOLDING_TIME:
            raise ValueError(f"持有时间参数应为{HOLDING_TIME}中的一个")

        result = {}  # 定义返回值
        con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
        with con:
            sql = f"""select * from '{ROE_TABLE}' """
            df = pd.read_sql_query(sql, con)
            columns = df.columns.tolist()
            for index, item in enumerate(columns):
                if index >= 3 and index+period <= len(columns):  # 动态构建查询范围
                    year_list = columns[index: index+period]
                    # 查询index: index+period年度均大于roe_list的股票
                    df_tmp = df[(df[year_list] >= roe_list).all(axis=1)]
                    col_tmp = columns[:3] + year_list
                    res = [tuple(x) for x in df_tmp[col_tmp].values.tolist()]
                    # 检查res股票清单是否在sw行业指数中
                    if trade_month >=10:
                        time_tail = "-" + str(trade_month) + "-" + "01"  # -11-01
                    else:
                        time_tail = "-" + "0" + str(trade_month) + "-" + "01"  # -09-01
                    first_trade_date = str(int(columns[index][1:5])+1) + time_tail
                    res = [item for item in res if sw.in_index_or_not(item[0][:6], first_trade_date)]
                    # 根据持有时间切分“箱子”, 将res赋值给每个“格子”
                    parts = 12 / holding_time
                    first_key = f"""{columns[index]}-{columns[index+period-1]}:"""  # 时间组键名第一部分
                    end_trade_date = str(int(columns[index][1:5])+2) + time_tail
                    date_range = pd.date_range(
                        first_trade_date, end_trade_date, freq=f'{holding_time}MS'
                    ).strftime('%Y-%m-%d').tolist()
                    today = datetime.datetime.now().strftime('%Y-%m-%d')
                    for item in range(int(parts)):
                        if date_range[item] > today:  # 持股起点还未到，取消该时间组
                            break
                        elif date_range[item+1] > today:  # 持股终点还未到，以今天为终点
                            second_key = f"{date_range[item]}:{today}"
                        else:
                            second_key = f"{date_range[item]}:{date_range[item+1]}"
                        time_key = first_key + second_key
                        result[time_key] = res
        return result

    def ROE_DIVIDEND_strategy_backtest_from_1991(
        self, 
        roe_list: List, 
        period: int, 
        dividend: float,
        holding_time: int = 12,
        trade_month: int = 6
    ) -> Dict:
        """
        ROE+股息率选股策略, 从1991年开始回测.筛选过程中使用INDICATOR_ROE_FROM_1991数据库的年度roe数据.
        :param roe_list: roe筛选列表,函数按照提供的参数值对股票进行筛选.提供了这个参数,则忽略roe_value参数.列表长度等于period.
        :param period: 筛选条件中roe数据包含的年份数.
        :param dividend: 股息率筛选值,在筛选出的股票中再次筛选,筛选条件为股息率大于等于dividend.
        :param holding_time: 持有时间,默认为12个月.
        :return: 返回值为字典格式。字典键为时间组,标明ROE起止期间及持股期间, 值标明选出股票代码集合及期间内年度ROE值.
        """
        if roe_list and len(roe_list) != period:
            raise Exception('roe_list列表长度和period不相等')
        if not all(map(lambda x: isinstance(x, float) or isinstance(x, int), roe_list)):
            raise Exception('roe_list列表元素应为浮点数或者整数')
        if dividend < 0:
            dividend = 0
        if holding_time not in HOLDING_TIME:
            raise Exception(f"持有时间参数应为{HOLDING_TIME}中的一个")

        result = {}  # 定义返回值
        tmp_result = self.ROE_only_strategy_backtest_from_1991(
            roe_list=roe_list, period=period, holding_time=holding_time, trade_month=trade_month
        )
        for date, stocks in tmp_result.items():  # 股息率筛选
            tmp_date = date.split(':')[1]  # 持股期间的起点
            tmp_stocks = []  # 保存筛选结果
            for stock in stocks: 
                dv_ttm = utils.get_indicator_in_trade_record(stock[0][0:6], tmp_date, 'dv_ttm')
                dv_ratio = utils.get_indicator_in_trade_record(stock[0][0:6], tmp_date, 'dv_ratio')
                if dv_ratio >= dividend:
                    stock = stock + (dv_ttm, dv_ratio)
                    tmp_stocks.append(stock)
            result[date] = tmp_stocks
        return result

    def ROE_MOS_strategy_backtest_from_1991(
        self, roe_list: List, mos_range: List, holding_time: int = 12, trade_month: int = 6
    ) -> Dict:
        """
        本策略在ROE_only的基础上,对每一时间组的测试结果再通过MOS_7筛选一次.
        roe_list和period: 含义和使用方法和ROE_only_strategy_backtest_from_1991方法相同.
        mos_range: mos_7筛选条件列表,长度为2,元素类型为整数或者浮点数.
        holding_time: 持有时间,默认为12个月.
        :return: 返回值为字典,含义和ROE_only_strategy_backtest_from_1991方法相同.
        返回值为字典,含义和ROE_only_strategy_backtest_from_1991方法相同.
        """
        if len(roe_list) != 7:
            raise Exception('roe_list参数年份数应为7年')
        if len(mos_range) != 2:  # mos 上下限区间值
            raise Exception('mos筛选区间列表应包含两个元素')
        if not all(map(lambda x: isinstance(x, int) or isinstance(x, float), roe_list)):
            raise Exception('roe_list列表元素应为浮点数或者整数')
        if not all(map(lambda x: isinstance(x, int) or isinstance(x, float), mos_range)):
            raise Exception('mos筛选区间列表元素应为浮点数或者整数')

        tmp_result = self.ROE_only_strategy_backtest_from_1991(
            roe_list=roe_list, period=7, holding_time=holding_time, trade_month=trade_month
        )
        result = {date: item for date, item in tmp_result.items() if int(date[7:11]) >= 1999}  # 定义返回值
        for date, stocks in result.items():
            tmp_date = date.split(':')[1]  # 持股期间的起点
            tmp_stocks = []
            for stock in stocks:
                mos_7 = utils.calculate_MOS_7_from_2006(code=stock[0][0:6], date=tmp_date)
                if mos_range[1] >= mos_7 >= mos_range[0]:
                    stock = stock + (mos_7,)
                    tmp_stocks.append(stock)
            result[date] = tmp_stocks
        return result

    def ROE_MOS_DIVIDEND_strategy_backtest_from_1991(
        self, 
        roe_list: List, 
        mos_range: List, 
        dividend: float,
        holding_time: int = 12,
        trade_month: int = 6
    ) -> Dict:
        """
        本策略在ROE_MOS的基础上,对每一时间组的测试结果再通过股息率筛选一次.
        :param roe_list: 含义和使用方法和ROE_only_strategy_backtest_from_1991方法相同.
        :param mos_range: 含义和使用方法和ROE_MOS_strategy_backtest_from_1991方法相同.
        :param dividend: 股息率筛选值,在筛选出的股票中再次筛选,筛选条件为股息率大于等于dividend.
        :param holding_time: 持有时间,默认为12个月.
        :return: 返回值为字典,含义和ROE_only_strategy_backtest_from_1991方法相同.
        """
        if len(roe_list) != 7:
            raise Exception('roe_list参数年份数应为7年')
        if dividend < 0:
            dividend = 0

        result = {}  # 定义返回值
        tmp_result = self.ROE_MOS_strategy_backtest_from_1991(
            roe_list=roe_list, mos_range=mos_range, holding_time=holding_time, trade_month=trade_month
        )
        for date, stocks in tmp_result.items():  # 股息率筛选
            tmp_date = date.split(':')[1]  # 持股期间的起点
            tmp_stocks = []
            for stock in stocks:
                dv_ttm = utils.get_indicator_in_trade_record(stock[0][0:6], tmp_date, 'dv_ttm')
                dv_ratio = utils.get_indicator_in_trade_record(stock[0][0:6], tmp_date, 'dv_ratio')
                if dv_ratio >= dividend:
                    stock = stock + (dv_ttm, dv_ratio)
                    tmp_stocks.append(stock)
            result[date] = tmp_stocks
        return result

    def ROE_MOS_MULTI_YIELD_strategy_backtest_from_1991(
        self,
        roe_list: List,
        mos_range: List,
        multi_value: float,
        holding_time: int = 12,
        trade_month: int = 6
    ) -> Dict:
        """
        本策略和ROE_MOS_DIVIDEND类似,只是用每个交易日10年国债利率的倍数替代固定股息率.
        :param roe_list: 含义和使用方法和ROE_only_strategy_backtest_from_1991方法相同.
        :param mos_range: 含义和使用方法和ROE_MOS_strategy_backtest_from_1991方法相同.
        :param multi_value: 当期10年国债利率的倍数.
        :param holding_time: 持有时间,默认为12个月.
        :return: 返回值为字典,含义和ROE_only_strategy_backtest_from_1991方法相同.
        """
        if len(roe_list) != 7:
            raise Exception('roe_list参数年份数应为7年')

        result = {}  # 定义返回值
        tmp_result = self.ROE_MOS_strategy_backtest_from_1991(
            roe_list=roe_list, mos_range=mos_range, holding_time=holding_time, trade_month=trade_month
        )
        for date, stocks in tmp_result.items():  # 股息率筛选
            trade_date = date.split(':')[1]  # 持股期间的起点
            row = utils.find_closest_row_in_curve_table(trade_date)
            yield_10 = row["value1"].values[0]
            multi_yield = yield_10 * multi_value  # 当期10年国债利率的倍数
            tmp_stocks = []
            for stock in stocks:
                dv_ttm = utils.get_indicator_in_trade_record(stock[0][0:6], trade_date, 'dv_ttm')
                dv_ratio = utils.get_indicator_in_trade_record(stock[0][0:6], trade_date, 'dv_ratio')
                if dv_ratio >= multi_yield:
                    stock = stock + (dv_ttm, dv_ratio, multi_yield)
                    tmp_stocks.append(stock)
            result[date] = tmp_stocks
        return result
    
    @staticmethod
    def select_portfolio_conditions_by_rate(
        table_name,
        sqlite_name = TEST_CONDITION_SQLITE3, 
        valid_percent = 0.33, 
        basic_ratio = 0.75, 
        inner_rate = 0.25,
        down_max = -0.30, 
    ) -> Union[pd.DataFrame, None]:
        """
        通过指标值获取符合条件的测试条件集,构建投资组合.
        :param table_name: sqlite3数据库表名
        :param sqlite_name: sqlite3数据库文件名
        :param valid_percent: 最低有效时间组占比
        :param basic_ratio: 最低对000300的胜率
        :param inner_rate: 最低内在收益率
        :param down_max: 最大回撤
        :return: 符合条件的测试条件集
        """
        con = sqlite3.connect(sqlite_name)
        with con:
            sql = f"""
                SELECT * FROM '{table_name}' 
                WHERE valid_percent >= {valid_percent} 
                AND basic_ratio >= {basic_ratio} 
                AND inner_rate >= {inner_rate}
                AND down_max >= {down_max}            
            """
            df = pd.read_sql_query(sql, con)
            if df.empty:
                return

            for index, row in df.iterrows():
                condition = json.loads(row['test_condition'])
                # 添加roe辅助列以便排序分组
                if row['strategy'] == "ROE":
                    df.loc[index, 'roe'] = condition['roe_value']*condition['period']
                elif row['strategy'] in ["ROE-MOS", "ROE-DIVIDEND", "ROE-MOS-DIVIDEND", "ROE-MOS-MULTI-YIELD"]:
                    df.loc[index, 'roe'] = condition['roe_list'][0]
                else:
                    ...
            # 按照strategy和roe进行分组,按照inner_rate进行排序
            df = df.groupby(['strategy', 'roe']).apply\
                (lambda x: x.sort_values(by='inner_rate', ascending=False)).reset_index(drop=True)
        return df

    def select_portfolio_conditions_by_percentile(
        self,
        table_name,
        sqlite_name=TEST_CONDITION_SQLITE3,
        valid_percentile=50,
        basic_ratio_percentile=50,
        inner_rate_percentile=50,
        down_max_percentile=50,
    ) -> Union[pd.DataFrame, None]:
        """
        通过指标百分位数获取符合条件的测试条件集,构建投资组合.
        :param table_name: sqlite3数据库表名
        :param sqlite_name: sqlite3数据库文件名
        :param valid_percentile: 最低有效时间组占比的百分位数
        :param basic_ratio_percentile: 最低对000300的胜率的百分位数
        :param inner_rate_percentile: 最低内在收益率的百分位数
        :param down_max_percentile: 最大回撤的百分位数
        :return: 符合条件的测试条件集
        NOTE:
        本函数通过调用self.select_portfolio_conditions_by_rate函数实现.
        四个指标百分位参数均位于0-100之间,如输入50,则表示获取中位数.
        """
        con = sqlite3.connect(sqlite_name)
        with con:
            sql = f"""
                SELECT * FROM '{table_name}' 
            """
            df = pd.read_sql_query(sql, con)
            if df.empty:
                return

        valid_percent = np.percentile(df['valid_percent'], valid_percentile)
        basic_ratio = np.percentile(df['basic_ratio'], basic_ratio_percentile)
        inner_rate = np.percentile(df['inner_rate'], inner_rate_percentile)
        down_max = np.percentile(df['down_max'], down_max_percentile)
        res = self.select_portfolio_conditions_by_rate(
            valid_percent=valid_percent,
            basic_ratio=basic_ratio,
            inner_rate=inner_rate,
            sqlite_name=sqlite_name,
            table_name=table_name
        )
        return res

    @staticmethod
    def comprehensive_sorting_test_condition_sqlite3(
        table_name, sqlite_name, riskmode=0
    ) -> Union[pd.DataFrame, None]:
        """ 
        综合排序测试条件数据库,按照综合得分进行排序.排序方法如下:
        以down_max inner_rate basic_ratio字段为排序基础
        对down_max进行降序排列,排第一位的行赋值为1,第二位赋值为2,以此类推
        对inner_rate进行降序排列,排第一位的行赋值为1,第二位赋值为2,以此类推
        对basic_ratio进行降序排列,排第一位的行赋值为1,第二位赋值为2,以此类推
        对上述三个字段的排名求和,得到一个新的字段,按照新字段进行升序排列(得分低者为优先选项).
        :param table_name: sqlite3数据库表名
        :param sqlite_name: sqlite3数据库文件名
        :param riskmode: 默认为0,风险优先模式,1为收益优先模式,2为均衡模式.
            在风险优先模式下,down_max的权重为2,inner_rate的权重为1.5, basic_ratio的权重为1
            在收益优先模式下,down_max的权重为1.5,inner_rate的权重为2, basic_ratio的权重为1
            在均衡模式下,down_max的权重为1,inner_rate的权重为1, basic_ratio的权重为1
        :return: 综合排序后的条件集(strategy test_conditon valid_groups_keys 
        basic_ratio inner_rate down_max sum_rank date)
        """
        if not os.path.exists(sqlite_name):
            return
        con = sqlite3.connect(sqlite_name)
        with con:
            sql = f"""
                SELECT * FROM '{table_name}' 
            """
            df = pd.read_sql_query(sql, con)
            if df.empty:
                return
            
            # 根据riskmode设置权重
            if riskmode == 0:
                risk_w, return_w, basic_w = (2, 1.5, 1)
            elif riskmode == 1:
                risk_w, return_w, basic_w = (1.5, 2, 1)
            elif riskmode == 2:
                risk_w, return_w, basic_w = (1, 1, 1)
            df['down_max_rank'] = df['down_max'].rank(ascending=False)
            df['inner_rate_rank'] = df['inner_rate'].rank(ascending=False)
            df['basic_ratio_rank'] = df['basic_ratio'].rank(ascending=False)
            df['sum_rank'] = df['down_max_rank']*risk_w + df['inner_rate_rank']*return_w + df['basic_ratio_rank']*basic_w
            df = df.sort_values(by='sum_rank', ascending=True).reset_index(drop=True)
            df =  df[['strategy', 'test_condition', 'valid_groups_keys', 'basic_ratio', 'inner_rate', 
                'down_max', 'sum_rank', 'date']]
            return df

if __name__ == "__main__":
    stockbacktest = Strategy()
    while True:
        print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+')
        print('+                                                                     +')
        print('+ ROE ROE-DIVIDEND ROE-MOS ROE-MOS-DIVIDEND ROE-MOS-MULTI-YIELD QUIT  +')
        print('+                                                                     +')
        print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+')
        msg = input('>>>> 请选择操作提示 <<<< ').strip()
        if msg.upper() == 'ROE':
            while True:
                try:
                    roe_value = float(input('>>>> 请输入roe筛选值(数字型, 999999重新选择策略) <<<< '))
                    break
                except:
                    ...
            if roe_value == 999999:
                continue
            while True:
                try:
                    period = int(input('>>>> 请输入时间跨度(整数型5-10之间, 999999重新选择策略) <<<< '))
                    if 10 >= period >= 5 or period == 999999:
                        break
                except:
                    ...
            if period == 999999:  # 双点退出
                continue
            roe_list = [roe_value]*period
            while True:
                try:
                    holding_time = int(input(f'>>>> 请输入持有时间({HOLDING_TIME}, 999999重新选择策略) <<<< '))
                    if holding_time in HOLDING_TIME or holding_time == 999999:
                        break
                except:
                    ...
            if holding_time == 999999:  # 双点退出
                continue 
            while True:
                try:
                    trade_month = int(input('>>>> 请输入交易月份(整数型5-12之间, 999999重新选择策略) <<<< '))
                    if trade_month in TRADE_MONTH or trade_month == 999999:
                        break
                except:
                    ...
            if trade_month == 999999:  # 双点退出
                continue
            condition =  {
                'strategy': 'ROE', 
                'test_condition': {
                    'roe_list': [roe_value,]*period,  
                    'roe_value': roe_value,
                    'period': period,
                    'holding_time': holding_time,
                    'trade_month': trade_month
                }
            }
            print('正在执行ROE选股策略,请稍等......')
            print('++'*50)
            tmp_res = stockbacktest.ROE_only_strategy_backtest_from_1991(
                roe_list=roe_list, period=period, holding_time=holding_time, trade_month=trade_month
                )
        elif msg.upper() == 'ROE-DIVIDEND':
            while True:
                try:
                    roe_value = float(input('>>>> 请输入roe筛选值(数字型, 999999重新选择策略) <<<< '))
                    break
                except:
                    ...
            if roe_value == 999999:
                continue
            while True:
                try:
                    period = int(input('>>>> 请输入时间跨度(整数型5-10之间, 999999重新选择策略) <<<< '))
                    if 10 >= period >= 5 or period == 999999:
                        break
                except:
                    ...
            if period == 999999:
                continue
            roe_list = [roe_value]*period
            while True:
                try:
                    dividend = float(input('>>>> 请输入股息率筛选值(数字型, 999999重新选择策略) <<<< '))
                    if dividend >= 0:
                        break
                except:
                    ...
            if dividend == 999999:
                continue
            while True:
                try:
                    holding_time = int(input(f'>>>> 请输入持有时间({HOLDING_TIME}, 999999重新选择策略) <<<< '))
                    if holding_time in HOLDING_TIME or holding_time == 999999:
                        break
                except:
                    ...
            if holding_time == 999999:  # 双点退出
                continue
            while True:
                try:
                    trade_month = int(input('>>>> 请输入交易月份(整数型5-12之间, 999999重新选择策略) <<<< '))
                    if trade_month in TRADE_MONTH or trade_month == 999999:
                        break
                except:
                    ...
            if trade_month == 999999:  # 双点退出
                continue
            condition =  {
                'strategy': 'ROE-DIVIDEND',
                'test_condition': {
                    'roe_list': roe_list,
                    'period': period,
                    'dividend': dividend,
                    'holding_time': holding_time,
                    'trade_month': trade_month,
                }
            }
            print('正在执行ROE-DIVIDEND选股策略,请稍等......')
            print('++'*50)
            tmp_res = stockbacktest.ROE_DIVIDEND_strategy_backtest_from_1991(
                roe_list=roe_list, period=period, dividend=dividend, holding_time=holding_time, trade_month=trade_month
                )
        elif msg.upper() == 'ROE-MOS':
            while True:
                try:
                    roe_value = float(input('>>>> 请输入roe筛选值(数字型, 999999重新选择策略) <<<< '))
                    break
                except:
                    ...
            if roe_value == 999999:
                continue
            roe_list = [roe_value] * 7
            while True:
                mos_tmp = input('>>>> 请输入MOS筛选值上下限(a,b形式,ab均为数字型, 999999重新选择策略) <<<< ')
                if mos_tmp == '999999':
                    break
                mos_list = mos_tmp.split(',')
                try:
                    a = float(mos_list[0])
                    b = float(mos_list[1])
                    mos_range = [a, b]
                    if -1 <= a <= b <= 1:
                        break
                except:
                    ...
            if mos_tmp == '999999':  # 双点退出
                continue
            while True:
                try:
                    holding_time = int(input(f'>>>> 请输入持有时间({HOLDING_TIME}, 999999重新选择策略) <<<< '))
                    if holding_time in HOLDING_TIME or holding_time == 999999:
                        break
                except:
                    ...
            if holding_time == 999999:  # 双点退出
                continue
            while True:
                try:
                    trade_month = int(input('>>>> 请输入交易月份(整数型5-12之间, 999999重新选择策略) <<<< '))
                    if trade_month in TRADE_MONTH or trade_month == 999999:
                        break
                except:
                    ...
            if trade_month == 999999:  # 双点退出
                continue
            condition =  {
                'strategy': 'ROE-MOS', 
                'test_condition': {
                    'roe_list': [roe_value]*7, 
                    'mos_range': mos_range,
                    'holding_time': holding_time,
                    'trade_month': trade_month
                }
            }
            print('正在执行ROE-MOS选股策略,请稍等......')
            print('++'*50)
            tmp_res = stockbacktest.ROE_MOS_strategy_backtest_from_1991(
                roe_list=roe_list, mos_range=mos_range, holding_time=holding_time, trade_month=trade_month
                )
        elif msg.upper() == 'ROE-MOS-DIVIDEND':
            while True:
                try:
                    roe_value = float(input('>>>> 请输入roe筛选值(数字型, 999999重新选择策略) <<<< '))
                    break
                except:
                    ...
            if roe_value == 999999:
                continue
            roe_list = [roe_value] * 7
            while True:
                mos_tmp = input('>>>> 请输入MOS筛选值上下限(a,b形式,ab均为数字型, 999999重新选择策略) <<<< ')
                if mos_tmp == '999999':
                    break
                mos_list = mos_tmp.split(',')
                try:
                    a = float(mos_list[0])
                    b = float(mos_list[1])
                    mos_range = [a, b]
                    if -1 <= a <= b <= 1:
                        break
                except:
                    ...
            if mos_tmp == '999999':  # 双点退出
                continue
            while True:
                try:
                    dividend = float(input('>>>> 请输入股息率筛选值(数字型, 999999重新选择策略) <<<< '))
                    if dividend >= 0:
                        break
                except:
                    ...
            if dividend == 999999:
                continue
            while True:
                try:
                    holding_time = int(input(f'>>>> 请输入持有时间({HOLDING_TIME}, 999999重新选择策略) <<<< '))
                    if holding_time in HOLDING_TIME or holding_time == 999999:
                        break
                except:
                    ...
            if holding_time == 999999:  # 双点退出
                continue
            while True:
                try:
                    trade_month = int(input('>>>> 请输入交易月份(整数型5-12之间, 999999重新选择策略) <<<< '))
                    if trade_month in TRADE_MONTH or trade_month == 999999:
                        break
                except:
                    ...
            if trade_month == 999999:  # 双点退出
                continue
            condition =  {
                'strategy': 'ROE-MOS-DIVIDEND',
                'test_condition': {
                    'roe_list': [roe_value]*7,
                    'mos_range': mos_range,
                    'dividend': dividend,
                    'holding_time': holding_time,
                    'trade_month': trade_month
                }
            }
            print('正在执行ROE-MOS-DIVIDEND选股策略,请稍等......')
            print('++'*50)
            tmp_res = stockbacktest.ROE_MOS_DIVIDEND_strategy_backtest_from_1991(
                roe_list=roe_list, mos_range=mos_range, dividend=dividend, holding_time=holding_time, trade_month=trade_month
                )
        elif msg.upper() == 'ROE-MOS-MULTI-YIELD':
            while True:
                try:
                    roe_value = float(input('>>>> 请输入roe筛选值(数字型, 999999重新选择策略) <<<< '))
                    break
                except:
                    ...
            if roe_value == 999999:
                continue
            roe_list = [roe_value] * 7
            while True:
                mos_tmp = input('>>>> 请输入MOS筛选值上下限(a,b形式,ab均为数字型, 999999重新选择策略) <<<< ')
                if mos_tmp == '999999':
                    break
                mos_list = mos_tmp.split(',')
                try:
                    a = float(mos_list[0])
                    b = float(mos_list[1])
                    mos_range = [a, b]
                    if -1 <= a <= b <= 1:
                        break
                except:
                    ...
            if mos_tmp == '999999':  # 双点退出
                continue
            while True:
                try:
                    multi_value = float(input('>>>> 请输入国债收益率倍数(数字型, 999999重新选择策略) <<<< '))
                    if multi_value >= 0:
                        break
                except:
                    ...
            if multi_value == 999999:
                continue
            while True:
                try:
                    holding_time = int(input(f'>>>> 请输入持有时间({HOLDING_TIME}, 999999重新选择策略) <<<< '))
                    if holding_time in HOLDING_TIME or holding_time == 999999:
                        break
                except:
                    ...
            if holding_time == 999999:  # 双点退出
                continue
            while True:
                try:
                    trade_month = int(input('>>>> 请输入交易月份(整数型5-12之间, 999999重新选择策略) <<<< '))
                    if trade_month in TRADE_MONTH or trade_month == 999999:
                        break
                except:
                    ...
            if trade_month == 999999:  # 双点退出
                continue
            condition =  {
                'strategy': 'ROE-MOS-MULTI-YIELD',
                'test_condition': {
                    'roe_list': [roe_value]*7,
                    'mos_range': mos_range,
                    'multi_value': multi_value,
                    'holding_time': holding_time,
                    'trade_month': trade_month
                }
            }
            print('正在执行ROE-MOS-MULTI-YIELD选股策略,请稍等......')
            print('++'*50)
            tmp_res = stockbacktest.ROE_MOS_MULTI_YIELD_strategy_backtest_from_1991(
                roe_list=roe_list, mos_range=mos_range, multi_value=multi_value, holding_time=holding_time, trade_month=trade_month
                )
        elif msg.upper() == 'QUIT':
            break
        else:
            continue

        # 显示细节
        for key, value in sorted(tmp_res.items(), key=lambda x: x[0]):
            print(key, '投资组合', f'共{len(value)}', '只股票')
            start_end = key.split(':')[0]  # 选股时间段
            start_year = int(start_end.split('-')[0][1:5])  # 选股起始年份
            end_year = int(start_end.split('-')[1][1:5])  # 选股结束年份
            columns = list(range(start_year, end_year-1, -1))
            columns = [f"Y{item}" for item in columns]
            columns = ["股票代码", "股票名称", "申万行业"] + columns
            if msg.upper() == "ROE-DIVIDEND":
                columns.append("DV_ttm")
                columns.append("DV_ratio")
            elif msg.upper() == "ROE-MOS":
                columns.append("MOS7")
            elif msg.upper() == "ROE-MOS-DIVIDEND":
                columns.append("MOS7")
                columns.append("DV_ttm")
                columns.append("DV_ratio")
            elif msg.upper() == "ROE-MOS-MULTI-YIELD":
                columns.append("MOS7")
                columns.append("DV_ttm")
                columns.append("DV_ratio")
                columns.append("M_Yield")
            else:
                pass
            df = pd.DataFrame(value, columns=columns)
            if not df.empty:
                print(df)
            stock_codes = df['股票代码'].tolist()
            stock_codes = [item[0:6] for item in stock_codes]
            start_date = key.split(':')[1]
            end_date = key.split(':')[2]
            res = utils.calculate_portfolio_rising_value(stock_codes, start_date, end_date)
            print('该组合在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            res1 = utils.calculate_index_rising_value('000300', start_date, end_date)
            print('沪深300在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res1*100))
            if 25 >= len(stock_codes) >= 5:
                res = round(res-res1, 4)
                print('该组合相对沪深300的超额收益为{:.2f}%'.format(res*100))
            print('--'*50)
        
        strategy = condition['strategy']
        test_condition = condition['test_condition']
        if strategy in STRATEGIES:
            portfolio_test_result = stockbacktest.test_strategy_portfolio(
                strategy=strategy, result=tmp_res
            )
            evaluate_result = stockbacktest.evaluate_portfolio_effect(
                test_condition=condition, 
                test_result=tmp_res, 
                portfolio_test_result=portfolio_test_result
            )
            print(f"策略{strategy}测试结果:")
            for key, value in test_condition.items():
                if key == "holding_time":
                    print(f"{key:<20}: {value}Ms")
                elif key == "trade_month":
                    print(f"{key:<20}: {value}M")
                else:
                    print(f"{key:<20}: {value}")
            print(f"{'valid_groups':<20}: {evaluate_result['valid_groups']}")
            print(f"{'basic_ratio':<20}: {evaluate_result['basic_ratio']:.2%}")
            print(f"{'inner_rate':<20}: {evaluate_result['inner_rate']:.2%}")
            print(f"{'down_max':<20}: {evaluate_result['down_max']:.2%}")
            print(f"{'highest_rate':<20}: {evaluate_result['highest_rate']:.2%}")
            print(f"{'avg_rate':<20}: {evaluate_result['avg_rate']:.2%}")
            print(f"{'std_rate':<20}: {evaluate_result['std_rate']:.2%}")
            print(f"{'delta_rate_min':<20}: {min(evaluate_result['delta_rate']):.2%}")
            print(f"{'delta_rate_max':<20}: {max(evaluate_result['delta_rate']):.2%}")
            print(f"{'delta_rate_last':<20}: {evaluate_result['delta_rate'][-1]:.2%}")

            # 计算持仓比例
            holding_time = test_condition['holding_time']
            last_year_rate_list = evaluate_result['rate_list'][-12//holding_time:]
            last_year_rate_list = [item+1 for item in last_year_rate_list]
            from functools import reduce
            last_year_rate = reduce(lambda x, y: x*y, last_year_rate_list)-1
            holdding_percent = utils.calculate_portfolio_holding_percent(
                last_year_rate=last_year_rate,
                inner_rate=evaluate_result['inner_rate'],
                last_series_rate=evaluate_result['rate_list'][-1],
                avg_rate=evaluate_result['avg_rate'],
                std_rate=evaluate_result['std_rate']
            )
            print(f"{'last_year_rate':<20}: {last_year_rate:.2%}")
            print(f"{'last_series_rate':<20}: {evaluate_result['rate_list'][-1]:.2%}")
            print(f"{'suggested_percent':<20}: {holdding_percent:.2%}")
            print('++'*50)