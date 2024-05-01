import sqlite3
import time
import random
import datetime
import json
import pandas as pd
import numpy as np
from typing import List, Dict, Union
import utils
import tsswindustry as sw
from path import INDICATOR_ROE_FROM_1991, ROE_TABLE, TEST_CONDITION_SQLITE3

class Strategy:
    def __init__(self):
        pass

    @staticmethod
    def generate_ROE_test_conditions(strategy: str, items: int = 10, mos_step: float = 0.25) -> List[Dict]:
        """
        生成测试条件列表,用于测试策略
        :param strategy: 策略名称, 例如: 'roe', 'roe-dividend', 'roe-mos', 'roe-mos-dividend'
        :param items: 生成的测试条件数量, 例如: 10
        :param mos_step: 生成的测试条件中MOS_RANGE的最大步长
        :return: 测试条件列表
        NOTE:
        roe_value取值范围为[0, 40], 超过win-stock系统预设值[10, 40]
        period取值范围为[5, 10], 和win-stock系统预设值[5, 10]相同
        mos_range取值范围为[-1, 1], 超过win-stock系统预设值[0.2, 1],但是step限制为不大于0.25
        dividend取值范围为[0, 10], 和win-stock系统预设值[0, 10]相同
        """
        if strategy.upper() not in ['ROE', 'ROE-DIVIDEND', 'ROE-MOS', 'ROE-MOS-DIVIDEND']:
            raise ValueError('请检查策略名称是否正确(ROE, ROE-DIVIDEND, ROE-MOS, ROE-MOS-DIVIDEND)')

        condition = []  # 定义返回值
        if strategy.upper() == 'ROE':
            for item in range(items):
                roe_value = random.randint(10, 40)
                period = random.randint(5, 10)
                tmp =  {
                    'strategy': strategy.upper(), 
                    'test_condition': {
                        'roe_list': [],  
                        'roe_value': roe_value, 
                        'period': period
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == 'ROE-MOS':
            for item in range(items):
                roe_value = random.randint(10, 40)
                mos_range = [round(random.uniform(-1, 1), 4) for _ in range(2)]
                mos_range.sort()
                if mos_range[1] - mos_range[0] > mos_step:
                    mos_range[1] = round(mos_range[0] + mos_step, 4)  # 限制mos_range的最大步长
                tmp =  {
                    'strategy': strategy.upper(), 
                    'test_condition': {
                        'roe_list': [roe_value]*7, 
                        'mos_range': mos_range
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == 'ROE-DIVIDEND':
            for item in range(items):
                roe_value = random.randint(10, 40)
                period = random.randint(5, 10)
                roe_list = [roe_value]*period
                dividend = random.randint(0, 10)
                tmp =  {
                    'strategy': strategy.upper(),
                    'test_condition': {
                        'roe_list': roe_list,
                        'period': period,
                        'dividend': dividend
                    }
                }
                condition.append(tmp)
        elif strategy.upper() == 'ROE-MOS-DIVIDEND':
            for item in range(items):
                roe_value = random.randint(10, 40)
                mos_range = [round(random.uniform(-1, 1), 4) for _ in range(2)]
                mos_range.sort()
                if mos_range[1] - mos_range[0] > mos_step:
                    mos_range[1] = round(mos_range[0] + mos_step, 4)  # 限制mos_range的最大步长
                dividend = random.randint(0, 10)
                tmp =  {
                    'strategy': strategy.upper(),
                    'test_condition': {
                        'roe_list': [roe_value]*7,
                        'mos_range': mos_range,
                        'dividend': dividend
                    }
                }
                condition.append(tmp)
        return condition

    def display_result_of_strategy(self, strategy: Dict):
        """
        显示选股策略的具体结果.策略使用self.get_conditions_from_sqlite3获取.
        结构如下:{'strategy': '...', 'test_condition': {...}}
        :param strategy: roe、roe-dividend、roe-mos、roe-mos-dividend策略.
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
        for key, value in sorted(res.items(), key=lambda x: x[0]):
            print(key, '投资组合', f'共{len(value)}', '只股票')
            stock_codes = []
            for item in value:
                print(item)
                stock_codes.append(item[0][0:6])
            start_date = str(int(key[1:5])+1)+'-06-01'
            end_date = str(int(key[1:5])+2)+'-06-01'
            res = utils.calculate_portfolio_rising_value(stock_codes, start_date, end_date)
            print('该组合在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            res = utils.calculate_index_rising_value('000300', start_date, end_date)
            print('沪深300在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            print('--'*50)

    def test_strategy_portfolio(
        self, 
        strategy: str, 
        result: Dict, 
        index_list: List = ['000300', '399006', '000905'], 
        max_nembers: int = 15
    ) -> Union[str, Dict]:
        """
        对选股策略的测试结果进行初步测试,生成该测试结果每个时间组股票组合的收益率和指定指数的收益率,即测试结果和指数的收益对比.
        :param strategy:选股策略名称,目前支持'ROE','PE-PB','ROE-DIVIDEND','ROE-MOS', 'ROE-MOS-DIVIDEND'.
        :param result:策略类方法的返回值,即测试条件相对应的测试结果.
        :param index_list:指定测试的指数,默认为沪深300(000300),创业板指(399006),中证500(000905),最多为3个指数.
        :param max_nembers:时间组最大平均选股数量,默认为15.
        :return:返回值为字典,键为时间组(和result参数时间组相同),值为该时间组的选股组合和指定指数的收益率.
        NOTE:
        quant-stock系统速度慢,相比win-stock而言,主打测试较小的组合.
        如果result参数时间组平均持股数量大于15,直接返回定制的测试结果
        """
        if strategy.upper() not in ['ROE', 'ROE-DIVIDEND', 'ROE-MOS', 'ROE-MOS-DIVIDEND']:
            raise ValueError('请检查策略名称是否正确(ROE, ROE-DIVIDEND, ROE-MOS, ROE-MOS-DIVIDEND)')
        if not all([item in ['000300', '399006', '000905'] for item in index_list]):
            raise ValueError('请检查指数代码是否正确')
        test_result = {date: [] for date in result.keys()}  # 定义返回值

        if sum([len(item) for item in result.values()])/len(result) > max_nembers:
            # print('测试结果股票数量过多,为减轻计算压力,返回定制的结果')
            return {date: [0, 0] for date in result.keys()}
        
        for date, stocks in sorted(result.items(), key=lambda x: x[0]):  # 对每个时间组的选股结果进行回测
            code_list = [item[0][0:6] for item in stocks]  # 不含后缀
            if strategy.upper() in ['ROE', 'ROE-PE-PB', 'ROE-MOS', 'ROE-DIVIDEND', 'ROE-MOS-DIVIDEND']:  # roe type strategy
                start_date = str(int(date[1:5])+1)+'-06-01'
                end_date = str(int(date[1:5])+2)+'-06-01'
            else:
                start_date = date
                end_date_tmp = datetime.datetime.strptime(date, '%Y-%m-%d') + datetime.timedelta(days=183)
                end_date = end_date_tmp.strftime('%Y-%m-%d')
            stock_return = utils.calculate_portfolio_rising_value(code_list, start_date, end_date)  # 获取组合的收益率
            test_result[date].append(stock_return)
            if index_list:  # 获取指数的收益率
                for index in index_list:
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
        down_max为所有有效时间组中最大回测.计算basic_ratio、inner_rate、down_max
        均使用有效时间组(valid_groups),即该时间组包含的股票数在5至25之间.
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
        evaluate_result['valid_groups'] = len(valid_groups)
        evaluate_result['valid_percent'] = round(len(valid_groups) / total_groups, 4)

        # 获取有效时间组的键名
        valid_groups_keys = list(valid_groups.keys())
        evaluate_result['valid_groups_keys'] = valid_groups_keys

        # 计算basic_ratio
        win_count = 0
        for date, stocks in valid_groups.items():
            if portfolio_test_result[date][0] > portfolio_test_result[date][1]:
                win_count += 1
        basic_ratio = win_count / len(valid_groups) if valid_groups else 0
        evaluate_result['basic_ratio'] = round(basic_ratio, 4)

        # 计算inner_rate 和 down_max
        # TODO:
        # 当时间组间隔不等于1年的情况下,是否需要调整？
        rate_list = []
        total_return = 1
        for date, stocks in valid_groups.items():
            total_return *= (1 + portfolio_test_result[date][0])
            rate_list.append(portfolio_test_result[date][0])
        inner_rate = total_return ** (1 / len(valid_groups)) - 1 if valid_groups else 0
        evaluate_result['inner_rate'] = round(inner_rate, 4)
        evaluate_result['down_max'] = round(min(rate_list), 4) if valid_groups else 0

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
        restest_or_not: bool = False,
        ):
        """
        测试回测类的闭环效果,测试对象为特定的测试条件,测试结果将保存到数据库
        :param condition: 测试条件,字典类型,结构如下:{'strategy': 'ROE', 'test_condition': {...}}
        :param table_name: 保存测试结果的sqlite3数据库中的表名
        :param sqlite_file: 保存测试结果的sqlite3数据库文件
        :param display: 是否显示中间结果
        :param restest_or_not: 是否重新测试
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
        # 如果是重新测试以前年度的全部测试条件,则删除一个时间组的测试结果，
        # 因为投资组合和指数的收益均为0，没有意义
        if restest_or_not:
            result.pop(list(result.keys())[0])
        if display:
            print('+'*120)
            print(result)
        
        # 测试该测试结果和指数的收益对比
        portfolio_test_result = self.test_strategy_portfolio(
            strategy=strategy, result=result, index_list=['000300']
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
        display: bool = False,
        restest_or_not: bool = False,
        ):
        """
        测试回测类的闭环效果,测试对象为随机生成的测试条件
        :param table_name: 保存测试结果的sqlite3数据库中的表名
        :param sqlite_file: 保存测试结果的sqlite3数据库文件
        :param times: 测试次数
        :param display: 是否显示中间结果
        :param restest_or_not: 是否重新测试
        :return: None
        """
        start = time.time()
        number = 0
        for i in range(times):
            print(f'第{i+1}轮测试......'.ljust(120, ' '))
            strategy = random.choice(['ROE-MOS', 'ROE-DIVIDEND', 'ROE', 'ROE-MOS-DIVIDEND'])
            items = random.randint(1, 5)
            mos_step = random.uniform(0.1, 0.30)
            number += items
            condition_list = self.generate_ROE_test_conditions(strategy=strategy, items=items, mos_step=mos_step)
            if display:
                print('+'*120)
                print(condition_list)
            for condition in condition_list:  # 测试
                print(f'测试条件：{condition}'.ljust(120, ' '))
                self.test_strategy_specific_condition(
                    condition=condition, display=display, 
                    sqlite_file=sqlite_file, table_name=table_name, 
                    restest_or_not=restest_or_not
                )
        end = time.time()
        print('+'*120)
        print(f'共测试{number}次，耗时{round(end-start, 4)}秒')
        print(f'平均每次测试耗时{round((end-start)/number, 4)}秒')

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
    ):
        """
        从指定的sqlite3数据库中获取测试条件集,重新测试后保存至指定的数据库.
        :param src_sqlite3: 指定的sqlite3数据库文件
        :param src_table: 指定的sqlite3数据库中的表名
        :param dest_sqlite3: 保存测试结果的sqlite3数据库文件
        :param dest_table: 保存测试结果的sqlite3数据库中的表名
        :return: None
        """
        conditions = self.get_conditions_from_sqlite3(
            src_sqlite3=src_sqlite3, src_table=src_table
        )
        for condition in conditions:  # 重新测试
            tmp_conditons = self.get_conditions_from_sqlite3(
                src_sqlite3=dest_sqlite3, src_table=dest_table
            )
            if condition not in tmp_conditons:
                print(f'正在重新测试条件：{condition}'.ljust(120, ' '))
                self.test_strategy_specific_condition(
                    condition=condition, display=False, 
                    sqlite_file=dest_sqlite3, table_name=dest_table,
                    restest_or_not=True
                )
    
    @staticmethod
    def ROE_only_strategy_backtest_from_1991(roe_list:List=[20]*5, roe_value=None, period:int=5) -> Dict:
        """
        带有回测功能的单一ROE选股策略, 从1991年开始回测.筛选过程中使用INDICATOR_ROE_FROM_1991数据库的年度roe数据.
        年度财务指标公布完成是4月30日,为避免信息误差的问题,以此构建组合的时间应该在第二年5月份以后,
        :param roe_list: roe筛选列表,函数按照提供的参数值对股票进行筛选.提供了这个参数,则忽略roe_value参数.列表长度等于period.
        :param roe_value: roe筛选值,函数将其转化为一个相同元素的列表,列表长度等于period.
        :param period: 筛选条件中roe数据包含的年份数. 
        :return:返回值为字典格式,键为时间组,标明ROE起止期间, 值标明选出股票代码集合及期间内年度ROE值.
        比如'Y2000-Y1994': [...], 表示该时间组选股是以1994年-2000年ROE值为筛选条件,列表内元素为选股结果,该列表是一个复合列表.
        """
        if roe_list and len(roe_list) != period:
            raise ValueError('roe_list列表长度应等于period')
        if not all(map(lambda x: isinstance(x, float) or isinstance(x, int), roe_list)):
            raise ValueError('roe_list列表元素应为数字')
        if not roe_list and not roe_value:
            raise ValueError('roe_list和roe_value不能同时为空')
        if not roe_list and roe_value:
            roe_list = [roe_value]*period
        
        result = {}  # 定义返回值
        con = sqlite3.connect(INDICATOR_ROE_FROM_1991)
        with con:
            sql = f"""select * from '{ROE_TABLE}' """
            df = pd.read_sql_query(sql, con)
            columns = df.columns
            del df

            sw_stocks = sw.get_all_stocks()
            sw_codes = [item[0] for item in sw_stocks]  # 申万行业分类股票代码集合
            for index, item in enumerate(columns):
                if index >= 3 and index+period <= len(columns):  # 动态构建查询范围
                    year_list = columns[index: index+period]
                    suffix = """stockcode, stockname, stockclass, """
                    for col, year in enumerate(year_list):
                        if col == len(year_list) -1:
                            suffix += f"""{year} """
                        else:
                            suffix += f"""{year}, """

                    sql = f"""select {suffix} from '{ROE_TABLE}' where """  # 构建查询条件
                    for col, year in enumerate(year_list):
                        if col == len(year_list) -1:
                            sql += f"""{year}>=?"""
                        else:
                            sql += f"""{year}>=? and """
                    res = con.execute(sql, tuple(roe_list)).fetchall()
                    # 检查res股票清单是否在申万行业分类取票sw_codes中
                    res = [item for item in res if item[0] in sw_codes]
                    # 检查res股票清单是否在sw行业指数中
                    trade_date = str(int(columns[index][1:5])+1)+'-06-01'
                    res = [item for item in res if sw.in_index_or_not(item[0][:6], trade_date)]
                    result[f"""{columns[index]}-{columns[index+period-1]}"""] = res
        return result

    def ROE_DIVIDEND_strategy_backtest_from_1991(
        self, 
        roe_list: List, 
        period: int, 
        dividend: float,
    ) -> Dict:
        """
        ROE+股息率选股策略, 从1991年开始回测.筛选过程中使用INDICATOR_ROE_FROM_1991数据库的年度roe数据.
        :param roe_list: roe筛选列表,函数按照提供的参数值对股票进行筛选.提供了这个参数,则忽略roe_value参数.列表长度等于period.
        :param period: 筛选条件中roe数据包含的年份数.
        :param dividend: 股息率筛选值,在筛选出的股票中再次筛选,筛选条件为股息率大于等于dividend.
        :return: 返回值为字典格式。字典键为时间组,标明ROE起止期间, 值标明选出股票代码集合及期间内年度ROE值.
        比如'Y2000-Y1994': [...], 表示该时间组选股是以1994年-2000年ROE值为筛选条件,
        列表内元素为选股结果,该列表是一个复合列表.(2023-04-24)
        """
        if roe_list and len(roe_list) != period:
            raise Exception('roe_list列表长度和period不相等')
        if not all(map(lambda x: isinstance(x, float) or isinstance(x, int), roe_list)):
            raise Exception('roe_list列表元素应为浮点数或者整数')
        if dividend < 0:
            dividend = 0

        result = {}  # 定义返回值
        tmp_result = self.ROE_only_strategy_backtest_from_1991(roe_list=roe_list, period=period)
        for date, stocks in tmp_result.items():  # 股息率筛选
            tmp_date = f"{int(date[1:5])+1}"+'-06-01'  # 目标日期行
            tmp_stocks = []  # 保存筛选结果
            for stock in stocks: 
                tmp_dividend = utils.get_indicator_in_trade_record(stock[0][0:6], tmp_date, 'dv_ttm')
                if tmp_dividend >= dividend:
                    tmp_stocks.append(stock)
            result[date] = tmp_stocks
        return result

    def ROE_MOS_strategy_backtest_from_1991(self, roe_list: List, mos_range: List) -> Dict:
        """
        本策略在ROE_only的基础上,对每一时间组的测试结果再通过MOS_7筛选一次.
        roe_list和period: 含义和使用方法和ROE_only_strategy_backtest_from_1991方法相同.
        mos_range: mos_7筛选条件列表,长度为2,元素类型为整数或者浮点数.
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

        tmp_result = self.ROE_only_strategy_backtest_from_1991(roe_list=roe_list, period=7)
        result = {date: item for date, item in tmp_result.items() if int(date[7:11]) >= 1999}  # 定义返回值（1999-2005序列）
        for date, stocks in result.items():
            tmp_date = f"{int(date[1:5])+1}"+'-06-01'
            tmp_stocks = []
            for stock in stocks:
                mos_7 = utils.calculate_MOS_7_from_2006(code=stock[0][0:6], date=tmp_date)
                if mos_range[1] >= mos_7 >= mos_range[0]:
                    tmp_stocks.append(stock)
            result[date] = tmp_stocks
        return result

    def ROE_MOS_DIVIDEND_strategy_backtest_from_1991(
        self, 
        roe_list: List, 
        mos_range: List, 
        dividend: float
    ) -> Dict:
        """
        本策略在ROE_MOS的基础上,对每一时间组的测试结果再通过股息率筛选一次.
        :param roe_list: 含义和使用方法和ROE_only_strategy_backtest_from_1991方法相同.
        :param mos_range: 含义和使用方法和ROE_MOS_strategy_backtest_from_1991方法相同.
        :param dividend: 股息率筛选值,在筛选出的股票中再次筛选,筛选条件为股息率大于等于dividend.
        :return: 返回值为字典,含义和ROE_only_strategy_backtest_from_1991方法相同.
        """
        if len(roe_list) != 7:
            raise Exception('roe_list参数年份数应为7年')
        if dividend < 0:
            dividend = 0

        result = {}  # 定义返回值
        tmp_result = self.ROE_MOS_strategy_backtest_from_1991(roe_list=roe_list, mos_range=mos_range)
        for date, stocks in tmp_result.items():  # 股息率筛选
            tmp_date = f"{int(date[1:5])+1}"+'-06-01'
            tmp_stocks = []
            for stock in stocks:
                tmp_dividend = utils.get_indicator_in_trade_record(stock[0][0:6], tmp_date, 'dv_ttm')
                if tmp_dividend >= dividend:
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
                elif row['strategy'] in ["ROE-MOS", "ROE-DIVIDEND", "ROE-MOS-DIVIDEND"]:
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
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        print('+++++++ ROE ROE-DIVIDEND ROE-MOS ROE-MOS-DIVIDEND QUIT ++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        msg = input('>>>> 请选择操作提示 <<<< ')
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
            print('正在执行ROE选股策略,请稍等......')
            print('++'*50)
            res = stockbacktest.ROE_only_strategy_backtest_from_1991(roe_list=roe_list, period=period)
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
            print('正在执行ROE-DIVIDEND选股策略,请稍等......')
            print('++'*50)
            res = stockbacktest.ROE_DIVIDEND_strategy_backtest_from_1991(roe_list=roe_list, period=period, dividend=dividend)
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
            print('正在执行ROE-MOS选股策略,请稍等......')
            print('++'*50)
            res = stockbacktest.ROE_MOS_strategy_backtest_from_1991(roe_list=roe_list, mos_range=mos_range)
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
            print('正在执行ROE-MOS-DIVIDEND选股策略,请稍等......')
            print('++'*50)
            res = stockbacktest.ROE_MOS_DIVIDEND_strategy_backtest_from_1991(roe_list=roe_list, mos_range=mos_range, dividend=dividend)
        elif msg.upper() == 'QUIT':
            break
        else:
            continue

        # 显示细节
        for key, value in sorted(res.items(), key=lambda x: x[0]):
            print(key, '投资组合', f'共{len(value)}', '只股票')
            stock_codes = []
            for item in value:
                print(item)
                stock_codes.append(item[0][0:6])
            start_date = str(int(key[1:5])+1)+'-06-01'
            end_date = str(int(key[1:5])+2)+'-06-01'
            res = utils.calculate_portfolio_rising_value(stock_codes, start_date, end_date)
            print('该组合在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            res = utils.calculate_index_rising_value('000300', start_date, end_date)
            print('沪深300在{}到{}期间的收益为{:.2f}%'.format(start_date, end_date, res*100))
            print('--'*50)

