import time
import pandas as pd
import sqlite3
from strategy import Strategy
from path import TEST_CONDITION_SQLITE3
import threading
import json

lock = threading.Lock()
# quant-stock系统所有测试条件保存在test-condition.sqlite3数据库中,
# 每个年份一个表,用于保存该年度的全部测试条件,表名格式为condition-年份.
# 每年的1-4月生成的测试条件保存在上年的表中,比如2023年1-4月生成的测试条件保存在condition-2022表中.
# 每年的5-12月生成的测试条件保存在当年的表中,比如2023年5-12月生成的测试条件保存在condition-2023表中.

def has_test_previous_year() -> bool:
    """
    判断是否已经重新测试过以前年度的测试条件.
    在test-condition.sqlite3数据库中设置一个flag表格,保存是否已经测试过以前年度的测试条件.
    该表格有两个字段:year,flag. year字段保存年份(格式为2023),flag字段保存是否已经测试过以前年度的测试条件.
    如果flag字段为Yes,则表示已经测试过以前年度的测试条件,否则为No.
    :return: True or False
    """
    con = sqlite3.connect(TEST_CONDITION_SQLITE3)
    with con:
        # 获取当年的flag值
        sql = f"""
            SELECT flag FROM flag WHERE year='{time.localtime().tm_year}'
        """
        res = con.execute(sql).fetchone()

        if res is None:
            return False
        else:
            return True if res[0] == 'Yes' else False

def auto_test():
    """
    自动测试函数.
    每年5月份建立年度表格CONDITION_TABLE,并重新测试以前年度的全部测试条件,
    完成重新测试动作以后,本函数开始随机测试新生成的测试条件.
    无限循环流程,断线后自动重连.
    NOTE:
    在建立年度表格CONDITION_TABLE前,最好暂停该函数.等年度ROE表、交易记录文件
    国债收益率文件以及其他相关文件都更新以后,再启动该函数.
    """
    now = time.localtime()
    table_name = f'condition-{now.tm_year}' if now.tm_mon >= 5 else f'condition-{now.tm_year-1}'
    con = sqlite3.connect(TEST_CONDITION_SQLITE3)
    with con:
        sql = """
            CREATE TABLE IF NOT EXISTS flag
            (
                year TEXT,
                flag TEXT,
                PRIMARY KEY(year)
            )
        """
        con.execute(sql)    
    case = Strategy()
    while True:
        # 动态获取年度表格名,必须要重新定义,否则会出现表格名不更新的情况
        now = time.localtime()
        table_name = f'condition-{now.tm_year}' if now.tm_mon >= 5 else f'condition-{now.tm_year-1}'
        # 第一步 重新检测以前年度的全部测试条件
        con = sqlite3.connect(TEST_CONDITION_SQLITE3)
        with con:
            if now.tm_mon in [1, 2, 3, 4]:
                # 设置当年的flag值为No
                sql = f"""
                    INSERT OR IGNORE INTO flag (year, flag) VALUES (?, ?)
                """
                params = (time.localtime().tm_year, 'No')
                con.execute(sql, params)
            else:  # now.tm_mon in [5, 6, 7, 8, 9, 10, 11, 12]:
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
                con.execute(sql)
                # 从以前年度表格中获取测试条件集合,执行retest_conditions_from_sqlite3函数
                if has_test_previous_year() is False:
                    # 获取以前年度表格的表名(不包含当年的表)
                    df = pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"', con)
                    all_table_names = df['name'].tolist()
                    prev_table_names = [
                        table for table in all_table_names if str(now.tm_year) not in table and table != 'flag'
                    ]
                    for prev_table in prev_table_names:
                        case.retest_conditions_from_sqlite3(
                            src_sqlite3=TEST_CONDITION_SQLITE3, 
                            src_table=prev_table, 
                            dest_sqlite3=TEST_CONDITION_SQLITE3, 
                            dest_table=table_name
                        )
                    # 获取table_name表格中的测试条件,遍历prev_table_names,对相同的测试条件,
                    # 则将table_name表格中的date字段替换为prev_table表格中的date字段
                    # 保留全部入选测试条件原始日期
                    retested_conditions = case.get_conditions_from_sqlite3(
                        src_sqlite3=TEST_CONDITION_SQLITE3, src_table=table_name
                    )
                    for condition in retested_conditions:
                        for prev_table in prev_table_names:
                            sql = f""" 
                                SELECT date FROM '{prev_table}' WHERE strategy=? AND test_condition=?
                            """
                            params = (condition["strategy"], json.dumps(condition["test_condition"]))
                            date = con.execute(sql, params).fetchone()
                            if date is not None:
                                sql = f"""
                                    UPDATE '{table_name}' SET date='{date[0]}' WHERE strategy=?
                                    AND test_condition=?
                                """
                                params = (condition["strategy"], json.dumps(condition["test_condition"]))
                                con.execute(sql, params)
                                break
                    # 更新当年的flag值为Yes
                    sql = f"""
                        UPDATE flag SET flag='Yes' WHERE year='{time.localtime().tm_year}'
                    """
                    con.execute(sql)
        # 第二步 随机测试新生成的测试条件
        with lock:
            try:
                case.test_strategy_random_condition(
                    sqlite_file=TEST_CONDITION_SQLITE3, 
                    table_name=table_name
                )
                time.sleep(10)
            except Exception as e:
                print(f"auto_test()函数出现异常:{e}")
                time.sleep(60)
        
if __name__ == '__main__':
    auto_test()