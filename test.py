import time
import pandas as pd
import sqlite3
from strategy import Strategy
from path import TEST_CONDITION_SQLITE3

# win-stock系统所有测试条件保存在test-condition.sqlite3数据库中,每个年份一个表,用于保存该年度的全部测试条件,表名格式为condition-年份.
# 每年的1-5月生成的测试条件保存在上年的表中,比如2023年1-5月生成的测试条件保存在condition-2022表中.
# 每年的6-12月生成的测试条件保存在当年的表中,比如2023年6-12月生成的测试条件保存在condition-2023表中.

# 每天早上6点至下午11点,执行回测函数.


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
        sql = """
            CREATE TABLE IF NOT EXISTS flag
            (
                year TEXT,
                flag TEXT,
                PRIMARY KEY(year)
            )
        """
        con.execute(sql)

        # 获取当年的flag值
        sql = f"""
            SELECT flag FROM flag WHERE year='{time.localtime().tm_year}'
        """
        res = con.execute(sql).fetchone()

        if res is None:
            return False
        else:
            return True if res[0] == 'Yes' else False


if __name__ == '__main__':
    case = Strategy()
    while True:
        now = time.localtime()
        table_name = f'condition-{now.tm_year}' if now.tm_mon >= 5 else f'condition-{now.tm_year-1}'
        if now.tm_mon in [1, 2, 3, 4, 5]:
            # 设置当年的flag值为No
            con = sqlite3.connect(TEST_CONDITION_SQLITE3)
            with con:
                sql = f"""
                    INSERT OR IGNORE INTO flag (year, flag) VALUES ('{time.localtime().tm_year}', 'No')
                """
                con.execute(sql)
        elif now.tm_mon in [6, 7, 8, 9, 10, 11, 12]:
            # 创建年度表格
            con = sqlite3.connect(TEST_CONDITION_SQLITE3)
            with con:
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
                        score REAL, 
                        date TEXT,
                        PRIMARY KEY(strategy, test_condition)
                    )
                """
                con.execute(sql)

                # 从以前年度表格中获取测试条件集合,执行retest_conditions_from_sqlite3函数,并保存结果到condition-2023表中.
                if has_test_previous_year() is False:
                    # 获取以前年度表格的表名(不包含当年的表)
                    df = pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"', con)
                    all_table_names = df['name'].tolist()
                    prev_table_names = [table for table in all_table_names if str(now.tm_year) not in table and table != 'flag']

                    for prev_table in prev_table_names:
                        case.retest_conditions_from_sqlite3(src_sqlite3=TEST_CONDITION_SQLITE3, src_table=prev_table, dest_sqlite3=TEST_CONDITION_SQLITE3, dest_table=table_name)

                    # 如果没有当年的flag记录,则插入当年的flag值为Yes
                    sql = f"""
                        INSERT OR IGNORE INTO flag (year, flag) VALUES ('{time.localtime().tm_year}', 'Yes')
                    """
                    con.execute(sql)

                    # 更新当年的flag值为Yes
                    sql = f"""
                        UPDATE flag SET flag='Yes' WHERE year='{time.localtime().tm_year}'
                    """
                    con.execute(sql)
        case.test_strategy_random_condition(table_name=table_name)
