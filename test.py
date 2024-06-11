import time
import pandas as pd
import sqlite3
from strategy import Strategy
from path import TEST_CONDITION_SQLITE3, COVER_YEARS, NEW_TABLE_MONTH
import threading
import json

lock = threading.Lock()
case = Strategy()
# quant-stock系统所有测试条件保存在test-condition.sqlite3数据库中,
# 每个年份一个表,用于保存该年度的全部测试条件,表名格式为condition-年份.
# 每年的1-4月生成的测试条件保存在上年的表中,比如2023年1-4月生成的测试条件保存在condition-2022表中.
# 每年的5-12月生成的测试条件保存在当年的表中,比如2023年5-12月生成的测试条件保存在condition-2023表中.

def create_retested_progress_table(con, cover_years: int=COVER_YEARS) -> None:
    """ 
    创建进度表格,记录TEST_CONDITION_SQLITE3中以前年度测试条件的测试进度.
    用于保存测试条件的测试进度,表格名为progress.
    :param con: sqlite3.connect()对象
    :param cover_years: 向前覆盖的年数,默认为1
    表格字段包括: table_name, total_rows, retested_rows, involved_years.
    """
    with con:
        sql = """
            CREATE TABLE IF NOT EXISTS progress
            (
                table_name TEXT,
                total_rows INTEGER,
                retested_rows INTEGER,
                involved_years TEXT,
                PRIMARY KEY(table_name, involved_years)
            )
        """
        con.execute(sql)
        con.commit()
        tables = pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"', con)
        tables = [table for table in tables['name'] if table.startswith('condition')]
        this_year = time.localtime().tm_year
        cover_table = [str(year) for year in range(this_year-cover_years, this_year)]
        tables = [table for table in tables if table.split('-')[1] in cover_table]
        for table in tables:
            # 获取表格中的总行数
            sql = f"""
                SELECT COUNT(*) FROM '{table}'
            """
            total_rows = con.execute(sql).fetchone()[0]
            sql = f"""
                INSERT OR IGNORE INTO progress 
                (table_name, total_rows, retested_rows, involved_years) 
                VALUES (?, ?, ?, ?)
            """
            params = (table, total_rows, 0, f"{this_year}")
            con.execute(sql, params)
            con.commit()

def get_restested_progress_detail(con, cover_years: int=COVER_YEARS) -> dict:
    """
    读取progress覆盖年度的测试条件完成进度
    :param cover_years: 向前覆盖的年数,默认为COVER_YEARS
    :param con: sqlite3.connect()对象
    :return: dict, key为表格名, value为total_rows\retested_rows\involved_years
    """
    with con:
        sql = f""" SELECT * FROM progress """
        df = pd.read_sql(sql, con)
        this_year = time.localtime().tm_year
        cover_table = [f"condition-{year}" for year in range(this_year-cover_years, this_year)]
        df = df[df['table_name'].str.contains('|'.join(cover_table))]  # 选择覆盖年度的表格
        df = df[df['involved_years'] == f"{this_year}"]  # 选择本年度的测试条件
        result = df.set_index('table_name').to_dict(orient='index')
    return result

def retest_previous_years_conditions(cover_years: int=COVER_YEARS) -> None:
    """ 
    重新测试以前年度的全部测试条件
    :param con: sqlite3.connect()对象
    :param cover_years: 向前覆盖的年数,默认为1
    """
    global case
    now = time.localtime()
    table_name = f'condition-{now.tm_year}' if now.tm_mon >= NEW_TABLE_MONTH else f'condition-{now.tm_year-1}'
    # 第一步 重新检测以前年度的全部测试条件
    con = sqlite3.connect(TEST_CONDITION_SQLITE3)
    with con:
        if now.tm_mon >= NEW_TABLE_MONTH:
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
            con.commit()
            # 从以前年度表格中获取测试条件集合,执行retest_conditions_from_sqlite3函数
            create_retested_progress_table(con=con, cover_years=cover_years)
            result = get_restested_progress_detail(con=con, cover_years=cover_years)
            prev_table_names = list(result.keys())
            for table, progress in result.items():
                if progress['retested_rows'] < progress['total_rows'] \
                    and progress['involved_years'] == f"{now.tm_year}":
                    print(f"开始重新测试{table}表格中的测试条件.")
                    case.retest_conditions_from_sqlite3(
                        src_sqlite3=TEST_CONDITION_SQLITE3, 
                        src_table=table, 
                        dest_sqlite3=TEST_CONDITION_SQLITE3, 
                        dest_table=table_name,
                        from_pos=progress['retested_rows']
                    )
                    # 更新进度表格中的retested_rows字段 TODO: 需要实时更新
                    sql = f"""
                        UPDATE progress SET retested_rows=? WHERE table_name=?
                    """
                    params = (progress['total_rows'], table)
                    con.execute(sql, params)
                    con.commit()
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
                        con.commit()
                        break

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
    global case
    while True:
        with lock:
            # 第一步 重新测试以前年度的全部测试条件
            try:
                retest_previous_years_conditions()
            except Exception as e:
                print(f"retest_previous_years_conditions函数出现异常:{e}")
                time.sleep(10)
                continue
            # 第二步 随机测试新生成的测试条件
            try:
                now = time.localtime()
                table_name = f'condition-{now.tm_year}' if now.tm_mon >= NEW_TABLE_MONTH \
                     else f'condition-{now.tm_year-1}'
                case.test_strategy_random_condition(
                    sqlite_file=TEST_CONDITION_SQLITE3, 
                    table_name=table_name
                )
                time.sleep(10)
            except Exception as e:
                print(f"test_strategy_random_condition函数出现异常:{e}")
                time.sleep(60)
        
if __name__ == '__main__':
    auto_test()