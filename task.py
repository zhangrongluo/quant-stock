"""
使用python自动化类执行每日数据更新和自动测试任务
更新的对象是curve.sqlite3和trade-record目录下的csv文件
indicator-roe-from-1991.sqlite3每年5月份以后更新或者新建一次即可
NOTE:
每日下午6点30分更新trade record csv和curve.sqlite3,
在imac机器上将TEST_CONDITION_SQLITE3拷贝到本地仓库.
auto_test全时段运行,使用threading.Semaphore和threading.Lock处理线程同步.
"""
import os
import time
import shutil
import sqlite3
import tushare as ts
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import tsswindustry as sw
import data
from test import auto_test
from path import TEST_CONDITION_SQLITE3, IMAC_REPOSITORY_PATH, INDICATOR_ROE_FROM_1991, ROE_TABLE
import threading
import platform

semaphore = threading.Semaphore(5)
scheduler = BackgroundScheduler()
thread = threading.Thread(target=auto_test)
# codes = [item[0][0:6] for item in sw.get_all_stocks()]

def is_trade_day(func):
    """
    装饰器,判断是否为交易日,如果是则执行数据更新函数.
    :param func: 待执行的函数
    :return: wrapper函数
    """
    def wrapper(*args, **kwargs):
        today = time.strftime('%Y%m%d', time.localtime())
        pro = ts.pro_api()
        df = pro.trade_cal(
            **{"exchange": "", "cal_date": today},
            fields=["is_open"]
        )
        res = df["is_open"][0]
        if res:
            return func(*args, **kwargs)
        else:
            pass
    return wrapper

# 每日下午6点25开始检查文件和数据完整性
@scheduler.scheduled_job('cron', hour=18, minute=25, misfire_grace_time=600)
def check_integrity():
    with semaphore:
        res = data.check_stockcodes_integrity()
        if res["roe_table"]:
            print("indicator_roe_from_1991.sqlite3文件中缺失的股票代码:")
            print(res["roe_table"])
            print("开始补齐缺失的数据...")
            with ThreadPoolExecutor() as pool:
                pool.map(data.create_ROE_indicators_table_from_1991, res["roe_table"])
            print("indicator_roe_from_1991.sqlite3文件中缺失的数据已补齐."+" "*50)
        if res["trade_record_path"]:
            print("TRADE_RECORD_PATH目录中缺失的股票交易信息代码:")
            print(res["trade_record_path"])
            print("开始补齐缺失的交易信息文件...")
            diff_codes = res["trade_record_path"].values()
            for codes in diff_codes:
                with ThreadPoolExecutor() as pool:
                    pool.map(data.create_trade_record_csv_table, codes)
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
        codes = [item[0][0:6] for item in sw.get_all_stocks()]
        print(f"当前申万行业股票代码数量:{len(codes)}")

# 每日下午8点0分开始更新一次trade record csv文件
@scheduler.scheduled_job('cron', hour=20, minute=0, misfire_grace_time=3600)
@is_trade_day
def update_trade_record_csv():
    with semaphore:
        print('开始更新trade record csv文件\r', end='', flush=True)
        codes = [item[0][0:6] for item in sw.get_all_stocks()]
        for index in range(0, len(codes), 20):
            stocks = codes[index:index+20]
            with ThreadPoolExecutor() as executor:
                executor.map(data.update_trade_record_csv, stocks)
            time.sleep(1.5)
        print('更新trade record csv文件完成.' + ' '*20, flush=True)

# 每日下午6点30分开始更新一次curve.sqlite3
@scheduler.scheduled_job('cron', hour=18, minute=30, misfire_grace_time=600)
@is_trade_day
def update_curve_sqlite3():
    with semaphore:
        print('开始更新curve.sqlite3\r', end='', flush=True)
        data.update_curve_value_table()
        print('更新curve.sqlite3完成.' + ' '*20, flush=True)

# 每日下午6点30分开始更新一次index_value.sqlite3
@scheduler.scheduled_job('cron', hour=18, minute=30, misfire_grace_time=600)
@is_trade_day
def update_index_value_sqlite3():
    with semaphore:
        print('开始更新index_value.sqlite3\r', end='', flush=True)
        for index in ["000300", "000905", "399006"]:
            data.update_index_indicator_table(index)
        print('更新index_value.sqlite3完成.' + ' '*20, flush=True)

# 每天下午6点35分将TEST_CONDITION_SQLITE3拷贝到本地仓库,更名为test-condition-quant.sqlite3
# 然后推送到gitee main分支. 本地仓库路径IMAC_REPOSITORY_PATH
@scheduler.scheduled_job('cron',  hour=18, minute=35, misfire_grace_time=600)
def copy_test_condition_sqlite3():
    from path import ROOT_PATH
    with semaphore:
        if "iMac" in platform.uname().node:  # 如果是在imac机器上
            shutil.copyfile(TEST_CONDITION_SQLITE3, os.path.join(IMAC_REPOSITORY_PATH, "test-condition-quant.sqlite3"))
            print('拷贝TEST_CONDITION_SQLITE3完成.')
            os.chdir(IMAC_REPOSITORY_PATH)
            os.system("git add .")
            os.system("git commit -m 'auto commit'")
            os.system("git push gitee main")
            os.chdir(ROOT_PATH)
            print('推送到gitee main分支完成.' + ' '*20, flush=True)
            
# 每年5月1日上午0点0分1秒更新indicator-roe-from-1991.sqlite3
@scheduler.scheduled_job('cron', month=5, day=1, hour=0, minute=0, second=1, misfire_grace_time=600)
def update_indicator_roe_from_1991():
    with semaphore:
        print('开始更新indicator-roe-from-1991.sqlite3\r', end='', flush=True)
        codes = [item[0][0:6] for item in sw.get_all_stocks()]
        for index in range(0, len(codes), 20):
            stocks = codes[index:index+20]
            with ThreadPoolExecutor(max_workers=8) as executor:
                executor.map(data.update_ROE_indicators_table_from_1991, codes)
            time.sleep(1)
        print('更新indicator-roe-from-1991.sqlite3完成.' + ' '*20, flush=True)

def run():
    scheduler.start()
    thread.start()
    while True:
        time.sleep(1)

if __name__ == '__main__':
    run()
