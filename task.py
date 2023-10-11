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
import datetime
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import swindustry as sw
import data
from test import auto_test
from path import TEST_CONDITION_SQLITE3
import threading
import platform

semaphore = threading.Semaphore(3)
scheduler = BackgroundScheduler()
thread = threading.Thread(target=auto_test)
codes = [item[0][0:6] for item in sw.get_all_stocks()]

# 每日下午6点30分开始更新一次trade record csv文件
@scheduler.scheduled_job('cron', hour=18, minute=30)
def update_trade_record_csv():
    with semaphore:
        print('开始更新trade record csv文件\r', end='', flush=True)
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(data.update_trade_record_csv, codes)
        print('更新trade record csv文件完成.' + ' '*20, flush=True)

# 每日下午6点30分开始更新一次curve.sqlite3
@scheduler.scheduled_job('cron', hour=18, minute=30)
def update_curve_sqlite3():
    with semaphore:
        print('开始更新curve.sqlite3\r', end='', flush=True)
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(data.update_curve_value_table, codes)
        print('更新curve.sqlite3完成.' + ' '*20, flush=True)

# 每周五上午7点30分将TEST_CONDITION_SQLITE3拷贝到本地仓库,更名为test-condition-quant.sqlite3
# 本地仓库路径: /Users/zhangrongluo/Desktop/pythonzone/win-stock-conditions/win-stock-conditions
# 每周五上午8点推送到github
@scheduler.scheduled_job('cron',  hour=18, minute=30)
def copy_test_condition_sqlite3():
    with semaphore:
        if "iMac" in platform.uname().node:  # 如果是在imac机器上
            print('开始拷贝TEST_CONDITION_SQLITE3\r', end='', flush=True)
            des_path = "/Users/zhangrongluo/Desktop/pythonzone/win-stock-conditions/win-stock-conditions"  # 本地仓库路径
            shutil.copyfile(TEST_CONDITION_SQLITE3, os.path.join(des_path, "test-condition-quant.sqlite3"))
            print('拷贝TEST_CONDITION_SQLITE3完成.' + ' '*20, flush=True)

# 每年5月1日上午2点更新一次indicator-roe-from-1991.sqlite3
@scheduler.scheduled_job('cron', month=5, day=1, hour=2, minute=0)
def update_indicator_roe_from_1991():
    with semaphore:
        print('开始更新indicator-roe-from-1991.sqlite3\r', end='', flush=True)
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(data.update_ROE_indicators_table_from_1991, codes)
        print('更新indicator-roe-from-1991.sqlite3完成.' + ' '*20, flush=True)

def run():
    scheduler.start()
    thread.start()
    while True:
        time.sleep(1)

if __name__ == '__main__':
    run()
