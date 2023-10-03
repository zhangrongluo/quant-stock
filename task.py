"""
使用python自动化类执行每日数据更新任务
更新的对象是curve.sqlite3和trade-record目录下的csv文件
indicator-roe-from-1991.sqlite3每年5月份以后更新或者新建一次即可
NOTE:
每日下午6点更新trade-record目录下的csv文件
每日下午7点更新curve.sqlite3
"""
import pandas as pd
import datetime
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import swindustry as sw
import data
from strategy import Strategy

def update_curve():
    """
    更新curve.sqlite3
    """
    print(f"{datetime.datetime.now()}开始更新curve.sqlite3")
    data.update_curve_value_table()
    print(f"{datetime.datetime.now()}更新curve.sqlite3完成")

def update_trade_record():
    """
    更新trade-record目录下的csv文件
    """
    print(f"{datetime.datetime.now()}开始更新trade-record目录下的csv文件")
    all_codes = [item[0][0:6] for item in sw.get_all_stocks()]
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.submit(data.update_trade_record_csv, all_codes)
    print(f"{datetime.datetime.now()}更新trade-record目录下的csv文件完成")

def test():
    import os
    print('this is a demo')
    file = 'dd.py'
    if os.path.exists(file):
        os.remove(file)
    else:
        # 创建文件
        with open(file, 'w') as f:
            pass
