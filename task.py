"""
使用python自动化类执行每日数据更新和自动测试任务
更新的对象是curve.sqlite3和trade-record目录下的csv文件
indicator-roe-from-1991.sqlite3每年5月份以后更新或者新建一次即可
NOTE:
每日下午6点05分开始更新一次trade record csv文件
每日下午6点10分开始更新一次curve.sqlite3
调用test.auto_test每天下午6点至7点30分之外的时间执行自动测试任务
"""
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import swindustry as sw
import data
import test

scheduler = BackgroundScheduler()
codes = [item[0][0:6] for item in sw.get_all_stocks()]

# 每日下午6点05分开始更新一次trade record csv文件
@scheduler.scheduled_job('cron', hour=18, minute=5, id='update_trade_record_csv')
def update_trade_record_csv():
    print('开始更新trade record csv文件\r', end='', flush=True)
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(data.update_trade_record_csv, codes)
    print('更新trade record csv文件完成' + ' '*20)

# 每日下午6点10分开始更新一次curve.sqlite3
@scheduler.scheduled_job('cron', hour=18, minute=10, id='update_curve_sqlite3')
def update_curve_sqlite3():
    print('开始更新curve.sqlite3\r', end='', flush=True)
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(data.update_curve_sqlite3, codes)
    print('更新curve.sqlite3完成' + ' '*20)

def run():
    scheduler.start()
    scheduler.print_jobs()
    test.auto_test()  # 每天下午6点至7点30分之外的时间执行自动测试任务

if __name__ == '__main__':
    run()