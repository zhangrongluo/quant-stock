import os
import time
import pandas as pd

# 内置目录
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
TRADE_RECORD_PATH = os.path.join(ROOT_PATH, "trade-record")  # 股票历史交易记录文件保存目录
DATA_PACKAGE_PATH = os.path.join(ROOT_PATH, "data-package")  # 数据包保存目录
SQL_PATH = os.path.join(ROOT_PATH, "sql")  # SQL文件保存目录
TEST_CONDITION_PATH = os.path.join(ROOT_PATH, "test-condition")  # 测试条件保存目录
STOCK_MOS_IMG = os.path.join(ROOT_PATH, "stock-mos-img")  # 股票MOS图保存目录
INDEX_MOS_IMG = os.path.join(ROOT_PATH, "index-mos-img")  # 指数MOS图保存目录

if not os.path.exists(DATA_PACKAGE_PATH):
    os.mkdir(DATA_PACKAGE_PATH)
if not os.path.exists(SQL_PATH):
    os.mkdir(SQL_PATH)
if not os.path.exists(TRADE_RECORD_PATH):
    os.mkdir(TRADE_RECORD_PATH)
if not os.path.exists(TEST_CONDITION_PATH):
    os.mkdir(TEST_CONDITION_PATH)
if not os.path.exists(STOCK_MOS_IMG):
    os.mkdir(STOCK_MOS_IMG)
if not os.path.exists(INDEX_MOS_IMG):
    os.mkdir(INDEX_MOS_IMG)

# 内置文件
SW_INDUSTRY_PATH = os.path.join(ROOT_PATH, "stock-list")
SW_INDUSTRY_XLS = os.path.join(ROOT_PATH, "stock-list", "sw-stock-list.xlsx")  # 申万行业分类文件
INDICATOR_ROE_FROM_1991 = os.path.join(ROOT_PATH, "data-package", "indicator-roe-from-1991.sqlite3")  # ROE数据文件
CURVE_SQLITE3 = os.path.join(ROOT_PATH, "data-package", "curve.sqlite3")  # 国债收益率曲线数据文件
TEST_CONDITION_SQLITE3 = os.path.join(ROOT_PATH, "test-condition", "test-condition.sqlite3")  # 测试条件数据文件
INDEX_VALUE = os.path.join(ROOT_PATH, "data-package", "index-value.sqlite3")  # 指数数据文件

if not os.path.exists(SW_INDUSTRY_XLS):
    raise FileNotFoundError(f"未在{SW_INDUSTRY_PATH}发现申万行业分类清单文件,请检查.")
SW_INDUSTRY_DF = pd.read_excel(SW_INDUSTRY_XLS, usecols=['股票代码', '公司简称', '新版一级行业'])

# 数据库表名
ROE_TABLE = "indicators"  # indicator-roe-from-1991.sqlite3中的表
CURVE_TABLE = "curve"  # curve.sqlite3中的表
NEW_TABLE_MONTH = 5  # 新年度表格生成月份

# iMac本地仓库路径
IMAC_REPOSITORY_PATH = "/Users/zhangrongluo/Desktop/pythonzone/win-stock-conditions/win-stock-conditions"

# 策略参数
STRATEGIES = ['ROE-DIVIDEND', 'ROE-MOS', 'ROE-MOS-DIVIDEND', 'ROE-MOS-MULTI-YIELD']
MOS_STEP = 0.25  # MOS步长
TRADE_MONTH = list(range(5, 13))  # 交易月份
HOLDING_TIME = [4, 6, 12]  # 每个时间组持有时间(月)
MAX_NUMBERS = 15  # 全部时间组最大平均股票数量
ROE_LIST = [8, 24]  # ROE范围
MOS_RANGE = [-1, 1]  # MOS范围
DV_LIST = [0, 10]  # 股息率范围
COVER_YEARS = 1  # 重新测试时向前覆盖年数