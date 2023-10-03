import os

# 内置目录
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
TRADE_RECORD_PATH = os.path.join(ROOT_PATH, "trade-record")  # 股票历史交易记录文件保存目录
DATA_PACKAGE_PATH = os.path.join(ROOT_PATH, "data-package")  # 数据包保存目录
SQL_PATH = os.path.join(ROOT_PATH, "sql")  # SQL文件保存目录
TEST_CONDITION_PATH = os.path.join(ROOT_PATH, "test-condition")  # 测试条件保存目录

if not os.path.exists(DATA_PACKAGE_PATH):
    os.mkdir(DATA_PACKAGE_PATH)
if not os.path.exists(SQL_PATH):
    os.mkdir(SQL_PATH)
if not os.path.exists(TRADE_RECORD_PATH):
    os.mkdir(TRADE_RECORD_PATH)
if not os.path.exists(TEST_CONDITION_PATH):
    os.mkdir(TEST_CONDITION_PATH)

# 内置文件
SW_INDUSTRY_PATH = os.path.join(ROOT_PATH, "stock-list")
SW_INDUSTRY_XLS = os.path.join(ROOT_PATH, "stock-list", "sw-stock-list.xlsx")  # 申万行业分类文件
INDICATOR_ROE_FROM_1991 = os.path.join(ROOT_PATH, "data-package", "indicator-roe-from-1991.sqlite3")  # ROE数据文件
CURVE_SQLITE3 = os.path.join(ROOT_PATH, "data-package", "curve.sqlite3")  # 国债收益率曲线数据文件
TEST_CONDITION_SQLITE3 = os.path.join(ROOT_PATH, "test-condition", "test-condition.sqlite3")  # 测试条件数据文件

if not os.path.exists(SW_INDUSTRY_XLS):
    raise FileNotFoundError(f"未在{SW_INDUSTRY_PATH}发现申万行业分类清单文件,请检查.")

# 数据库表名
ROE_TABLE = "indicators"  # indicator-roe-from-1991.sqlite3中的表
CURVE_TABLE = "curve"  # curve.sqlite3中的表
CONDITION_TABLE = "conditions"  # test-condition.sqlite3中的表