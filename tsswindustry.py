""" 
使用TuSharePro数据源重写申万行业分类数据管理接口,
保证了每个选股策略中组合的样本和申万行业样本动态吻合.(2024年4月26日)
"""
import tushare as ts
import pandas as pd
from typing import List

def _get_stock_index_and_classes(src="SW2021", level="L1") -> pd.DataFrame:
    """
    获取申万行业分类代码和行业名称
    :param src: 申万行业分类数据版本, 默认为"SW2021"
    :param level: 申万行业分类级别, 默认为"L1"
    :return: 申万行业分类数据
    """
    pro = ts.pro_api()
    df = pro.index_classify(
        **{"index_code": "", "level": level, "src": src,}, 
        fields=["index_code", "industry_name"]
        )
    return df

def _get_all_stock_list() -> pd.DataFrame:
    """
    获取申万行业下所有股票清单
    :return: 申万行业下所有股票清单
    NOTE:
    字段名包括index_code, index_name, con_code, con_name, in_date, out_date, is_new
    返回值中包含了某只股票多次进入和退出申万行业指数的情况,此处不能进行去重处理
    """
    pro = ts.pro_api()
    tmp = _get_stock_index_and_classes()
    indexes = tmp["index_code"].unique().tolist()
    df_list = []
    for index in indexes:
        df = pro.index_member(
            **{"index_code": index,}, 
            fields=[
            "index_code", "index_name", "con_code","con_name", "in_date", "out_date", "is_new"
            ])
        df_list.append(df)
    result = pd.concat(df_list)
    result = result[result["con_code"].str.endswith("SZ") | result["con_code"].str.endswith("SH")]
    result["index_name"] = result["index_name"].map(lambda x: x[:-4])  # 去除index_name中"(申万)"字符
    # 剔除con_code不以6和0开头的股票 T00018.SH???
    result = result[result["con_code"].map(lambda x: x.startswith("6") or x.startswith("0"))]
    return result

SWDF = _get_all_stock_list()  # 未去重
DF = SWDF.drop_duplicates(subset=['con_code'])  # 去重

def get_stock_classes() -> List:
    """
    获取申万行业分类清单
    :return: 申万行业分类清单
    """
    result = DF['index_name'].unique().tolist()
    return result

def get_name_and_class_by_code(code: str) -> List:
    """
    通过股票代码获取公司简称及行业分类
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: [公司简称, 行业分类]
    """
    code = code + '.SH' if code.startswith('6') else code + '.SZ'
    if code not in DF['con_code'].values.tolist():
        raise ValueError(f"申万指数中不包括股票代码{code}, 请检查.")
    tmp = DF.loc[DF['con_code'] == code]
    result = tmp[['con_name', 'index_name']].values.tolist()[0]
    return result

def get_stocks_of_specific_class(stock_class: str) -> List:
    """
    获取stock_class指定的行业下股票代码 公司简称 行业分类
    :param stock_class: 行业分类
    :return: [[股票代码, 公司简称, 行业分类], ...]
    """
    tmp = DF.loc[DF['index_name'] == stock_class]  # 选出类所在的若干行
    result = tmp[['con_code', 'con_name', 'index_name']].values.tolist()
    return result

def get_all_stocks() -> List:
    """
    获取申万指数所有股票代码 公司简称 行业分类
    :return: [[股票代码, 公司简称, 行业分类], ...]
    """
    result = DF[['con_code', 'con_name', 'index_name']].values.tolist()
    return result

def in_index_or_not(code: str, date: str) -> bool:
    """
    判断股票在给定的日期是否在申万行业指数中
    :param code: 股票代码, 例如: '600000' or '000001'
    :param date: 日期, 例如: '2021-04-26'
    :return: True or False
    NOTE:
    在SWDF中查询con_code=code的行,然后判断date是否在in_date和out_date之间
    当股票存在多次进入和退出申万行业指数的情况时,只要有一次进入申万行业指数,就返回True
    """
    full_code = code + '.SH' if code.startswith('6') else code + '.SZ'
    row = SWDF.loc[SWDF['con_code'] == full_code]
    if row.empty:
        raise ValueError(f"申万指数中不包括股票代码{full_code}, 请检查.")
    date = date.replace('-', '')  # 日期格式转换成20210426
    in_date = row['in_date'].values
    out_date = row['out_date'].values
    result = False
    for i in range(len(in_date)):
        if in_date[i] <= date and (out_date[i] is None or out_date[i] >= date):
            result = True
            break
    return result
    
if __name__ == "__main__":
    res = get_all_stocks()[0:10]
    print(res)