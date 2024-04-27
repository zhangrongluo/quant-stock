""" 
使用TuSharePro管理申万行业分类数据,比sw-stock-list.xlsx要多700只左右股票,
但是获取数据速度没有sw-stock-list.xlsx快.(2024年4月26日)
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

def _get_stock_classes(src="SW2021", level="L1") -> List:
    """
    获取申万行业名称清单
    :param src: 申万行业分类数据版本, 默认为"SW2021"
    :param level: 申万行业分类级别, 默认为"L1"
    :return: 申万行业分类清单
    """
    df = _get_stock_index_and_classes(src=src, level=level)
    result = df["industry_name"].unique().tolist()
    return result

def _get_all_stock_list(isnew=True) -> pd.DataFrame:
    """
    获取申万行业下所有股票清单
    :param isnew: 是否最新的申万行业分类数据, 默认为True
    :return: 申万行业下所有股票清单
    NOTE:
    字段名包括index_code, index_name, con_code, con_name, in_date, out_date, is_new
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
    # 仅保留以SZ和SH结尾的股票
    result = result[result["con_code"].str.endswith("SZ") | result["con_code"].str.endswith("SH")]
    # 去除index_name中的后四个字符 "(申万)"
    result["index_name"] = result["index_name"].map(lambda x: x[:-4])
    if isnew:
        result = result[result["is_new"] == "Y"]
    return result

df = _get_all_stock_list()

def get_stock_classes() -> List:
    """
    获取申万行业分类清单
    :return: 申万行业分类清单
    """
    result = df['index_name'].unique().tolist()
    return result

def get_name_and_class_by_code(code: str) -> List:
    """
    通过股票代码获取公司简称及行业分类
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: [公司简称, 行业分类]
    """
    code = code + '.SH' if code.startswith('6') else code + '.SZ'
    if code not in df['con_code'].values.tolist():
        raise ValueError(f"申万指数中不包括股票代码{code}, 请检查.")
    tmp = df.loc[df['con_code'] == code]
    result = tmp[['con_name', 'index_name']].values.tolist()[0]
    return result

def get_stocks_of_specific_class(stock_class: str) -> List:
    """
    获取stock_class指定的行业下股票代码 公司简称 行业分类
    :param stock_class: 行业分类
    :return: [[股票代码, 公司简称, 行业分类], ...]
    """
    tmp = df.loc[df['index_name'] == stock_class]  # 选出类所在的若干行
    result = tmp[['con_code', 'con_name', 'index_name']].values.tolist()
    return result

def get_all_stocks(isnew=True) -> List:
    """
    获取申万指数所有股票代码 公司简称 行业分类
    :param isnew: 是否最新的申万行业分类数据, 默认为True
    :return: [[股票代码, 公司简称, 行业分类], ...]
    """
    result = df[['con_code', 'con_name', 'index_name']].values.tolist()
    return result

if __name__ == "__main__":
    res = get_all_stocks()[0:10]
    print(res)