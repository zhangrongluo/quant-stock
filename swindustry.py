"""
管理申万行业分类股票
"""
import os
import pandas as pd
from typing import List
from path import SW_INDUSTRY_XLS

def get_stock_classes() -> List:
    """
    获取申万行业分类清单
    :return: 申万行业分类清单
    """
    df = pd.read_excel(SW_INDUSTRY_XLS, usecols=['新版一级行业'])
    result = df['新版一级行业'].unique().tolist()
    return result

def get_name_and_class_by_code(code: str) -> List:
    """
    通过股票代码获取公司简称及行业分类
    :param code: 股票代码, 例如: '600000' or '000001'
    :return: [公司简称, 行业分类]
    """
    df = pd.read_excel(SW_INDUSTRY_XLS, usecols=['股票代码', '公司简称', '新版一级行业'])
    code = code + '.SH' if code.startswith('6') else code + '.SZ'
    if code not in df['股票代码'].values.tolist():
        raise ValueError(f"申万指数中不包括股票代码{code}, 请检查.")
    tmp = df.loc[df['股票代码'] == code]
    result = tmp[['公司简称', '新版一级行业']].values.tolist()[0]
    return result

def get_stocks_of_specific_class(stock_class: str, contain_foreign = False) -> List:
    """
    获取stock_class指定的行业下股票代码 公司简称 行业分类
    :param stock_class: 行业分类
    :param contain_foreign: 是否包含境外公司, 默认不包含
    :return: [[股票代码, 公司简称, 行业分类], ...]
    """
    df = pd.read_excel(SW_INDUSTRY_XLS, usecols=['股票代码', '公司简称', '新版一级行业'])
    tmp = df.loc[df['新版一级行业'] == stock_class]  # 选出类所在的若干行
    if not contain_foreign:
        criterion = df['股票代码'].map(lambda x: ('.SZ' in x) or ('.SH' in x) or ('.sh' in x) or ('.sz' in x))
        result = tmp[criterion][['股票代码', '公司简称', '新版一级行业']].values.tolist()
    else:
        result = tmp[['股票代码', '公司简称', '新版一级行业']].values.tolist()
    return result

def get_all_stocks(contain_foreign = False) -> List:
    """
    获取申万指数所有股票代码 公司简称 行业分类
    :return: [[股票代码, 公司简称, 行业分类], ...]
    """
    df = pd.read_excel(SW_INDUSTRY_XLS, usecols=['股票代码', '公司简称', '新版一级行业'])
    if not contain_foreign:
        criterion = df['股票代码'].map(lambda x: ('.SZ' in x) or ('.SH' in x) or ('.sh' in x) or ('.sz' in x))
        result = df[criterion][['股票代码', '公司简称', '新版一级行业']].values.tolist()
    else:
        result = df[['股票代码', '公司简称', '新版一级行业']].values.tolist()
    return result


if __name__ == '__main__':
    res = get_all_stocks()
    print(res[:10])