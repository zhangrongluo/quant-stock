### 数据初始化
- 运行data.py文件
```python
python3 data.py
```
- 选择create_curve、create_roe_table和create_trade_record,分别创建国债收益率、历史ROE表和交易记录CSV文件.
- 整个过程需要约一个小时,耐心等待.

### 日常运行
- 运行task.py文件即可完成每日数据更新和测试.
```python
python3 task.py
```
- 也可以运行data.py文件中的update_...函数,完成数据更新.
### NOTE
- 本项目仅供学习参考,不保证盈利,请谨慎使用.
- quant-stock和win-stock互相独立对比,两者得出的结果可以互相参考.
- quant-stock主要使用tushare获取数据,win-stock主要使用爬虫获取数据,quant-stock在速度上比win-stock慢不少.

### 备注和说明
- 2024年4月27日起，使用TuSharePro数据源重写了申万行业信息接口，可以动态跟踪更新申万行业股票清单信息变化。
- 增加了数据完整性检查接口check_stockcodes_integrity，检查和修复历史交易记录文件和历史ROE表中的股票代码完整性。