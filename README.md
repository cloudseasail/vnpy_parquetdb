# Parquet文件数据库接口

<p align="center">
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.8|3.9|3.10-blue.svg" />
</p>

## 说明

相比于传统数据库，使用文件的方式管理 Bar/Tick 数据更加方便和高效。
这里选择使用parquet文件方式存储数据，在处理速度和存储大小上有很优秀的性能。

内部使用polars dataframe处理parquet文件，可以直接使用dataframe处理数据，同时完全兼容vnpy的格式

```Python
from vnpy_parquetdb import Database
database = Database(format = 'polars')  # 输出格式 polars dataframe
database = Database(format = 'vnpy')    # 输出格式兼容 vnpy 
    
```


## 使用

在VeighNa中使用Parquetdb时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.name|名称|是|parquetdb|

## Example
[Partquetdb 增删改查](https://github.com/cloudseasail/vnpy_parquetdb/blob/main/example/test_parquetdb.ipynb)
[vntrader datamanager](https://github.com/cloudseasail/vnpy_parquetdb/blob/main/example/vntrader_datamanager.py)
[Tools: Future Bar generation from ticks](https://github.com/cloudseasail/vnpy_parquetdb/blob/main/example/future_bar_generation.ipynb)



