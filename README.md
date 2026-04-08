# hqdata - A股历史与实时行情数据统一接入、清洗与存储

<p align="center">
    <img src="https://img.shields.io/pypi/v/hqdata.svg"/>
    <img src="https://img.shields.io/pypi/pyversions/hqdata.svg"/>
    <img src="https://img.shields.io/badge/tushare-%3E%3D1.4.29-blue"/>
    <img src="https://img.shields.io/badge/rqdatac-%3E%3D3.1.4-blue"/>
</p>

## 定位

`hqdata` 是 HonestQuant 量化系统的**数据基础层**，职责边界清晰：

- 对下：封装各数据源 SDK，屏蔽接口差异
- 对上：提供统一的查询接口
- 上层策略和引擎**只调用 `hqdata.api`**，不直接接触任何数据源

## 支持的数据源

| 数据源      | 状态   | 说明                                        |
| ----------- | ------ | ------------------------------------------- |
| **AKShare** | 计划中 | 免费，实时数据                              |
| **Tushare** | 已接入 | 需满足账户积分要求，支持股票&指数的历史日线 |
| **米筐**    | 接入中 | 需license，支持Tick                         |
| **迅投**    | 计划中 | 需迅投终端                                  |
| **iTick**   | 计划中 | 需注册                                      |

## 安装

### 方式一：通过pip安装

```bash
# 基础安装（仅包含核心功能）
pip install hqdata

# 按需安装数据源依赖
pip install hqdata[tushare]      # Tushare 支持
pip install hqdata[ricequant]    # 米筐支持
pip install hqdata[tushare,ricequant]  # 同时安装两者

# 复制模板并自行填入所需字段
cp .env.example .env # 放在你运行 Python 代码的当前目录（优先）/包安装目录
```

### 方式二：本地开发

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖 (editable 模式，改代码直接生效)
pip install -e .

# 复制模板并自行填入所需字段
cp .env.example .env # 放在你运行 Python 代码的当前目录（优先）/包安装目录
```

## 使用

以 Tushare 为例：

```python
from hqdata import init_source, get_stock_list, get_stock_bar, get_index_list, get_index_bar

# 初始化
init_source("tushare")

# 查询股票列表 (支持5种过滤参数)
get_stock_list() # 等同于get_stock_list(list_status="L")
get_stock_list(symbol="000001.SZ")
get_stock_list(symbol="000001.SZ,600000.SH")
get_stock_list(exchange="SSE")
get_stock_list(exchange="SSE,SZSE")
get_stock_list(market="MB")
get_stock_list(market="MB,GEM,STAR")
get_stock_list(list_status="D")
get_stock_list(is_hs="N")
get_stock_list(market="MB", list_status="D")

# 查询股票日线数据
get_stock_bar("000001.SZ", frequency="day", start_date="20260101", end_date="20260401")
get_stock_bar("000001.SZ,600000.SH", frequency="day", start_date="20260101", end_date="20260401")

# 查询指数列表
get_index_list(symbol="000300.SH")
get_index_list(symbol="000300.SH,000905.SH")
get_index_list(market="SSE")
get_index_list(market="SSE,SZSE")
get_index_list(symbol="000300.SH", market="SZSE") # 同时传入symbol和market, 只有symbol字段会生效

# 查询指数日线数据
get_index_bar("000300.SH", start_date="20260101", end_date="20260401")
get_index_bar("000300.SH,000905.SH", start_date="20260330", end_date="20260401")
```

## 测试

```bash
pytest tests/ -v # 运行全部测试
pytest tests/test_tushare.py::TestTushareIntegration::test_get_stock_bar  # 运行单个测试
```

## 输入参数说明

### symbol（股票代码）

symbol 参数统一使用 `交易所简写代码` 作为后缀：

| 交易所 | 交易所简写代码 | 示例        |
| ------ | -------------- | ----------- |
| 上交所 | SH             | `600000.SH` |
| 深交所 | SZ             | `000001.SZ` |

### start_date / end_date（日期区间）

日期格式为 `YYYYMMDD`，如 `20260401` 表示 2026年4月1日。

- `start_date`：开始日期（包含）
- `end_date`：结束日期（包含）

### frequency（频率）

| 值      | 说明         |
| ------- | ------------ |
| `tick`  | 实时         |
| `1m`    | 1分钟线      |
| `5m`    | 5分钟线      |
| `15m`   | 15分钟线     |
| `30m`   | 30分钟线     |
| `60m`   | 60分钟线     |
| `day`   | 日线（默认） |
| `week`  | 周线         |
| `month` | 月线         |

### exchange（交易所）

| 交易所 | 代码 | 说明           |
| ------ | ---- | -------------- |
| 上交所 | SSE  | 上海证券交易所 |
| 深交所 | SZSE | 深圳证券交易所 |
| 北交所 | BSE  | 北京证券交易所 |

### list_status（上市状态）

| 值  | 说明         |
| --- | ------------ |
| `L` | 上市（默认） |
| `D` | 退市         |
| `P` | 暂停上市     |
| `G` | 过会未交易   |

### is_hs（是否沪深港通标的）

| 值  | 说明   |
| --- | ------ |
| `H` | 沪股通 |
| `S` | 深股通 |
| `N` | 否     |

### market（市场板块）

market 参数需根据具体接口确定，参见各接口说明。
