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

| 数据源      | 状态   | 说明                                           |
| ----------- | ------ | ---------------------------------------------- |
| **AKShare** | 计划中 | 免费，实时数据                                 |
| **Tushare** | 已接入 | 需满足账户积分要求，支持日线；分钟线需独立权限 |
| **米筐**    | 已接入 | 需license，支持日线/分钟线                     |
| **迅投**    | 计划中 | 需迅投终端                                     |
| **iTick**   | 计划中 | 需注册                                         |

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

以 Tushare/米筐为例 为例：

```python
from hqdata import (
    init_source, get_calendar,
    get_stock_list, get_stock_daily_bar, get_stock_minute_bar,
    get_index_list, get_index_daily_bar, get_index_minute_bar,
)

# 初始化 Tushare
init_source("tushare") # 如果使用米筐数据源则将"tushare"替换为"ricequant"，其它数据源同理

# 查询交易日历
get_calendar("20260301", "20260401") # 返回[20260301, 20260401]内所有自然日，不筛选
get_calendar("20260301", "20260401", is_open=True) # 只返回交易日
get_calendar("20260301", "20260401", is_open=False) # 只返回非交易日

# 查询当日股票列表
get_stock_list() # 返回所有股票
get_stock_list(symbol="000001.SZ,600000.SH") # 按股票代码筛选
get_stock_list(exchange="SSE,SZSE") # 按交易所筛选
get_stock_list(board="MB,GEM,STAR") # 按板块筛选
get_stock_list(board="MB", exchange="SSE") # 多参数时取交集

# 查询股票分钟线数据
get_stock_minute_bar("000001.SZ,600000.SH", frequency="1m", start_date="20260401", end_date="20260401")

# 查询股票日线数据
get_stock_daily_bar("000001.SZ,600000.SH", start_date="20260101", end_date="20260401")

# 查询当日指数列表
get_index_list() # 返回所有指数
get_index_list(symbol="000300.SH,000905.SH") # 按指数代码筛选
get_index_list(market="SSE,SZSE") # 按市场筛选
get_index_list(symbol="000300.SH", market="SZSE") # 同时传入symbol和market时，只有symbol生效

# 查询指数分钟线数据
get_index_minute_bar("000300.SH,000905.SH", frequency="1m", start_date="20260401", end_date="20260401")

# 查询指数日线数据
get_index_daily_bar("000300.SH,000905.SH", start_date="20260101", end_date="20260401")
```

## 测试

```bash
pytest tests/ -v # 运行全部测试
pytest tests/test_tushare.py::TestTushareIntegration::test_get_stock_daily_bar  # 运行单个测试
```

## 输入参数说明

### symbol（股票代码）

symbol 参数统一使用 `交易所简写代码` 作为后缀，支持以 `,` 分隔的多个symbol传入

| 交易所 | 交易所简写代码 | symbol示例  |
| ------ | -------------- | ----------- |
| 上交所 | SH             | "600000.SH" |
| 深交所 | SZ             | "000001.SZ" |

### start_date / end_date（日期区间）

日期格式为 `YYYYMMDD`，如 `"20260401"` 表示 2026年4月1日。

- `start_date`：开始日期（包含）
- `end_date`：结束日期（包含）

### frequency（频率）

`get_stock_minute_bar` / `get_index_minute_bar` 支持：

| 值    | 说明     | Tushare（需权限） | 米筐 |
| ----- | -------- | ----------------- | ---- |
| "1m"  | 1分钟线  | ✓                 | ✓    |
| "5m"  | 5分钟线  | ✓                 | ✓    |
| "15m" | 15分钟线 | ✓                 | ✓    |
| "30m" | 30分钟线 | ✓                 | ✓    |
| "60m" | 60分钟线 | ✓                 | ✓    |

### exchange（交易所）

| 代码   | 说明           |
| ------ | -------------- |
| "SSE"  | 上海证券交易所 |
| "SZSE" | 深圳证券交易所 |
| "BSE"  | 北京证券交易所 |

### is_open（是否交易日）

| 值    | 说明                   |
| ----- | ---------------------- |
| None  | 返回所有自然日（默认） |
| True  | 只返回交易日           |
| False | 只返回非交易日         |

### board（股票板块）

| 值     | 说明   |
| ------ | ------ |
| "MB"   | 主板   |
| "GEM"  | 创业板 |
| "STAR" | 科创板 |
| "BJSE" | 北交所 |

### market（指数市场）

| 值     | 说明       | 支持源       |
| ------ | ---------- | ------------ |
| "CSI"  | 中证指数   | Tushare      |
| "CICC" | 中金指数   | Tushare      |
| "SSE"  | 上交所指数 | Tushare,米筐 |
| "SZSE" | 深交所指数 | Tushare,米筐 |
| "BJSE" | 北交所指数 | 米筐         |
| "SW"   | 申万指数   | Tushare      |
| "MSCI" | MSCI 指数  | Tushare      |
| "OTH"  | 其他指数   | Tushare      |

特性说明：

* Tushare将北交所指数归类到了OTH中
* 米筐除了3个交易所指数，其他指数的market为NaN