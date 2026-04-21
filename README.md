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

## 支持的主要功能

| 功能         | API                    | tushare | ricequant | 说明                           |
| ------------ | ---------------------- | :-----: | :-------: | ------------------------------ |
| 交易日历     | `get_calendar`         |    ✓    |     ✓     |                                |
| 股票列表     | `get_stock_list`       |    ✓    |     ✓     | 返回当日上市状态的股票基本信息 |
| 股票实时快照 | `get_stock_snapshot`   |    ✓    |     ✓     | 含5档盘口                      |
| 股票日线     | `get_stock_daily_bar`  |    ✓    |     ✓     |                                |
| 股票分钟线   | `get_stock_minute_bar` |    ✗    |     ✓     |                                |
| 指数列表     | `get_index_list`       |    ✓    |     ✓     | 默认返回 SSE、SZSE 指数        |
| 指数日线     | `get_index_daily_bar`  |    ✓    |     ✓     |                                |
| 指数分钟线   | `get_index_minute_bar` |    ✗    |     ✓     |                                |

另有更多功能，可以前往api.py查看所有功能。

## 支持的数据源

| 数据源        | 状态   | 说明                                   |
| ------------- | ------ | -------------------------------------- |
| **tushare**   | 已接入 | 需满足账户2000积分, 部分功能需独立权限 |
| **ricequant** | 已接入 | 需license，试用请前往官网申请权限      |
| **AKShare**   | 计划中 | 免费，实时数据                         |
| **迅投**      | 计划中 | 需迅投终端                             |
| **iTick**     | 计划中 | 需注册                                 |

## 安装

### 方式一：通过pip安装

```bash
# 基础安装（仅包含核心功能）
pip install hqdata

# 按需安装数据源依赖
pip install hqdata[tushare]      # tushare 支持
pip install hqdata[ricequant]    # ricequant 支持
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

以 tushare/ricequant为例 为例：

```python
import hqdata

hqdata.init_source("tushare")
hqdata.get_stock_list() # 查询当日股票列表(上市状态)
```

## 命令行工具

安装后可直接使用 `hqdata` 命令从数据源拉取数据并按日期存储为 CSV 文件。

```bash
hqdata [--source SOURCE] [--output DIR] COMMAND [options]
```

| 参数       | 说明             | 默认值      |
| ---------- | ---------------- | ----------- |
| `--source` | 数据源，逗号分隔 | `tushare`   |
| `--output` | 输出根目录       | `~/.hqdata` |

使用子命令和 `--help` 可查看具体用法

## 测试

```bash
pytest tests/ -v # 运行全部测试
pytest tests/test_calendar.py::TestTradingCalendar::test_is_trading_day  # 运行单个测试
```

## 参数说明

### 输入

#### symbol（股票代码）

symbol 参数统一使用 `交易所简写代码` 作为后缀，支持以 `,` 分隔的多个symbol传入

| 交易所 | 交易所简写代码 | symbol示例  |
| ------ | -------------- | ----------- |
| 上交所 | SH             | "600000.SH" |
| 深交所 | SZ             | "000001.SZ" |

#### start_date / end_date（日期区间）

日期格式为 `YYYYMMDD` 的str，如 `"20260401"` 表示 2026年4月1日。

- `start_date`：开始日期（包含）
- `end_date`：结束日期（包含）

#### frequency（频率）

| 值    | 说明     | 支持源    |
| ----- | -------- | --------- |
| "1m"  | 1分钟线  | ricequant |
| "5m"  | 5分钟线  | ricequant |
| "15m" | 15分钟线 | ricequant |
| "30m" | 30分钟线 | ricequant |
| "60m" | 60分钟线 | ricequant |

#### exchange（交易所）

| 代码   | 说明           |
| ------ | -------------- |
| "SSE"  | 上海证券交易所 |
| "SZSE" | 深圳证券交易所 |
| "BSE"  | 北京证券交易所 |

#### is_open（是否交易日）

| 值    | 说明                   |
| ----- | ---------------------- |
| None  | 返回所有自然日（默认） |
| True  | 只返回交易日           |
| False | 只返回非交易日         |

#### board（股票板块）

| 值     | 说明   |
| ------ | ------ |
| "MB"   | 主板   |
| "GEM"  | 创业板 |
| "STAR" | 科创板 |
| "BJSE" | 北交所 |

#### market（指数市场）

market未指定时，默认为"SSE,SZSE"，即只返回上交所和深交所的指数

| 值     | 说明       | 支持源            |
| ------ | ---------- | ----------------- |
| "CSI"  | 中证指数   | tushare           |
| "CICC" | 中金指数   | tushare           |
| "SSE"  | 上交所指数 | tushare,ricequant |
| "SZSE" | 深交所指数 | tushare,ricequant |
| "BJSE" | 北交所指数 | ricequant         |
| "SW"   | 申万指数   | tushare           |
| "MSCI" | MSCI 指数  | tushare           |
| "OTH"  | 其他指数   | tushare           |

特性说明：

* tushare将北交所指数归类到了OTH中
* ricequant除了3个交易所指数，其他指数的market为NaN

### 输出参数说明

#### volume（成交量）

单位：手（lots，1手=100股，科创板1手=200股）

#### turnover（成交额）

单位：元（yuan）