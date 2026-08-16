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

| 功能         | API                   | tushare | ricequant | akshare | 说明                             |
| ------------ | --------------------- | :-----: | :-------: | :-----: | -------------------------------- |
| 交易日历     | `get_calendar`        |    ✓    |     ✓     |    ✓    |                                  |
| 股票列表     | `get_stock_list`      |    ✓    |     ✓     |    ✓    | 获取指定交易日当天的上市股票列表 |
| 股票实时快照 | `get_stock_snapshot`  |    ✓    |     ✓     |    ✗    | 含5档盘口                        |
| 股票日线     | `get_stock_daily_bar` |    ✓    |     ✓     |    ✗    |                                  |
| 复权因子     | `get_stock_factor`    |    ✓    |     ✓     |    ✗    | 累积后复权乘数                   |

**akshare 能力说明：** akshare 免费、无需 token，底层是对新浪财经/东方财富网页数据的抓取封装：
- 只接入了交易日历（`get_calendar`）和股票列表（`get_stock_list`）。日线、复权因子、实时快照
  三个接口经实测数据源不稳定（抓取式接口，无官方限流保障，容易被新浪/东财临时封 IP，且只支持
  单 symbol 查询），已停止支持。需要日线/复权因子/实时快照的场景请用 tushare 或 ricequant。
- `get_stock_list` 中，akshare的历史股票池重建缺少北交所退市股票的数据源（沪深两所有对应的终止上市接口，
  北交所没有找到），所以某个历史交易日已退市的北交所股票不会出现在结果里

## 支持的数据源

| 数据源        | 状态   | 说明                              |
| ------------- | ------ | --------------------------------- |
| **tushare**   | 已接入 | 需满足账户2000积分                |
| **ricequant** | 已接入 | 需license，试用请前往官网申请权限 |
| **akshare**   | 已接入 | 免费，无需注册                    |

## 安装

### 方式一：通过pip安装

```bash
# 基础安装（仅包含核心功能）
pip install hqdata

# 按需安装数据源依赖
pip install hqdata[tushare]      # tushare 支持
pip install hqdata[ricequant]    # ricequant 支持
pip install hqdata[akshare]      # akshare 支持
pip install hqdata[tushare,ricequant]  # 同时安装两者
```

### 方式二：本地开发

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖 (editable 模式，改代码直接生效)
pip install -e .
```

## 配置数据源

凭证支持两种配置方式，本质相同（最终都写入 `os.environ`）：

### 方式一：系统环境变量

直接在 shell 中导出，或写入 `~/.bashrc` / `~/.zshrc`：

```bash
export TUSHARE_TOKEN=your_token # tushare
```

### 方式二：`.env` 文件

复制示例文件并填入凭证，`import hqdata` 时会自动加载：

```bash
cp .env.example .env   # 放在运行 Python 的当前目录（优先）或包安装目录
```

## 使用

以 tushare/ricequant为例 为例：

```python
import hqdata

hqdata.init_source("tushare")
hqdata.get_stock_list()                         # 查询当日股票列表（上市状态）
hqdata.get_stock_list(trade_date="20260401")    # 查询历史时点股票池
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

已落盘数据都可以直接做对比，以交易日历举例（默认对比 tushare/ricequant）：

```bash
hqdata --output ~/.hqdata compare calendar
```

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

#### exchange（交易所）

| 代码  | 说明           |
| ----- | -------------- |
| "SSE" | 上海证券交易所 |
| "SZE" | 深圳证券交易所 |
| "BSE" | 北京证券交易所 |

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
| "BSE"  | 北交所 |

### 输出参数说明

#### volume（成交量）

单位：手（lots，1手=100股）

各板块（含科创板）成交量统一按 1手=100股 折算，不受"科创板最低买入200股"这一交易规则影响（后者是申报单位限制，不是成交量统计口径）

#### turnover（成交额）

单位：元（yuan）

#### factor（复权因子）

累积后复权（后复权，hfq）乘数：`raw_close * factor` 还原出该数据源的后复权价格序列。

- tushare：直接取 `adj_factor` 接口返回值
- ricequant：取 `get_price(adjust_type='post').close / get_price(adjust_type='none').close`，数值上与 `get_ex_factor` 返回的 `ex_cum_factor` 等价（已用真实数据验证），但按天返回，不需要额外按 `ex_end_date` 区间匹配

**注意：`factor` 的绝对值不可跨数据源比较**——两家的累积基准点不同（tushare 从更早的历史起点累积，ricequant 从各自的起点累积），只有同一数据源内的跨日比值 `factor[t] / factor[t-1]` 才有跨源可比性（该比值在正常交易日应为 1.0，在除权除息日应等于当次除权比例）。