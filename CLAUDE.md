# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在此仓库中工作时提供指导。

## 常用命令

```bash
# 以可编辑模式安装
pip install -e .

# 安装代码格式化工具（需与 black==25.1.0 保持一致）
pip install black==25.1.0

# 运行所有测试
pytest tests/ -v

# 运行单个测试
pytest tests/test_tushare.py::TestTushareIntegration::test_get_stock_daily_bar
```

## 架构设计

`hqdata` 是 HonestQuant 量化系统的数据基础层。上层（策略、引擎）**只调用 `hqdata.api`**，不直接接触任何数据源 SDK。

```
hqdata/api.py           # 公开接口：init_source()、get_stock_daily_bar()、get_stock_minute_bar()、get_stock_list()、get_index_list()、get_index_daily_bar()、get_index_minute_bar()
hqdata/sources/
  base.py               # BaseSource 抽象基类，定义接口规范
  tushare.py            # Tushare 适配器（支持日线、分钟线；股票列表、指数）
  ricequant.py          # 米筐适配器（支持日线/分钟线；股票列表、指数列表）
hqdata/config.py        # 在 import 时从项目根目录加载 .env
```

**新增数据源：** 继承 `BaseSource`，实现 `get_stock_daily_bar()`、`get_stock_minute_bar()`、`get_stock_list()`、`get_index_list()`、`get_index_daily_bar()`、`get_index_minute_bar()`，在 `api.py:init_source()` 中注册。

## 关键约定

- **股票代码格式：** `代码.交易所`，如 `600000.SH`（上交所）、`000001.SZ`（深交所）
- **指数代码格式：** 同股票代码，如 `000300.SH`（沪深300）、`000905.SH`
- **指数市场参数（`get_index_list` 的 `market`）：** `CSI`（中证）| `CICC`（中金）| `SSE`（上交所）| `SZSE`（深交所）| `BJSE`（北交所） | `SW`（申万）| `MSCI` | `OTH`（其他）
- **日期格式：** `YYYYMMDD` 字符串
- **频率参数（分钟线）：** `"1m"` | `"5m"` | `"15m"` | `"30m"` | `"60m"`
  - Tushare：需更高权限，调用 `stk_mins` / `idx_mins`
  - Ricequant：支持全部分钟频率
- **凭据配置：** 从项目根目录的 `.env` 加载（参考 `.env.example`）
- `pyproject.toml` 已被 gitignore，需在本地手动创建

## 开发规范

### 代码格式化
项目使用 `black==25.1.0` 统一格式化风格。`.claude/settings.json` 中已配置 Claude Code hook，每次修改 Python 文件后自动运行 black。团队成员需确保本地已安装对应版本（`pip install black==25.1.0`）。

### 接口联动检查
改上层接口（`api.py` / `__init__.py`）后，必须检查下层接口（`sources/*.py`）和测试文件是否需要联动改动，确保代码库各处一致。

### 参数描述一致性
同一参数（如 `symbol`、`frequency`、`date`）在不同数据源的 docstring 和 error message 中描述必须完全一致，不能有的地方写 `.SH/.SZ`，有的地方写 `.XSHG/.XSHE`。

### 读取 API 文档规范
读取 API 文档时要仔细，确保完全理解了再动手实现。不要对 API 参数格式做假设或猜测，要严格按照文档来。

### 测试用例设计
一个接口会返回沪深两个市场数据时，测试应该每个市场都挑一个股票测一下，而不是拆成多个 test case（如 `test_get_stock_bar_000001_sz`、`test_get_stock_bar_600000_sh`），应该是 `test_get_stock_bar()` 一次测完。

## 测试结构

- **单元测试**：mock 外部 SDK，无需凭据即可运行
- **集成测试**（`TestXxxIntegration`）：若 `.env` 中未配置凭据则自动跳过

## 数据获取

当提到与米筐相关的金融问题，且需要获取金融数据时务必使用rqdata获取，`.claude\commands\ricequant-doc-index.md` 文件中有相关说明。无法Fetch文档时，请用curl命令行工具获取文档。不要通过websearch等方式获取文档。

当提到与Tushare相关的金融问题，且需要获取金融数据时务必使用tushare获取，`.claude\commands\tushare-doc-index.md` 文件中有相关说明。无法Fetch文档时，请用curl命令行工具获取文档。不要通过websearch等方式获取文档。
