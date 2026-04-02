# hqdata

> A股历史与实时行情数据统一接入、清洗与存储

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

### 本地开发

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖 (editable 模式，改代码直接生效)
pip install -e .

# 配置凭据 (复制模板并编辑)
cp .env.example .env
# 编辑 .env 填入 RQDATA_USERNAME (米筐) 或 TUSHARE_TOKEN (Tushare)
```

### 通过 pip 安装

```bash
# 从 PyPI 安装 (暂未发布，等待 license 申请完成)
pip install hqdata

参考 `.env.example` 配置你的 `.env` 文件

python-dotenv 会自动从**运行代码的当前目录**加载 `.env` 文件。

## 使用

```python
from hqdata import init_source, get_bar

# 初始化 Tushare (日线数据)
init_source("tushare")

# 获取日线数据 (symbol 格式: "代码.交易所")
df = get_bar("600000.SH", frequency="1day", start_date="20260401", end_date="20260402")
print(df.head())
```

## 输入参数格式

### 股票代码

symbol 参数统一使用 `代码.交易所` 格式：

| 交易所 | 代码 | 示例        |
| ------ | ---- | ----------- |
| 上交所 | SH   | `600000.SH` |
| 深交所 | SZ   | `000001.SZ` |

## 测试

```bash
pytest tests/ -v
pytest tests/test_tushare.py::TestTushareIntegration::test_get_bar  # 运行单个测试
```
