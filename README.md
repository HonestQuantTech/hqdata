# hqdata

> A股历史与实时行情数据统一接入、清洗与存储

## 定位

`hqdata` 是 HonestQuant 量化系统的**数据基础层**，职责边界清晰：

- 对下：封装各数据源 SDK，屏蔽接口差异
- 对上：提供统一的查询接口
- 上层策略和引擎**只调用 `hqdata.api`**，不直接接触任何数据源

## 支持的数据源

| 数据源      | 状态   |
| ----------- | ------ |
| **AKShare** | 计划中 |
| **Tushare** | 计划中 |
| **米筐**    | 已接入 |
| **迅投**    | 计划中 |
| **iTick**   | 计划中 |

## 安装

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 安装依赖
pip install -e .

# 配置凭据 (复制模板并编辑)
cp .env.example .env
# 编辑 .env 填入 RQDATA_USERNAME 和 RQDATA_PASSWORD
```

## 使用

```python
from hqdata import init_source, get_tick

# 初始化 (凭据从 .env 自动读取)
init_source("ricequant")

# 获取Tick数据
df = get_tick("600000", exchange="XSHG", start_date="2026-04-01", end_date="2026-04-02")
print(df.head())
```

## 交易所代码

| 交易所 | 代码  | 说明 |
| ------ | ----- | ---- |
| 上交所 | XSHG  | SSE  |
| 深交所 | XSHE  | SZSE |

## 测试

```bash
pytest tests/ -v
pytest tests/test_ricequant.py::test_name  # 运行单个测试
```
