# Ricequant Docs Index

## 文档地址
- 总索引：https://www.ricequant.com/doc/rqdata/python/index-rqdatac
- 通用API：https://www.ricequant.com/doc/rqdata/python/generic-api
- A股财务：https://www.ricequant.com/doc/rqdata/python/stock-mod
- 指数基金：https://www.ricequant.com/doc/rqdata/python/indices-mod
- 期货：https://www.ricequant.com/doc/rqdata/python/futures-mod
- 期权：https://www.ricequant.com/doc/rqdata/python/options-mod
- 可转债：https://www.ricequant.com/doc/rqdata/python/convertible-mod
- 基金：https://www.ricequant.com/doc/rqdata/python/fund-mod

---

## 模块总览

| 模块 | 路径 | 内容 |
|------|------|------|
| 跨品种通用API | generic-api | 合约信息、行情、交易日历 |
| A股 | stock-mod | 财务数据、分红、融资融券、板块行业 |
| 港股 | stock-hk | 行情、复权因子、财务、行业分类 |
| 期货 | futures-mod | 主力合约、仓单、升贴水、交易参数 |
| 期权 | options-mod | 合约、希腊字母、PCR/skew指标 |
| 指数、场内基金 | indices-mod | 指数值、成分及权重 |
| 基金 | fund-mod | 公募基金、ETF、估值、持仓、经理 |
| 可转债 | convertible-mod | 可转债信息、强赎、回售、现金流 |
| 宏观 | macro-economy | 存款准备金率、货币供应量、宏观因子 |
| 另类数据 | alternative-data | 一致预期、新闻舆情、ESG |

---

## 一、通用API (rqdatac)

### 1.1 合约信息

#### all_instruments
获取所有合约基础信息
```python
rq.all_instruments(type=None, date=None, market='cn')
```
- `type`: `'CS'`(股票)/`'INDX'`(指数)/`'Future'`/'ETF'/`'Option'`等
- 返回: pandas DataFrame

#### instruments
获取合约详细信息
```python
rq.instruments(order_book_ids, market='cn')
```
- 返回: Instrument 对象或列表
- 示例: `instruments('000001.XSHE')`

#### id_convert
交易所代码与Ricequant格式互转
```python
rq.id_convert(order_book_ids, to=None)
```
- `to='normal'`: `'000001.XSHE'` → `'000001.SZ'`
- `to='XSHG'`: `'600000.SH'` → `'600000.XSHG'`
- `to='XSHE'`: `'000001.SZ'` → `'000001.XSHE'`

### 1.2 行情数据

#### get_price (核心接口)
获取行情数据
```python
rq.get_price(order_book_ids, start_date=None, end_date=None,
             frequency='1d', fields=None, adjust_type='pre',
             skip_suspended=False, expect_df=True, time_slice=None, market='cn')
```
- `frequency`: `'1d'`(日线)/`'1w'`(周线)/`'1m'`/`'5m'`/`'15m'`/`'30m'`/`'60m'`/`'tick'`
- `adjust_type`: `'none'`/`'pre'`/`'post'`（前复权/后复权）
- `expect_df=True`: 返回 MultiIndex(order_book_id, date) DataFrame
- 返回: DataFrame，字段包括 open/high/low/close/volume/amount

#### get_ticks
获取日内Tick数据
```python
rq.get_ticks(order_book_id, start_date=None, end_date=None, expect_df=True, market='cn')
```

#### get_live_ticks
获取带时间切片的Tick数据
```python
rq.get_live_ticks(order_book_ids, start_dt=None, end_dt=None, fields=None, market='cn')
```

#### get_auction_info
获取盘后定价数据（竞价数据）
```python
rq.get_auction_info(order_book_ids, start_date=None, end_date=None, frequency='1d', market='cn')
```

#### get_open_auction_info
获取开盘竞价数据
```python
rq.get_open_auction_info(order_book_ids, start_date=None, end_date=None, market='cn')
```

#### current_minute
获取最新1分钟K线
```python
rq.current_minute(order_book_ids, skip_suspended=False, market='cn')
```

#### current_snapshot
获取当前行情快照
```python
rq.current_snapshot(order_book_ids, market='cn')
```
- 返回: Tick 对象

#### get_vwap
获取VWAP
```python
rq.get_vwap(order_book_ids, start_date=None, end_date=None, frequency='1d')
```

#### get_price_change_rate
获取涨跌幅
```python
rq.get_price_change_rate(order_book_ids, start_date=None, end_date=None, expect_df=True, market='cn')
```

### 1.3 交易日历

```python
rq.get_trading_dates(start_date, end_date, market='cn')
rq.get_previous_trading_date(date, n=1, market='cn')
rq.get_next_trading_date(date, n=1, market='cn')
rq.get_latest_trading_date(market='cn')
```

### 1.4 交易时间

```python
rq.get_trading_hours(order_book_id, date=None, market='cn')
# 返回: '09:31-11:30,13:01-15:00'
```

### 1.5 收益率曲线

```python
rq.get_yield_curve(start_date=None, end_date=None, tenor=None, market='cn')
```

---

## 二、期货API (rq.futures)

| 函数 | 说明 | 关键参数 |
|------|------|---------|
| `get_dominant` | 获取主力合约 | underlying_symbol, rule, rank |
| `get_contracts` | 获取可交易合约列表 | underlying_symbol, date |
| `get_dominant_price` | 主力连续合约行情 | underlying_symbols, frequency, adjust_type |
| `get_ex_factor` | 复权因子 | underlying_symbols |
| `get_contract_multiplier` | 合约乘数 | underlying_symbols |
| `get_exchange_daily` | 交易所日线数据 | order_book_ids |
| `get_continuous_contracts` | 连续合约 | underlying_symbol, type |
| `get_member_rank` | 会员持仓排名 | obj, trading_date, rank_by |
| `get_warehouse_stocks` | 仓单数据 | underlying_symbols |
| `get_basis` | 升贴水数据 | order_book_ids, frequency |
| `get_current_basis` | 实时升贴水 | order_book_ids |
| `get_trading_parameters` | 交易参数 | order_book_ids |
| `get_roll_yield` | 展期收益率 | underlying_symbol, type |

主力合约选取规则(rule):
- `0`: 持仓量最大
- `1`: 成交量最大
- `2`: 持仓量+成交量综合

---

## 三、期权API (rq.options)

| 函数 | 说明 | 关键参数 |
|------|------|---------|
| `get_contracts` | 筛选期权合约 | underlying, option_type, maturity, strike |
| `get_greeks` | 希腊字母 | order_book_ids, start_date, model |
| `get_contract_property` | ETF期权合约属性 | order_book_ids |
| `get_dominant_month` | 主力月份 | underlying_symbols, rule, rank |
| `get_indicators` | 期权衍生指标 | underlying_symbols, maturity |

返回字段: iv, delta, gamma, vega, theta, rho, AM_PCR, OI_PCR, VL_PCR, skew

---

## 四、可转债API (rq.convertible)

| 函数 | 说明 |
|------|------|
| `all_instruments` | 所有可转债基础信息 |
| `instruments` | 合约详细信息 |
| `get_conversion_price` | 转股价变动 |
| `get_conversion_info` | 转股规模变动 |
| `get_call_info` | 强赎信息 |
| `get_put_info` | 回售信息 |
| `get_cash_flow` | 现金流数据 |
| `is_suspended` | 是否停牌 |
| `get_indicators` | 衍生指标（转股溢价率、到期收益率、双低指标） |
| `get_credit_rating` | 债项评级 |
| `get_close_price` | 全价净价数据 |

---

## 五、基金API (rq.fund)

### 基础信息
| 函数 | 说明 |
|------|------|
| `all_instruments` | 公募基金列表 |
| `instruments` | 基金详细信息 |
| `get_nav` | 净值信息 |
| `get_transaction_status` | 申购赎回状态 |

### 持仓数据
| 函数 | 说明 |
|------|------|
| `get_holdings` | 基金持仓 |
| `get_stock_change` | 重大持仓变动 |
| `get_asset_allocation` | 资产配置 |
| `get_industry_allocation` | 行业配置 |

### 衍生指标
| 函数 | 说明 |
|------|------|
| `get_snapshot` | 最新衍生数据（30+指标） |
| `get_indicators` | 衍生指标时间序列 |

### 基金经理
| 函数 | 说明 |
|------|------|
| `get_manager` | 管理信息 |
| `get_manager_info` | 经理背景 |
| `get_manager_indicators` | 经理衍生指标 |

### ETF
| 函数 | 说明 |
|------|------|
| `get_etf_components` | 申购赎回清单 |
| `get_etf_cash_components` | 现金差额 |

---

## 六、A股财务数据

### 财务数据点-in-time
```python
get_pit_financials_ex(order_book_ids, fields, start_quarter, end_quarter,
                      date=None, statements='latest', market='cn')
```
- `statements='latest'`: 取最新财报
- `statements='all'`: 取所有财报

### 财务指标
```python
get_factor(order_book_ids, factor, start_date=None, end_date=None, market='cn')
```

### 数据后缀含义
- `_mrq_n`: 单季度
- `_ttm_n`: 滚动12个月
- `_lyr_n`: 年报
- `_lf`: 最近一期

---

## hqdata适配器关键映射

### 代码格式转换
```
Ricequant: 000001.XSHE / 600000.XSHG
hqdata:    000001.SZ   / 600000.SH
转换: id_convert('000001.SZ') → '000001.XSHE'
     id_convert('000001.XSHE', to='normal') → '000001.SZ'
```

### 频率映射
```
hqdata: "day"   → RQData: '1d'
hqdata: "week"  → RQData: '1w'
hqdata: "1m"    → RQData: '1m'
hqdata: "5m"    → RQData: '5m'
hqdata: "15m"   → RQData: '15m'
hqdata: "30m"   → RQData: '30m'
hqdata: "60m"   → RQData: '60m'
```

### 列名映射
```
RQData: total_turnover → hqdata: amount
RQData: prev_close     → hqdata: pre_close
RQData: change/pct_change → 需手动计算
```

---

## 关键约束
- 不支持月线 (`month`)
- tick 数据可用但需注意格式
- 频率 `'1d'` 相当于日线
- 期货/期权有独立的 `rq.futures`/`rq.options` 子模块
