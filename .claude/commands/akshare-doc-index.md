# AKShare Docs Index

## 文档地址
- 股票数据总览：https://akshare.akfamily.xyz/data/stock/stock.html
- 工具箱（交易日历等）：https://akshare.akfamily.xyz/data/tool/tool.html
- 源码仓库：https://github.com/akfamily/akshare（`pip show akshare` 找不到细节时，
  直接 `curl` 对应模块的 raw 文件比翻文档页快，见下方"源码定位"）

## 抓取方式

文档页是纯静态 HTML，`WebFetch` 对这个域名经常超时/连接失败，改用：

```bash
curl -s -A "Mozilla/5.0" https://akshare.akfamily.xyz/data/stock/stock.html -o /tmp/akshare_stock.html
```

拿到 HTML 后再用 Python 去标签转文本，因为文档页的表格是逐行纵向排列的（表头、类型、
描述各占一行），直接搜索接口名（`接口：xxx`）定位比读渲染后的表格更快。

## 当前接入范围

akshare 适配器（`AkshareSource`）现在只实现 `get_calendar`/`get_stock_list`。日线/复权因子/
实时快照三个接口（`stock_zh_a_hist`/`stock_zh_a_daily`/`stock_bid_ask_em`）经实测数据源不稳定
（抓取式接口、无官方限流、易被临时封 IP），已停止支持——调用对应 hqdata 方法会抛出
`NotImplementedError`。下方"股票日线"/"复权因子"/"实时快照"三节仅作历史记录保留，不代表当前
代码行为。

## hqdata 适配器使用的接口（按 hqdata 函数顺序）

### 交易日历 `tool_trade_date_hist_sina`
- 来源：新浪财经；无参数，一次性返回 1990-12-19 至今的全部交易日（`datetime.date` 列表）
- hqdata 用法：本地生成 `[start_date, end_date]` 的自然日序列，按是否在返回集合里标记
  `is_open`（做法与 ricequant 适配器一致，因为 ricequant 也没有区间标记接口）

### 股票列表（当前上市）
| 接口 | 交易所 | 关键参数 | 关键字段 |
|---|---|---|---|
| `stock_info_sh_name_code` | 上交所 | `symbol="主板A股"` / `"科创板"`（无 `date` 参数） | 证券代码、证券简称、上市日期 |
| `stock_info_sz_name_code` | 深交所 | `symbol="A股列表"` | 板块（主板/创业板）、A股代码、A股简称、A股上市日期 |
| `stock_info_bj_name_code` | 北交所 | 无参数 | 证券代码、证券简称、上市日期 |

三个接口只返回"当前仍在交易"的股票，没有历史快照参数。

### 股票列表（已退市，用于历史快照重建）
| 接口 | 交易所 | 关键参数 | 关键字段 |
|---|---|---|---|
| `stock_info_sh_delist` | 上交所 | `symbol="全部"`（含主板+科创板） | 公司代码、公司简称、上市日期、暂停上市日期 |
| `stock_info_sz_delist` | 深交所 | `symbol="终止上市公司"` | 证券代码、证券简称、上市日期、终止上市日期 |

**已知缺口：北交所没有对应的退市股票接口**，翻遍文档和源码
（`akshare/stock/stock_info.py`）都没有找到。hqdata 适配器对此不做补偿，直接在 README
里注明这个限制——某个历史交易日已退市的北交所股票不会出现在 `get_stock_list` 结果里。

B股需要手动排除：沪市 B 股代码以 `9` 开头，深市 B 股代码以 `2` 开头。板块（MB/GEM/STAR）
在"已退市"接口里没有字段返回，需要按代码前缀推断：`688` 开头是科创板，深市 `300/301/302`
开头是创业板，其余是主板。

### 股票日线 `stock_zh_a_hist`
- 来源：东方财富；`symbol` **只能传单个代码**，不带交易所后缀（如 `"000001"`）
- 参数：`period="daily"/"weekly"/"monthly"`，`start_date`/`end_date`（`YYYYMMDD`），
  `adjust=""`（不复权）/`"qfq"`（前复权）/`"hfq"`（后复权）
- 返回字段（中文列名）：日期、股票代码、开盘、收盘、最高、最低、成交量（手）、成交额（元）、
  振幅（%）、涨跌幅（%）、涨跌额（元）、换手率（%）
- **没有 `pre_close` 字段**，hqdata 适配器用 `收盘 - 涨跌额` 推导
- 底层是对东方财富 `push2his.eastmoney.com/api/qt/stock/kline/get` 的封装，这个 API 路径
  有明显的反爬拦截（根路径 `push2.eastmoney.com/` 能访问，但 `/api/qt/*` 路径在短时间内
  多次请求后会被 WAF 拒绝连接，报 `SSL: UNEXPECTED_EOF_WHILE_READING` 或 `ProxyError`），
  需要控制请求频率

### 复权因子 `stock_zh_a_daily`（adjust 参数用 `hfq-factor`）
- 来源：新浪财经；`symbol` **只能传单个代码**，带交易所前缀（小写，如 `"sz000001"`、
  `"sh600000"`、`"bj920082"`）
- 文档原话建议切换到 `stock_zh_a_hist`，但那个接口不提供复权因子，所以复权因子仍需用这个
  接口；文档同时提示"多次获取容易封禁 IP"
- 返回的是**除权除息事件表**（稀疏，一行一个事件），不是每日序列：列名 `date`/`hfq_factor`，
  事件之间的值保持不变，最早一行日期通常是 `1900-01-01`（基准值 1.0）
- hqdata 适配器取 `trade_date` 当天或之前最近一次事件的值，因为该值在事件之间保持不变

### 实时快照 `stock_bid_ask_em`
- 来源：东方财富；`symbol` **只能传单个代码**，不带交易所后缀
- 返回格式是 `item`/`value` 两列的竖排报价单，不是常规宽表；关键字段：
  `sell_1~sell_5`/`sell_1_vol~sell_5_vol`（卖1-5价/量）、`buy_1~buy_5`/`buy_1_vol~buy_5_vol`
  （买1-5价/量）、`最新`/`昨收`/`今开`/`最高`/`最低`/`总手`/`金额`
- 五档量（`sell_x_vol`/`buy_x_vol`）单位是**股**，需要除以 100 换算成"手"以匹配
  tushare/ricequant 的口径
- **没有交易所侧的真实报价时间字段**（不像 tushare 的 `DATE`/`TIME`，或 ricequant 的
  `tick.datetime`），hqdata 适配器的 `ets` 用本地抓取时刻代替，与 `lts` 相同
- 底层调用同样是 `push2.eastmoney.com/api/qt/stock/get`，同样有上面提到的反爬拦截风险

## 代码格式转换（hqdata ↔ akshare）

| 接口类别 | hqdata 格式 | akshare 格式 | 转换 |
|---|---|---|---|
| 日线 `stock_zh_a_hist` | `"000001.SZ"` | `"000001"` | 去掉交易所后缀 |
| 复权因子/`stock_zh_a_daily` | `"000001.SZ"` | `"sz000001"` | 后缀转小写前缀：SH→sh，SZ→sz，BJ→bj |
| 快照 `stock_bid_ask_em` | `"000001.SZ"` | `"000001"` | 去掉交易所后缀 |
| exchange | `SSE`/`SZE`/`BSE` | 无统一参数，靠三个不同接口分别取 | SSE→`stock_info_sh_name_code`，SZE→`stock_info_sz_name_code`，BSE→`stock_info_bj_name_code` |
| board | `MB`/`GEM`/`STAR`/`BSE` | 各接口字面不统一（"主板A股"/"科创板"/"主板"/"创业板"），或需要按代码前缀推断 | 见上方"股票列表"章节 |
| date | 日线用 `YYYYMMDD`；股票列表/退市列表返回 `datetime.date` 或 `"YYYY-MM-DD"` 字符串 | — | 统一转换为 `YYYYMMDD` |

## 关键约束（与 tushare/ricequant 的能力差距）

- **日线/复权因子/快照三个核心行情接口都只支持单 symbol**，hqdata 层面的多 symbol 查询
  在适配器内部退化为逐个串行 HTTP 请求（间隔 `AkshareSource._REQUEST_DELAY_SECONDS`），
  不适合像 tushare/ricequant 一样批量拉取全市场数据
- **没有官方限流值**，`akshare` 文档里对新浪接口明确写"重复运行会被暂时封 IP"；东方财富的
  `/api/qt/*` 路径也观察到实际的 WAF 拦截（本次接入调研中亲身遇到，见"股票日线"章节）
- **股票列表无历史快照参数**，历史股票池是靠本地合并"当前上市"+"已退市"两组接口、按
  `list_date <= trade_date < delist_date` 重建的，不是数据源原生支持
- **无需任何凭证**——`init_source("akshare")` 不需要 token/license
