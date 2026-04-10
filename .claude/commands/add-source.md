# Add Data Source

新增数据源时，按以下步骤执行：

1. **阅读文档** — 仔细阅读目标数据源的 API 文档，理解所有参数格式，不做假设
2. **创建适配器** — 在 `hqdata/sources/` 下新建 `<name>.py`，继承 `BaseSource`
3. **实现四个方法** — `get_stock_list()`, `get_stock_bar()`, `get_index_list()`, `get_index_bar()`
4. **代码格式转换** — 确保输入输出均使用 hqdata 约定格式（`.SH/.SZ/.BJ`，`YYYYMMDD`）
5. **注册到 api.py** — 在 `init_source()` 中添加新数据源的分支
6. **更新 __init__.py** — 检查导出是否需要更新
7. **编写测试** — 在 `tests/test_<name>.py` 中编写单元测试（mock）和集成测试（跳过逻辑）
8. **更新 README** — 在数据源兼容性表格中添加新数据源
9. **运行测试** — `pytest tests/ -v`

参数描述一致性：symbol、frequency、date 等参数的 docstring 和 error message 必须与其他适配器完全一致。
