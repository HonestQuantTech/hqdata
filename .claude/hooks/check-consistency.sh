#!/bin/bash
# 读取 stdin JSON，检查被修改的文件路径，输出联动检查提醒

FILE_PATH=$(jq -r '.tool_input.file_path // ""' 2>/dev/null)

if echo "$FILE_PATH" | grep -qE "hqdata/sources/[^/]+\.py$"; then
    jq -n '{
        "additionalContext": "⚠️ 你修改了数据源适配器文件。请检查：\n1. hqdata/api.py — 接口签名是否需要同步更新？\n2. hqdata/sources/base.py — 抽象方法定义是否需要同步更新？\n3. 相关测试文件是否需要联动修改？"
    }'
elif echo "$FILE_PATH" | grep -qE "hqdata/api\.py$"; then
    jq -n '{
        "additionalContext": "⚠️ 你修改了 api.py。请检查：\n1. hqdata/sources/*.py — 各数据源适配器是否需要联动更新？\n2. hqdata/__init__.py — 导出是否需要更新？\n3. tests/ — 测试文件是否需要联动修改？"
    }'
fi

exit 0
