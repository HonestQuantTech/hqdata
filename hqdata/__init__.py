"""hqdata - A股历史与实时行情数据统一接入、清洗与存储"""

from hqdata.config import *  # Load .env on import
from hqdata.api import init_source, get_tick

__all__ = ["init_source", "get_tick"]
