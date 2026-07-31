"""股票代码格式转换器。

CSV 使用 tushare 风格: "600519.SH" / "300750.SZ"
AKShare 各函数需要不同的格式:
  - stock_margin_detail_sse/szse: "600519" (纯数字)
  - stock_individual_fund_flow:   stock="600519", market="sh"
  - stock_zh_a_hist_163:          symbol="sh600519"
"""

import re
import pandas as pd


def parse_ts_code(code: str) -> tuple[str, str]:
    """将 '600519.SH' → ('sh', '600519')"""
    if not code or "." not in code:
        return ("", code)
    parts = code.split(".")
    num = parts[0]
    exchange = parts[1].lower()
    market = {"sh": "sh", "sz": "sz", "bj": "bj"}.get(exchange, exchange)
    return (market, num)


def to_akshare_margin(code: str) -> str:
    """→ '600519' (纯数字，用于融资融券)"""
    _, num = parse_ts_code(code)
    return num


def to_akshare_fund_flow(code: str) -> dict:
    """→ {'stock': '600519', 'market': 'sh'} (用于资金流向)"""
    market, num = parse_ts_code(code)
    return {"stock": num, "market": market}


def to_akshare_hist_163(code: str) -> str:
    """→ 'sh600519' (用于网易历史行情/流通市值)"""
    market, num = parse_ts_code(code)
    return f"{market}{num}"


def to_ts_code(stock_num: str, market: str) -> str:
    """反向: ('sh', '600519') → '600519.SH'"""
    return f"{stock_num}.{market.upper()}"


def batch_to_akshare_fund_flow(codes: list[str]) -> pd.DataFrame:
    """将一批 ts_code 转为 fund_flow 参数 DataFrame"""
    records = []
    for c in codes:
        m, n = parse_ts_code(c)
        records.append({"stock": n, "market": m, "ts_code": c})
    return pd.DataFrame(records)
