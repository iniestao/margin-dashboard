"""数据拉取模块——融资融券、资金流向、流通市值，带本地缓存与增量更新"""

import os
import time
import json
import akshare as ak
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta

from config import MARGIN_DIR, FUND_FLOW_DIR, MARKET_CAP_DIR, REQUEST_INTERVAL
from code_mapper import to_akshare_margin, to_akshare_fund_flow, to_akshare_hist_163


# ============================================================
#  融资融券
# ============================================================

def _get_trading_dates(n_days: int = 100) -> list[str]:
    """生成近 N 个交易日列表（YYYYMMDD），从 AKShare 交易日历获取"""
    try:
        cal = ak.tool_trade_date_hist_sina()
        # 转为 YYYYMMDD 字符串，过滤掉未来日期
        today_str = datetime.now().strftime("%Y%m%d")
        dates = []
        for d in cal["trade_date"].unique():
            if hasattr(d, "strftime"):
                ds = d.strftime("%Y%m%d")
            else:
                ds = str(d).replace("-", "")
            if ds <= today_str:
                dates.append(ds)
        dates = sorted(dates, reverse=True)[:n_days]
        return dates
    except Exception:
        # 降级：用自然日估算
        today = datetime.now()
        dates = []
        d = today
        while len(dates) < n_days:
            ds = d.strftime("%Y%m%d")
            if d.weekday() < 5:
                dates.append(ds)
            d -= timedelta(days=1)
        return dates


def _fetch_sse_margin_detail_direct(date_str: str) -> pd.DataFrame:
    """直接查询上交所融资融券明细 API"""
    url = "https://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
    params = {
        "isPagination": "true",
        "tabType": "mxtype",
        "detailsDate": date_str,
        "pageHelp.pageSize": "5000",
        "pageHelp.pageCount": "50",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "21",
    }
    headers = {
        "Referer": "https://www.sse.com.cn/",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.get(url, params=params, headers=headers, timeout=30)
    data = r.json()
    raw = data.get("result", [])
    if not raw:
        return pd.DataFrame()

    # SSE 返回 list[dict]，字段名: stockCode, rzye, rzmre, opDate, securityAbbr
    records = []
    for row in raw:
        try:
            code = str(row.get("stockCode", "")).strip().zfill(6)
            rzye = row.get("rzye")
            if rzye is None:
                continue
            records.append({
                "trade_date": str(row.get("opDate", date_str)).replace("-", ""),
                "stock_code": code,
                "rzye": float(rzye),
                "rzmre": float(row.get("rzmre", 0) or 0),
                "rzche": float(row.get("rzche", 0) or 0),
            })
        except (ValueError, TypeError):
            continue

    return pd.DataFrame(records)


def fetch_margin_single_date(date_str: str) -> pd.DataFrame:
    """拉取单日全市场融资融券明细。

    完整性规则（重要）：
        SSE 与 SZSE 两个交易所的数据必须同时成功才保存缓存；
        若只有一方返回（数据发布延迟/单边缺失），不保存任何文件，
        该日期视为未完成，等待下次运行自动重试。
        已存在的单边缓存文件会被删除后重新拉取。
    """
    MARGIN_DIR.mkdir(parents=True, exist_ok=True)
    cache_sse = MARGIN_DIR / f"sh_{date_str}.parquet"
    cache_sz = MARGIN_DIR / f"sz_{date_str}.parquet"

    # 已有完整缓存（两边都在）→ 直接读
    if cache_sse.exists() and cache_sz.exists():
        return pd.concat([pd.read_parquet(cache_sse), pd.read_parquet(cache_sz)],
                         ignore_index=True)

    # 只有一边的残缺缓存 → 删掉重拉（可能是历史遗留的单边文件）
    for p in (cache_sse, cache_sz):
        if p.exists():
            print(f"  [融资融券] {date_str} 缓存不完整(仅单边)，删除重拉: {p.name}")
            p.unlink()

    # ── 拉取 SSE ──
    df_sse = pd.DataFrame()
    try:
        df_sse = _fetch_sse_margin_detail_direct(date_str)
    except Exception as e:
        print(f"  [融资融券] SSE {date_str} 失败: {e}")
    time.sleep(REQUEST_INTERVAL)

    # ── 拉取 SZSE ──
    df_sz = pd.DataFrame()
    try:
        df_raw = ak.stock_margin_detail_szse(date=date_str)
        if df_raw is not None and not df_raw.empty:
            df_sz = pd.DataFrame()
            df_sz["stock_code"] = df_raw["证券代码"].astype(str).str.zfill(6)
            for src, dst in [("融资余额", "rzye"), ("融资买入额", "rzmre"),
                             ("融券余量", "rqyl"), ("融券余额", "rqye")]:
                if src in df_raw.columns:
                    df_sz[dst] = pd.to_numeric(df_raw[src], errors="coerce")
            # SZSE 没有 trade_date 列，用参数 date_str
            df_sz["trade_date"] = date_str
    except Exception as e:
        print(f"  [融资融券] SZSE {date_str} 失败: {e}")
    time.sleep(REQUEST_INTERVAL)

    # 两个交易所都为空 → 数据未公布或非交易日，不缓存
    if df_sse.empty and df_sz.empty:
        print(f"  [融资融券] {date_str}: 两所均无数据（未公布或非交易日）")
        return pd.DataFrame()

    # 只有一方成功 → 数据不完整，不保存缓存，下次重试
    if df_sse.empty or df_sz.empty:
        side = "SSE" if not df_sse.empty else "SZSE"
        print(f"  [融资融券] {date_str}: 仅 {side} 返回数据，不完整，不保存缓存，等待下次重试")
        return pd.DataFrame()

    # 两所都成功 → 保存缓存并返回
    df_sse.to_parquet(cache_sse, index=False)
    df_sz.to_parquet(cache_sz, index=False)
    result = pd.concat([df_sse, df_sz], ignore_index=True)
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce")
    return result


def fetch_margin_batch(dates: list[str]) -> pd.DataFrame:
    """批量拉取多个日期的融资融券数据"""
    all_data = []
    for i, d in enumerate(dates):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  融资融券: {i+1}/{len(dates)} ({d})")
        df = fetch_margin_single_date(d)
        if not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# ============================================================
#  资金流向
# ============================================================

def fetch_fund_flow_single(ts_code: str) -> pd.DataFrame | None:
    """拉取单只股票近100交易日资金流向，缓存到 parquet"""
    FUND_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    parts = to_akshare_fund_flow(ts_code)
    stock_num = parts["stock"]
    market = parts["market"]
    cache_path = FUND_FLOW_DIR / f"{ts_code.replace('.', '_')}.parquet"

    if cache_path.exists():
        return pd.read_parquet(cache_path)

    try:
        df = ak.stock_individual_fund_flow(stock=stock_num, market=market)
        if df is None or df.empty:
            return None

        # 标准化列名
        col_map = {}
        for c in df.columns:
            if "日期" in c:
                col_map[c] = "trade_date"
            elif "主力净流入-净额" in c:
                col_map[c] = "main_net_amount"
            elif "主力净流入-净占比" in c:
                col_map[c] = "main_net_pct"
            elif "超大单净流入-净额" in c:
                col_map[c] = "elg_net_amount"
            elif "大单净流入-净额" in c:
                col_map[c] = "lg_net_amount"
            elif "中单净流入-净额" in c:
                col_map[c] = "md_net_amount"
            elif "小单净流入-净额" in c:
                col_map[c] = "sm_net_amount"
            elif "收盘价" in c:
                col_map[c] = "close"
            elif "涨跌幅" in c:
                col_map[c] = "pct_chg"

        df = df.rename(columns=col_map)
        df["ts_code"] = ts_code
        keep = [c for c in ["trade_date", "ts_code", "main_net_amount",
                             "main_net_pct", "elg_net_amount", "lg_net_amount",
                             "close", "pct_chg"] if c in df.columns]
        df = df[keep].copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        for col in ["main_net_amount", "elg_net_amount", "lg_net_amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df.to_parquet(cache_path, index=False)
        time.sleep(REQUEST_INTERVAL)
        return df

    except Exception as e:
        print(f"  [资金流向] {ts_code} 失败: {e}")
        return None


def fetch_fund_flow_batch(ts_codes: list[str], verbose: bool = True) -> pd.DataFrame:
    """批量拉取多只股票的资金流向"""
    all_data = []
    total = len(ts_codes)
    for i, code in enumerate(ts_codes):
        if verbose and ((i + 1) % 20 == 0 or i == 0):
            print(f"  资金流向: {i+1}/{total}")
        df = fetch_fund_flow_single(code)
        if df is not None and not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# ============================================================
#  流通市值 (网易源)
# ============================================================

def fetch_market_cap_single(ts_code: str, start_date: str = "20250101",
                            end_date: str = "20260729") -> pd.DataFrame | None:
    """拉取单只股票的每日流通市值"""
    MARKET_CAP_DIR.mkdir(parents=True, exist_ok=True)
    symbol = to_akshare_hist_163(ts_code)
    cache_path = MARKET_CAP_DIR / f"{ts_code.replace('.', '_')}.parquet"

    if cache_path.exists():
        return pd.read_parquet(cache_path)

    try:
        df = ak.stock_zh_a_hist_163(symbol=symbol, start_date=start_date,
                                     end_date=end_date)
        if df is None or df.empty:
            return None

        col_map = {}
        for c in df.columns:
            if "日期" in c:
                col_map[c] = "trade_date"
            elif "收盘价" in c:
                col_map[c] = "close"
            elif "流通市值" in c:
                col_map[c] = "float_market_cap"
            elif "总市值" in c:
                col_map[c] = "total_market_cap"
            elif "成交金额" in c:
                col_map[c] = "amount"
            elif "换手率" in c:
                col_map[c] = "turnover"

        df = df.rename(columns=col_map)
        df["ts_code"] = ts_code
        keep = [c for c in ["trade_date", "ts_code", "close", "float_market_cap",
                             "total_market_cap", "amount", "turnover"] if c in df.columns]
        df = df[keep].copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        for col in ["float_market_cap", "total_market_cap", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df.to_parquet(cache_path, index=False)
        time.sleep(REQUEST_INTERVAL)
        return df

    except Exception as e:
        print(f"  [流通市值] {ts_code} 失败: {e}")
        return None


def fetch_market_cap_batch(ts_codes: list[str], start_date: str = "20250101",
                           end_date: str = "20260729",
                           verbose: bool = True) -> pd.DataFrame:
    """批量拉取流通市值"""
    all_data = []
    total = len(ts_codes)
    for i, code in enumerate(ts_codes):
        if verbose and ((i + 1) % 20 == 0 or i == 0):
            print(f"  流通市值: {i+1}/{total}")
        df = fetch_market_cap_single(code, start_date, end_date)
        if df is not None and not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# ============================================================
#  一站式拉取入口
# ============================================================

def fetch_all_data(stock_codes: list[str], n_trading_days: int = 100) -> dict:
    """一站式拉取：融资融券 + 资金流向 + 流通市值

    Returns:
        {"margin": DataFrame, "fund_flow": DataFrame, "market_cap": DataFrame}
    """
    print("=== 1/3 融资融券 ===")
    dates = _get_trading_days(n_trading_days)
    margin = fetch_margin_batch(dates)
    print(f"  融资融券: {len(margin)} 条记录")

    print("\n=== 2/3 资金流向（按股拉取，首次较慢）===")
    fund_flow = fetch_fund_flow_batch(stock_codes)
    print(f"  资金流向: {len(fund_flow)} 条记录")

    print("\n=== 3/3 流通市值 ===")
    market_cap = fetch_market_cap_batch(stock_codes)
    print(f"  流通市值: {len(market_cap)} 条记录")

    return {"margin": margin, "fund_flow": fund_flow, "market_cap": market_cap}
