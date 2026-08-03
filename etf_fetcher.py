"""ETF 份额数据拉取模块 —— 按交易日增量拉取沪深两市 ETF 份额，本地缓存

数据源（均为交易所官方，免费无 token）：
    沪市: akshare fund_etf_scale_sse(date)  ← query.sse.com.cn 基金规模接口
    深市: akshare fund_scale_daily_szse     ← szse.cn 基金规模报表

缓存规则（与融资数据一致）：
    - 每个交易日一个文件，沪市 sh_YYYYMMDD.parquet、深市 sz_YYYYMMDD.parquet
    - 沪深两市都成功才保存；单边缺失不保存，等待下次重试（历史日期可回补）
"""

import time

import akshare as ak
import pandas as pd
from pathlib import Path

from config import ETF_SCALE_DIR, REQUEST_INTERVAL


def _cached_dates() -> set:
    """返回沪深两市都完整的日期集合"""
    def side(prefix: str) -> set:
        s = set()
        for f in ETF_SCALE_DIR.glob(f"{prefix}_*.parquet"):
            parts = f.stem.split("_")
            if len(parts) >= 2:
                s.add(parts[1])
        return s
    return side("sh") & side("sz")


def _fetch_sse(date_str: str) -> pd.DataFrame:
    """拉取沪市 ETF 份额（单日快照）"""
    df = ak.fund_etf_scale_sse(date=date_str)
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame({
        "trade_date": date_str,
        "fund_code": df["基金代码"].astype(str).str.zfill(6),
        "fund_name": df["基金简称"].astype(str),
        "total_share": pd.to_numeric(df["基金份额"], errors="coerce"),
    })
    if "ETF类型" in df.columns:
        out["etf_type"] = df["ETF类型"].astype(str)
    return out.dropna(subset=["fund_code"])


def _fetch_szse(date_str: str) -> pd.DataFrame:
    """拉取深市 ETF 份额（单日）"""
    df = ak.fund_scale_daily_szse(start_date=date_str, end_date=date_str, symbol="ETF")
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame({
        "trade_date": date_str,
        "fund_code": df["基金代码"].astype(str).str.zfill(6),
        "fund_name": df["基金简称"].astype(str),
        "total_share": pd.to_numeric(df["基金份额"], errors="coerce"),
    })
    return out.dropna(subset=["fund_code"])


def fetch_etf_scale_single_date(date_str: str) -> pd.DataFrame:
    """拉取单日沪深 ETF 份额；双所齐全才保存缓存，单边缺失不保存（下次重试）"""
    ETF_SCALE_DIR.mkdir(parents=True, exist_ok=True)
    cache_sh = ETF_SCALE_DIR / f"sh_{date_str}.parquet"
    cache_sz = ETF_SCALE_DIR / f"sz_{date_str}.parquet"

    # 已有完整缓存 → 直接返回
    if cache_sh.exists() and cache_sz.exists():
        return pd.concat([pd.read_parquet(cache_sh), pd.read_parquet(cache_sz)],
                         ignore_index=True)

    # 单边残缺缓存 → 删除重拉
    for p in (cache_sh, cache_sz):
        if p.exists():
            print(f"  [ETF份额] {date_str} 缓存不完整(仅单边)，删除重拉: {p.name}")
            p.unlink()

    df_sh = pd.DataFrame()
    try:
        df_sh = _fetch_sse(date_str)
    except Exception as e:
        print(f"  [ETF份额] 沪市 {date_str} 失败: {e}")
    time.sleep(REQUEST_INTERVAL)

    df_sz = pd.DataFrame()
    try:
        df_sz = _fetch_szse(date_str)
    except Exception as e:
        print(f"  [ETF份额] 深市 {date_str} 失败: {e}")
    time.sleep(REQUEST_INTERVAL)

    if df_sh.empty and df_sz.empty:
        print(f"  [ETF份额] {date_str}: 两所均无数据（未公布或非交易日）")
        return pd.DataFrame()
    if df_sh.empty or df_sz.empty:
        side = "沪市" if not df_sh.empty else "深市"
        print(f"  [ETF份额] {date_str}: 仅 {side} 返回数据，不完整，不保存缓存，等待下次重试")
        return pd.DataFrame()

    # 统一列并保存
    cols = ["trade_date", "fund_code", "fund_name", "total_share"]
    df_sh[cols].to_parquet(cache_sh, index=False)
    df_sz[cols].to_parquet(cache_sz, index=False)
    print(f"  [ETF份额] {date_str}: 沪市 {len(df_sh)} 只 + 深市 {len(df_sz)} 只")
    return pd.concat([df_sh[cols], df_sz[cols]], ignore_index=True)


def fetch_etf_scale_batch(dates: list[str]) -> pd.DataFrame:
    """批量拉取多个交易日的 ETF 份额（仅缺失日期）"""
    all_data = []
    cached = _cached_dates()
    missing = sorted(set(dates) - cached, reverse=True)
    if not missing:
        print(f">>> ETF份额缓存完整（{len(cached)} 个交易日）")
        return pd.DataFrame()
    print(f">>> ETF份额缺少 {len(missing)} 个交易日: {missing[0]} ~ {missing[-1]}")
    for i, d in enumerate(missing):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  ETF份额: {i+1}/{len(missing)} ({d})")
        df = fetch_etf_scale_single_date(d)
        if not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def load_etf_scale_cache() -> pd.DataFrame:
    """读取全部已缓存的 ETF 份额（用于看板展示）"""
    files = sorted(ETF_SCALE_DIR.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    return df.dropna(subset=["trade_date"])


if __name__ == "__main__":
    # 自测：拉最近 5 个交易日
    from data_fetcher import _get_trading_dates
    dates = _get_trading_dates(5)
    print("目标日期:", dates)
    fetch_etf_scale_batch(dates)
    print("\n缓存状态:", sorted(_cached_dates()))
