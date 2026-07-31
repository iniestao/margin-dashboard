"""云端数据更新脚本 —— 供 GitHub Actions 每日调用（只更新数据，不启动看板）

用法:
    python cloud_update.py

说明:
    1. 增量拉取近 N 个交易日的融资融券数据（SSE + SZSE）
    2. 按指数成分股聚合，写入 data/aggregated/
    3. 拉取天数可用环境变量 LOOKBACK_DAYS 覆盖（云端默认 45 天，够算 30 日变化）
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd

from config import MARGIN_DIR, discover_all_indices
from index_loader import load_etf_mapping
from data_fetcher import fetch_margin_batch, _get_trading_dates
from aggregator import aggregate_all_indices

# 云端只需保留 45 个交易日（覆盖 30 日变化 + 缓冲）
LOOKBACK = int(os.environ.get("LOOKBACK_DAYS", "45"))


def _cached_dates():
    dates = set()
    for f in MARGIN_DIR.glob("*.parquet"):
        parts = f.stem.split("_")
        if len(parts) >= 2:
            dates.add(parts[1])
    return dates


def _load_margin_cache():
    dfs = [pd.read_parquet(f) for f in MARGIN_DIR.glob("*.parquet")]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def main():
    all_indices = discover_all_indices()
    index_codes = [c for c, _ in all_indices]

    # 1. 增量拉取融资融券
    cached = _cached_dates()
    all_dates = set(_get_trading_dates(LOOKBACK))
    missing = sorted(all_dates - cached, reverse=True)

    if missing:
        print(f">>> 融资融券缺少 {len(missing)} 个交易日: {missing[0]} ~ {missing[-1]}")
        fetch_margin_batch(missing)
    else:
        print(f">>> 融资融券缓存完整 ({len(cached)} 个交易日)")

    # 2. 重新聚合
    margin_all = _load_margin_cache()
    if margin_all.empty:
        print("错误: 融资融券数据为空")
        sys.exit(1)

    etf_map = load_etf_mapping()
    empty_df = pd.DataFrame()
    print(f"\n>>> 开始聚合 {len(index_codes)} 个指数...")
    results = aggregate_all_indices(index_codes, margin_all, empty_df, empty_df, etf_map)
    print(f"聚合完成: {len(results)}/{len(index_codes)} 个指数")

    for code, df in sorted(results.items()):
        name = df["index_name"].iloc[0] if "index_name" in df.columns else code
        latest = df.sort_values("trade_date").iloc[-1]
        rz = latest.get("total_rz_balance", 0)
        dt = latest.get("trade_date", "-")
        print(f"  {name}: 融资余额 {rz/1e8:,.1f}亿 ({dt})")

    print("\n>>> 云上数据更新完成")


if __name__ == "__main__":
    main()
