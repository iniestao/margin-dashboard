"""云端数据更新脚本 —— 供 GitHub Actions 每日调用（只更新数据，不启动看板）

用法:
    python cloud_update.py

说明:
    1. 增量拉取近 N 个交易日的融资融券数据（SSE + SZSE）
    2. 按指数成分股聚合，写入 data/aggregated/
    3. 拉取天数可用环境变量 LOOKBACK_DAYS 覆盖（云端默认 100 天，够算 30 日变化）
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

# 云端只需保留 100 个交易日（覆盖 30 日变化 + 缓冲）
LOOKBACK = int(os.environ.get("LOOKBACK_DAYS", "100"))


def _cached_dates():
    """返回 SSE 与 SZSE 两边都完整的日期（单边缓存视为未完成，会触发重拉）"""
    def _side_dates(prefix: str) -> set:
        s = set()
        for f in MARGIN_DIR.glob(f"{prefix}_*.parquet"):
            parts = f.stem.split("_")
            if len(parts) >= 2:
                s.add(parts[1])
        return s
    return _side_dates("sh") & _side_dates("sz")


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

    # 1.5 增量拉取 ETF 份额（与本地 run.py 逻辑一致）
    from etf_fetcher import fetch_etf_scale_batch, _cached_dates as etf_cached_dates_fn
    etf_cached = etf_cached_dates_fn()
    etf_missing = sorted(all_dates - etf_cached, reverse=True)
    if etf_missing:
        print(f">>> ETF份额缺少 {len(etf_missing)} 个交易日: {etf_missing[0]} ~ {etf_missing[-1]}")
        fetch_etf_scale_batch(etf_missing)
    else:
        print(f">>> ETF份额缓存完整 ({len(etf_cached)} 个交易日)")

    # 1.6 拉取 ETF 单位净值（国家队池，用于金额视图）
    from etf_fetcher import fetch_etf_nav, NATIONAL_TEAM_ETF
    print(f">>> 拉取 ETF 净值（{len(NATIONAL_TEAM_ETF)} 只国家队池）...")
    fetch_etf_nav(list(NATIONAL_TEAM_ETF.keys()))

    # 1.7 拉取全市场资金流向快照（东财）
    from fund_flow_fetcher import fetch_fund_flow_snapshot, fetch_fund_flow_history
    fetch_fund_flow_snapshot()

    # 1.8 资金流向历史回补（首次补齐过去交易日，与其他数据一致）
    from index_loader import get_all_available_indices, load_index_weights
    hist_codes = set()
    for _ic in get_all_available_indices():
        _w = load_index_weights(_ic)
        if not _w.empty:
            hist_codes.update(_w["stock_code"].astype(str).str.split(".").str[0])
    if hist_codes:
        fetch_fund_flow_history(sorted(hist_codes), sorted(all_dates))

    # 2. 重新聚合
    margin_all = _load_margin_cache()
    if margin_all.empty:
        print("错误: 融资融券数据为空")
        sys.exit(1)

    from fund_flow_fetcher import load_fund_flow_cache
    fund_flow_all = load_fund_flow_cache()
    etf_map = load_etf_mapping()
    empty_df = pd.DataFrame()
    print(f"\n>>> 开始聚合 {len(index_codes)} 个指数...")
    results = aggregate_all_indices(index_codes, margin_all, fund_flow_all, empty_df, etf_map)
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
