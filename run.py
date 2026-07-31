"""A股资金变化看板 - 一键入口"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from config import MARGIN_DIR, AGGREGATED_DIR, LOOKBACK_DAYS, discover_all_indices
from index_loader import load_etf_mapping
from data_fetcher import fetch_margin_batch, _get_trading_dates
from aggregator import aggregate_all_indices


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
    all_dates = set(_get_trading_dates(LOOKBACK_DAYS))
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
        return

    etf_map = load_etf_mapping()
    empty_df = pd.DataFrame()
    print(f"\n>>> 开始聚合 {len(index_codes)} 个指数...")
    results = aggregate_all_indices(index_codes, margin_all, empty_df, empty_df, etf_map)
    print(f"聚合完成: {len(results)}/{len(index_codes)} 个指数\n")

    for code, df in sorted(results.items()):
        name = df["index_name"].iloc[0] if "index_name" in df.columns else code
        latest = df.sort_values("trade_date").iloc[-1]
        rz = latest.get("total_rz_balance", 0)
        dt = latest.get("trade_date", "-")
        print(f"  {name}: 融资余额 {rz/1e8:,.1f}亿 ({dt})")

    # 3. 启动看板
    print("\n>>> 启动看板...")
    import subprocess, shutil
    streamlit_bin = shutil.which("streamlit") or shutil.which("streamlit.exe")
    if not streamlit_bin:
        print("错误: 未找到 streamlit")
        return
    app_path = Path(__file__).parent / "app.py"
    subprocess.run([streamlit_bin, "run", str(app_path), "--server.port=8501"])


if __name__ == "__main__":
    main()
