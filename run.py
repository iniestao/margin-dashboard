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
    all_dates = set(_get_trading_dates(LOOKBACK_DAYS))
    missing = sorted(all_dates - cached, reverse=True)

    if missing:
        print(f">>> 融资融券缺少 {len(missing)} 个交易日: {missing[0]} ~ {missing[-1]}")
        fetch_margin_batch(missing)
    else:
        print(f">>> 融资融券缓存完整 ({len(cached)} 个交易日)")

    # 1.5 增量拉取 ETF 份额
    from etf_fetcher import fetch_etf_scale_batch, _cached_dates as etf_cached_dates_fn
    etf_cached = etf_cached_dates_fn()
    etf_missing = sorted(all_dates - etf_cached, reverse=True)
    if etf_missing:
        print(f">>> ETF份额缺少 {len(etf_missing)} 个交易日: {etf_missing[0]} ~ {etf_missing[-1]}")
        fetch_etf_scale_batch(etf_missing)
    else:
        print(f">>> ETF份额缓存完整 ({len(etf_cached)} 个交易日)")

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
