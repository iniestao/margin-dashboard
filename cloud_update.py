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
    #   每日跑批只做「当日/T-1 增量」：拉当日快照，失败仅告警、不触发全历史回补、不 exit(1)。
    #   历史残缺（如 5/08~8/07 那段）由 BACKFILL_HISTORY=1 在本机单独补，见 1.8。
    from fund_flow_fetcher import fetch_fund_flow_snapshot, fetch_fund_flow_history
    _snap_df = fetch_fund_flow_snapshot()
    if _snap_df.empty:
        print("⚠️ 资金流向当日快照为空（东财 push2 走 runner 常成片 502），本次跳过，下次跑批重试")

    # 1.8 资金流向历史回补（本机补历史专用，云端每日跑批默认关闭）
    #   背景：为修 9/24 那批历史残缺加的「全历史残缺检测 + 全市场回补」，把每日跑批也拖进了
    #   一个跑不完的回补黑洞——65 个历史日（含 3/16~4/01 接口窗口已过、永远补不齐的那段），
    #   5226 只 × 65 天逐只回补 ≈ 2 小时+，每次都被 90min timeout 取消（"绿着失败"）。
    #   解法：历史回补与每日增量解耦。只有显式设置 BACKFILL_HISTORY=1（本机执行）时才跑，
    #   云端每日跑批不再碰它，只补当日/T-1。
    ff_deficit = {}
    if os.environ.get("BACKFILL_HISTORY") == "1":
        from fund_flow_fetcher import _full_universe_threshold
        from config import FUND_FLOW_DIR, STOCK_UNIVERSE_CSV
        uni_codes = []
        try:
            _u = pd.read_csv(STOCK_UNIVERSE_CSV, dtype={"stock_code": str})
            uni_codes = sorted(set(_u["stock_code"].astype(str).str.split(".").str[0].str.zfill(6)))
            print(f">>> 资金流向回补清单：全市场 {len(uni_codes)} 只")
        except Exception as _e:
            print(f"⚠️ 全市场清单读取失败（{str(_e)[:60]}），跳过资金流回补")
        if uni_codes:
            _thresh = _full_universe_threshold()
            broken = []
            for _d in sorted(all_dates):
                _f = FUND_FLOW_DIR / f"ff_{_d}.parquet"
                if not _f.exists():
                    continue
                try:
                    if len(pd.read_parquet(_f)) < _thresh:
                        broken.append(_d)
                except Exception:
                    broken.append(_d)
            if broken:
                print(f">>> 检测到残缺资金流快照 {len(broken)} 天 {broken}（完整下限 {_thresh} 只），"
                      f"用全市场清单强制重拉")
            _ff_res = fetch_fund_flow_history(uni_codes, sorted(all_dates), overwrite_dates=broken or None)
            ff_deficit = (_ff_res or {}).get("deficit") or {}

    # 1.9 全市场成交集中度（拥挤度）更新：T-1 口径，缺失日用腾讯日线补齐（无缺失秒级跳过）
    from crowd_fetcher import ensure_crowd_history
    try:
        ensure_crowd_history()
    except Exception as e:
        print(f"⚠️ 拥挤度更新失败（不阻塞主流程）: {e}")

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

    # 数据完整性收口：仅在「本机补历史模式」（BACKFILL_HISTORY=1）下，回补后仍有残缺日才 exit(1)。
    # 云端每日跑批（默认）不再因历史残缺而变红——当日快照失败已在 1.7 告警并跳过，
    # 增量跑批的职责是「尽可能拉到当日数据」，不承担「补齐 65 天历史」这个不可能在 90min 内完成的任务。
    if ff_deficit:
        print(f"\n⚠️ 资金流历史回补后仍有 {len(ff_deficit)} 天残缺（完整下限 {_full_universe_threshold()} 只）:")
        for _d, _n in sorted(ff_deficit.items()):
            print(f"     {_d}: {_n} 只")
        print("   原因通常是访问东财不稳定（云端 runner 在境外，实测失败率可达 57%）；"
              "残缺日请在本机用 BACKFILL_HISTORY=1 分块温和回补。")
        if os.environ.get("BACKFILL_HISTORY") == "1":
            print("   >>> 本机补历史模式下判定为「数据不完整」，退出码 1。")
            sys.exit(1)


if __name__ == "__main__":
    main()
