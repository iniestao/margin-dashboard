"""聚合引擎——将个股数据按指数成分股汇总，生成指数级资金指标"""

import pandas as pd
import numpy as np
from pathlib import Path

from config import AGGREGATED_DIR, FOCUS_INDICES, FOCUS_NAMES
from index_loader import load_index_weights, load_etf_mapping


def aggregate_index(
    index_code: str,
    margin_df: pd.DataFrame,
    fund_flow_df: pd.DataFrame,
    market_cap_df: pd.DataFrame,
    index_name: str = "",
) -> pd.DataFrame:
    """对单个指数做 融资+资金流向+融资占比 聚合。

    Args:
        index_code: 如 "000300.SH"
        margin_df: 全市场融资融券数据 (trade_date, stock_code, rzye, rzmre, ...)
        fund_flow_df: 全市场资金流向数据 (trade_date, ts_code, main_net_amount, ...)
        market_cap_df: 全市场流通市值数据 (trade_date, ts_code, float_market_cap, amount, ...)
        index_name: 指数中文名

    Returns:
        DataFrame，每行=一个交易日，列:
          trade_date, index_code, index_name,
          total_rz_balance(融资余额总和), total_rz_buy(融资买入总和),
          融资占比(%), total_main_net(主力净流入总和), 资金强度(%),
          成分股数, 有融资数据股数
    """
    # 加载成分股
    weights = load_index_weights(index_code)
    if weights.empty:
        return pd.DataFrame()

    # 成分股代码是 "600519.SH" 格式，margin 是 "600519"，统一去后缀
    constituents = set(c.split(".")[0] for c in weights["stock_code"].unique())

    # ── 融资融券聚合 ──
    margin = margin_df[margin_df["stock_code"].isin(constituents)].copy()
    if not margin.empty:
        agg_dict = {"total_rz_balance": ("rzye", "sum"), "total_rz_buy": ("rzmre", "sum"),
                     "margin_stock_count": ("stock_code", "nunique")}
        if "rzche" in margin.columns:
            agg_dict["total_rz_repay"] = ("rzche", "sum")
        margin_agg = margin.groupby("trade_date").agg(**agg_dict).reset_index()
    else:
        margin_agg = pd.DataFrame()

    # ── 资金流向聚合 ──
    if fund_flow_df is not None and not fund_flow_df.empty and "ts_code" in fund_flow_df.columns:
        ff = fund_flow_df[fund_flow_df["ts_code"].str.split(".").str[0].isin(constituents)].copy()
        if not ff.empty:
            ff_agg = ff.groupby("trade_date").agg(
                total_main_net=("main_net_amount", "sum"),
                fund_flow_stock_count=("ts_code", "nunique"),
            ).reset_index()
        else:
            ff_agg = pd.DataFrame()
    else:
        ff_agg = pd.DataFrame()

    # ── 流通市值 + 成交额聚合 ──
    if market_cap_df is not None and not market_cap_df.empty and "ts_code" in market_cap_df.columns:
        mc = market_cap_df[market_cap_df["ts_code"].str.split(".").str[0].isin(constituents)].copy()
        if not mc.empty:
            mc_agg = mc.groupby("trade_date").agg(
                total_float_cap=("float_market_cap", "sum"),
                total_amount=("amount", "sum"),
                mc_stock_count=("ts_code", "nunique"),
            ).reset_index()
        else:
            mc_agg = pd.DataFrame()
    else:
        mc_agg = pd.DataFrame()

    # ── 合并 ──
    result = None
    for part in [margin_agg, ff_agg, mc_agg]:
        if part.empty:
            continue
        if result is None:
            result = part
        else:
            result = result.merge(part, on="trade_date", how="outer")

    if result is None or result.empty:
        return pd.DataFrame()

    # ── 计算比率 ──
    result["index_code"] = index_code
    result["index_name"] = index_name
    result["constituent_count"] = len(constituents)

    # 融资占比 = 总融资余额 / 总流通市值 * 100
    if "total_rz_balance" in result.columns and "total_float_cap" in result.columns:
        result["margin_ratio_pct"] = (
            result["total_rz_balance"] / result["total_float_cap"] * 100
        )

    # 资金强度 = 总主力净流入 / 总成交额 * 100
    if "total_main_net" in result.columns and "total_amount" in result.columns:
        result["flow_intensity_pct"] = (
            result["total_main_net"] / result["total_amount"] * 100
        )

    return result.sort_values("trade_date")


def aggregate_all_indices(
    index_codes: list[str],
    margin_df: pd.DataFrame,
    fund_flow_df: pd.DataFrame,
    market_cap_df: pd.DataFrame,
    etf_map: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """批量聚合所有指数。

    Returns:
        {index_code: DataFrame} 字典
    """
    results = {}
    for i, code in enumerate(index_codes):
        # 从ETF映射获取中文名
        name = ""
        if etf_map is not None:
            matched = etf_map[etf_map["index_code"] == code]
            if not matched.empty:
                name = matched.iloc[0]["etf_name"]
        if not name:
            name = FOCUS_NAMES.get(code, code)

        print(f"  聚合: [{i+1}/{len(index_codes)}] {code} ({name})")
        try:
            df = aggregate_index(code, margin_df, fund_flow_df, market_cap_df, name)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                results[code] = df
                # 缓存到本地
                cache_path = AGGREGATED_DIR / f"{code.replace('.', '_')}.parquet"
                AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)
                df.to_parquet(cache_path, index=False)
        except Exception as e:
            print(f"    [跳过] {code}: {e}")

    return results


def load_cached_aggregation(index_code: str) -> pd.DataFrame | None:
    """从缓存加载已聚合的数据"""
    cache_path = AGGREGATED_DIR / f"{index_code.replace('.', '_')}.parquet"
    if cache_path.exists():
        return pd.read_parquet(cache_path)
    return None


def get_latest_snapshot(results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """从聚合结果中提取最新一日的摘要，用于首页雷达卡片。

    返回每个指数最新日期的：融资余额、融资占比、主力净流入、资金强度
    """
    rows = []
    for code, df in results.items():
        if df.empty:
            continue
        latest = df.sort_values("trade_date").iloc[-1]
        row = {
            "index_code": code,
            "index_name": latest.get("index_name", code),
            "trade_date": latest["trade_date"],
            "total_rz_balance": latest.get("total_rz_balance"),
            "margin_ratio_pct": latest.get("margin_ratio_pct"),
            "total_main_net": latest.get("total_main_net"),
            "flow_intensity_pct": latest.get("flow_intensity_pct"),
            "constituent_count": latest.get("constituent_count"),
        }
        rows.append(row)
    return pd.DataFrame(rows)
