"""ETF 份额/净值数据拉取模块 —— 按交易日增量拉取沪深两市 ETF 份额与单位净值

数据源（免费无 token）：
    沪市份额: akshare fund_etf_scale_sse(date)  ← query.sse.com.cn 基金规模接口
    深市份额: akshare fund_scale_daily_szse     ← szse.cn 基金规模报表
    单位净值: akshare fund_open_fund_info_em    ← 天天基金网（东财）净值披露

缓存规则（与融资数据一致）：
    - 份额：每个交易日一个文件，沪市 sh_YYYYMMDD.parquet、深市 sz_YYYYMMDD.parquet
    - 净值：每只 ETF 一个文件（全历史），data/etf_nav/{code}.parquet
    - 沪深两市都成功才保存；单边缺失不保存，等待下次重试（历史日期可回补）
"""

import time

import akshare as ak
import pandas as pd
from pathlib import Path

from config import ETF_SCALE_DIR, ETF_NAV_DIR, REQUEST_INTERVAL


# 国家队持仓 ETF 池 —— 中央汇金系 2025 年报披露完整重仓清单
# 依据：中国证券报/Wind（2025年末）：汇金投资持 21 只 + 汇金资管持 15 只（去重后 24 只宽基）
NATIONAL_TEAM_ETF = {
    # ── 沪深300 系（汇金持仓最重）──
    "510300": "华泰柏瑞300ETF",   # 投资+资管 82.76%
    "510310": "易方达300ETF",     # 投资+资管 ~85%
    "510330": "华夏300ETF",       # 投资+资管
    "159919": "嘉实300ETF",       # 投资+资管
    # ── 上证50 ──
    "510050": "华夏上证50ETF",    # 投资+资管 86.05%
    "510100": "易方达上证50ETF",  # 汇金投资 5.83%
    # ── 中证500 ──
    "510500": "南方中证500ETF",   # 投资+资管
    "512500": "华夏中证500ETF",   # 汇金投资（华夏汇金资管计划增持）
    "159922": "嘉实中证500ETF",   # 汇金投资
    # ── 创业板 ──
    "159915": "创业板ETF易方达",   # 投资+资管 54.03%
    "159952": "广发创业板ETF",    # 汇金投资
    "159977": "天弘创业板ETF",    # 汇金投资 2025Q4 增持
    # ── 科创板50 ──
    "588000": "华夏科创50ETF",    # 汇金投资+资管
    "588080": "易方达科创50ETF",  # 投资+资管
    "588050": "工银瑞信科创50ETF",  # 汇金投资
    # ── 中证1000 ──
    "512100": "南方中证1000ETF",  # 投资+资管
    "159845": "华夏中证1000ETF",  # 投资+资管
    "560010": "广发中证1000ETF",  # 投资+资管（>40%）
    "159629": "富国中证1000ETF",  # 投资+资管
    # ── 其他宽基 ──
    "510180": "华安上证180ETF",   # 汇金投资 91.93%
    "510230": "国泰上证180金融ETF",  # 汇金投资 77.59%
    "159901": "易方达深证100ETF", # 投资+资管
    "515800": "汇添富中证800ETF", # 汇金资管
    "560050": "汇添富MSCI中国A50ETF",  # 汇金资管
}


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


# ============================================================
#  ETF 单位净值（天天基金网）—— 用于计算 ETF 规模金额 = 份额 × 净值
# ============================================================

def fetch_etf_nav(codes: list[str]) -> pd.DataFrame:
    """拉取指定 ETF 的单位净值历史（全量覆盖写 data/etf_nav/{code}.parquet）

    净值数据源：天天基金网（fund.eastmoney.com），基金公司每日披露，历史可回补。
    缓存策略：若缓存最新净值日期已覆盖"最近已收盘交易日"则跳过，否则重拉（保证每日更新）。
    """
    from datetime import datetime
    from data_fetcher import _get_trading_dates

    ETF_NAV_DIR.mkdir(parents=True, exist_ok=True)
    # 目标 = 最近已收盘交易日（今天未收盘则取上一交易日）
    tdates = _get_trading_dates(10)
    today = datetime.now().strftime("%Y%m%d")
    target = tdates[0] if tdates else today
    if tdates and tdates[0] == today and datetime.now().hour < 15:
        target = tdates[1] if len(tdates) > 1 else tdates[0]
    target_dt = pd.to_datetime(target)

    all_data = []
    for i, code in enumerate(codes):
        cache = ETF_NAV_DIR / f"{code}.parquet"
        need_fetch = not cache.exists()
        if cache.exists():
            # 缓存已含最近交易日 → 跳过；否则重拉
            try:
                cached = pd.read_parquet(cache)
                latest_nav = pd.to_datetime(cached["nav_date"], errors="coerce").max()
                need_fetch = pd.isna(latest_nav) or latest_nav < target_dt
            except Exception:
                need_fetch = True
        if not need_fetch:
            all_data.append(pd.read_parquet(cache))
            continue
        try:
            nav = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            if nav is None or nav.empty:
                print(f"  [ETF净值] {code} 无数据")
                continue
            out = pd.DataFrame({
                "fund_code": code,
                "nav_date": pd.to_datetime(nav["净值日期"], errors="coerce"),
                "unit_nav": pd.to_numeric(nav["单位净值"], errors="coerce"),
            }).dropna(subset=["nav_date", "unit_nav"])
            if out.empty:
                print(f"  [ETF净值] {code} 解析为空")
                continue
            out.to_parquet(cache, index=False)
            all_data.append(out)
            print(f"  [ETF净值] {code}: {len(out)} 条 (最新 {out['nav_date'].max().date()})")
        except Exception as e:
            print(f"  [ETF净值] {code} 失败: {e}")
        time.sleep(REQUEST_INTERVAL)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def load_etf_nav_cache() -> pd.DataFrame:
    """读取全部已缓存的 ETF 净值（用于看板展示）"""
    files = sorted(ETF_NAV_DIR.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["nav_date"] = pd.to_datetime(df["nav_date"], errors="coerce")
    return df.dropna(subset=["nav_date"])


def compute_etf_amount(scale_df: pd.DataFrame, nav_df: pd.DataFrame) -> pd.DataFrame:
    """按日期把份额 × 净值 得到规模金额。

    返回 scale_df 基础上新增 amount(元)：amount = total_share × unit_nav
    """
    if scale_df is None or scale_df.empty or nav_df is None or nav_df.empty:
        return scale_df.copy() if scale_df is not None else pd.DataFrame()
    nav = nav_df.rename(columns={"nav_date": "trade_date"})
    merged = scale_df.merge(nav[["fund_code", "trade_date", "unit_nav"]],
                            on=["fund_code", "trade_date"], how="left")
    merged["amount"] = merged["total_share"] * merged["unit_nav"]
    return merged


if __name__ == "__main__":
    # 自测：拉最近 5 个交易日
    from data_fetcher import _get_trading_dates
    dates = _get_trading_dates(5)
    print("目标日期:", dates)
    fetch_etf_scale_batch(dates)
    print("\n缓存状态:", sorted(_cached_dates()))
