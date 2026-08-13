"""A股资金监控看板 —— Tab1: 融资额 / Tab2: ETF份额监控 / Tab3: 资金流向 / Tab4: 实时行情"""

import sys; sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

import streamlit as st
st.cache_data.clear()
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
from zoneinfo import ZoneInfo

from config import discover_all_indices, AGGREGATED_DIR, MARGIN_DIR, FOCUS_INDICES, FOCUS_NAMES
from index_loader import load_index_weights
from etf_fetcher import (load_etf_scale_cache as _load_etf_scale_cache,
                         load_etf_nav_cache as _load_etf_nav_cache,
                         compute_etf_amount,
                         NATIONAL_TEAM_ETF)
from fund_flow_fetcher import load_fund_flow_cache
from realtime_fetcher import (fetch_index_quotes, fetch_stock_flow_realtime,
                              index_secid, fetch_daily_amount_history,
                              fetch_index_kline_close,
                              aggregate_flow_realtime, build_index_map, get_proxies,
                              fetch_market_sentiment)

st.set_page_config(page_title="A股资金监控看板", page_icon="💰", layout="wide", initial_sidebar_state="collapsed")

# 实时行情参数
REFRESH_MS = 60_000       # 自动刷新间隔（毫秒）
VOLUME_RATIO = 1.3        # 放量阈值（实时成交额 / 基准 >= 该值标记放量）
_BJ = ZoneInfo("Asia/Shanghai")

# 中国A股配色
RED, GREEN = "#E24B4A", "#22A45D"
BG_CARD = "#FAFBFC"
BORDER = "#E8EAED"
TEXT_MAIN = "#333"
TEXT_SUB = "#888"
TEXT_MUTED = "#AAA"


def fmt_pct(v):
    return f"+{v:.2f}%" if v > 0 else f"{v:.2f}%"


def pct_color(v):
    return RED if v > 0 else GREEN


def fmt_amount(v: float) -> str:
    """金额格式化：>=1万亿显示万亿，否则显示亿"""
    if v is None or (isinstance(v, float) and (v != v)):
        return "N/A"
    if v >= 1e12:
        return f"{v / 1e12:.2f}万亿"
    return f"{v / 1e8:,.0f}亿"


@st.cache_data(ttl=3600)
def load_aggregated():
    dfs = {}
    for code, name in discover_all_indices():
        p = AGGREGATED_DIR / f"{code.replace('.', '_')}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                df["index_name"] = name
                dfs[code] = df
    return dfs


@st.cache_data(ttl=3600)
def load_margin_history():
    import glob as g
    files = sorted(g.glob(str(MARGIN_DIR / "*.parquet")))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


@st.cache_data(ttl=3600)
def compute_market_overview():
    """全市场融资概览：全市场（非仅指数成分）融资余额/买入额的历史汇总与关键指标"""
    margin_all = load_margin_history()
    if margin_all is None or margin_all.empty:
        return None
    if "rzye" not in margin_all.columns:
        return None

    df = margin_all.copy()
    daily = df.groupby("trade_date").agg(
        total_balance=("rzye", "sum"),
        total_buy=("rzmre", "sum"),
    ).sort_index()
    if daily.empty:
        return None

    latest = daily.iloc[-1]
    cur_bal = float(latest["total_balance"])

    def pct_chg(days):
        if len(daily) > days:
            prev = float(daily.iloc[-(days + 1)]["total_balance"])
            if prev and prev > 0:
                return (cur_bal - prev) / prev * 100
        return None

    return {
        "daily": daily,
        "latest_date": daily.index[-1],
        "balance": cur_bal,
        "buy": float(latest["total_buy"]),
        "chg5": pct_chg(5),
        "chg15": pct_chg(15),
        "chg30": pct_chg(30),
    }


def compute_changes(dfs, lb):
    rows = []
    for code, df in dfs.items():
        name = df["index_name"].iloc[0]
        df_s = df.sort_values("trade_date")
        # 融资列的最后非空值作为"最新"（聚合结果可能含资金流等更晚日期）
        bal = df_s["total_rz_balance"].dropna()
        if len(bal) < lb + 1:
            continue
        cur = df_s.loc[bal.index[-1]]
        prev = df_s.loc[bal.index[-(lb + 1)]]
        cv = float(cur["total_rz_balance"])
        pv = float(prev["total_rz_balance"])
        if pv == 0:
            continue
        cb = cur.get("total_rz_buy", 0) or 0
        pb = prev.get("total_rz_buy", 0) or 0
        rows.append({
            "指数": name, "代码": code,
            "融资变化%": round((cv - pv) / pv * 100, 2),
            "融资余额(亿)": round(cv / 1e8, 1),
            "融资买入(亿)": round(cb / 1e8, 1) if pd.notna(cb) else 0,
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("融资变化%", ascending=False)


# ============================================================
#  ETF 份额 / 金额
# ============================================================
# 国家队持仓 ETF 池定义见 etf_fetcher.py（NATIONAL_TEAM_ETF）


@st.cache_data(ttl=3600)
def load_etf_scale_data():
    return _load_etf_scale_cache()


def compute_etf_changes(etf_df: pd.DataFrame, lb: int) -> pd.DataFrame:
    """按周期 lb 计算每只 ETF 的份额变化"""
    if etf_df is None or etf_df.empty:
        return pd.DataFrame()
    pv = etf_df.pivot_table(index="trade_date", columns="fund_code",
                            values="total_share", aggfunc="sum").sort_index()
    if len(pv) <= lb:
        return pd.DataFrame()
    latest, prev = pv.iloc[-1], pv.iloc[-(lb + 1)]
    name_map = etf_df.groupby("fund_code")["fund_name"].last()

    rows = []
    for code in latest.index:
        cur = latest.get(code, np.nan)
        old = prev.get(code, np.nan)
        if pd.isna(cur) or pd.isna(old) or old == 0:
            continue
        rows.append({
            "代码": code,
            "名称": str(name_map.get(code, code)),
            f"{lb}日份额变化%": round((cur - old) / old * 100, 2),
            f"{lb}日份额变化(亿份)": round((cur - old) / 1e8, 2),
            "最新份额(亿份)": round(cur / 1e8, 2),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(f"{lb}日份额变化%", ascending=False)


def compute_etf_changes_multi(etf_df: pd.DataFrame) -> pd.DataFrame:
    """全市场 ETF 份额变化：最新份额 + 1/5/20 日变化%（一次给出，便于横向对比）"""
    if etf_df is None or etf_df.empty:
        return pd.DataFrame()
    pv = etf_df.pivot_table(index="trade_date", columns="fund_code",
                            values="total_share", aggfunc="sum").sort_index()
    if len(pv) < 2:
        return pd.DataFrame()
    latest = pv.iloc[-1]
    name_map = etf_df.groupby("fund_code")["fund_name"].last()
    rows = []
    for code in latest.index:
        cur = latest.get(code, np.nan)
        if pd.isna(cur):
            continue
        row = {"代码": code, "名称": str(name_map.get(code, code)),
               "最新份额(亿份)": round(cur / 1e8, 2)}
        for d, lbl in [(1, "1日变化%"), (5, "5日变化%"), (20, "20日变化%")]:
            if len(pv) > d:
                old = pv.iloc[-(d + 1)].get(code, np.nan)
                if pd.notna(old) and old != 0:
                    row[lbl] = round((cur - old) / old * 100, 2)
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("20日变化%", ascending=False, na_position="last")


def compute_nt_pool_stats(etf_df: pd.DataFrame, codes: list, value_col: str = "total_share") -> dict:
    """国家队池：按日加总全部池内 ETF 份额/金额，计算总量与各周期变化"""
    sub = etf_df[etf_df["fund_code"].isin(codes)]
    daily = sub.groupby("trade_date")[value_col].sum().sort_index()
    if daily.empty:
        return None
    latest = float(daily.iloc[-1])

    def delta(days):
        if len(daily) > days:
            return float(daily.iloc[-1]) - float(daily.iloc[-(days + 1)])
        return None

    def chg_pct(days):
        d = delta(days)
        if d is None:
            return None
        old = float(daily.iloc[-(days + 1)])
        return d / old * 100 if old else None

    return {"daily": daily, "latest": latest,
            "d1": delta(1), "chg5": chg_pct(5),
            "chg20": chg_pct(20), "chg60": chg_pct(60)}


def compute_nt_detail(etf_df: pd.DataFrame, codes: list,
                      value_col: str = "total_share", unit: str = "亿份") -> pd.DataFrame:
    """国家队池明细：每只 ETF 的最新份额/规模与 1/5/20/60 日绝对变化（unit: 亿份/亿元）"""
    sub = etf_df[etf_df["fund_code"].isin(codes)]
    pv = sub.pivot_table(index="trade_date", columns="fund_code",
                         values=value_col, aggfunc="sum").sort_index()
    if pv.empty:
        return pd.DataFrame()
    name_map = sub.groupby("fund_code")["fund_name"].last()
    latest = pv.iloc[-1]
    latest_lbl = f"最新{unit}"
    rows = []
    for code in codes:
        if code not in latest.index:
            continue
        cur = latest.get(code, np.nan)
        if pd.isna(cur):
            continue
        row = {"代码": code,
               "ETF": NATIONAL_TEAM_ETF.get(code, str(name_map.get(code, code))),
               latest_lbl: round(cur / 1e8, 2)}
        for d, lbl in [(1, f"1日变化({unit})"), (5, f"5日变化({unit})"),
                       (20, f"20日变化({unit})"), (60, f"60日变化({unit})")]:
            if len(pv) > d:
                old = pv.iloc[-(d + 1)].get(code, np.nan)
                if pd.notna(old):
                    row[lbl] = round((cur - old) / 1e8, 2)
        rows.append(row)
    return pd.DataFrame(rows)


def compute_flow_changes(results: dict, lb: int) -> pd.DataFrame:
    """每个指数近 lb 日成分股主力净流入累计（亿元）"""
    rows = []
    for code, df in results.items():
        if "total_main_net" not in df.columns:
            continue
        df_s = df.sort_values("trade_date")
        if df_s.empty:
            continue
        name = df_s["index_name"].iloc[0]
        if lb == 1:
            total = float(df_s.iloc[-1].get("total_main_net", 0) or 0)
        else:
            total = float(df_s.tail(lb)["total_main_net"].fillna(0).sum())
        rows.append({
            "指数": name, "代码": code,
            f"{lb}日主力净流入(亿)": round(total / 1e8, 2),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(f"{lb}日主力净流入(亿)", ascending=False)


def compute_flow_multi(results: dict, lb: int) -> pd.DataFrame:
    """每个指数近 lb 日 主力/中单/小单 净流入（亿元），一张表对比（周期跟随切换）"""
    label = "当日" if lb == 1 else f"{lb}日"
    rows = []
    for code, df in results.items():
        if "total_main_net" not in df.columns:
            continue
        df_s = df.sort_values("trade_date")
        if df_s.empty:
            continue
        name = df_s["index_name"].iloc[0]
        row = {"指数": name, "代码": code}
        for src, key in [("total_main_net", f"{label}主力(亿)"),
                         ("total_mid_net", f"{label}中单(亿)"),
                         ("total_small_net", f"{label}小单(亿)")]:
            if src not in df_s.columns:
                row[key] = None
                continue
            vals = df_s[src].fillna(0)
            t = float(vals.iloc[-1]) if lb == 1 else float(vals.tail(min(lb, len(vals))).sum())
            row[key] = round(t / 1e8, 2)
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(f"{label}主力(亿)", ascending=False, na_position="last")


def color_pct(v):
    if v is None or (isinstance(v, float) and (v != v)):
        return ""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    return f"color:{RED}" if v > 0 else f"color:{GREEN}"


def style_color(df: pd.DataFrame, cols: list):
    """对指定列应用红涨绿跌着色（兼容 pandas 2.1+ 的 Styler.map 与旧版 applymap）"""
    styler = df.style
    fn = getattr(styler, "map", None)
    if fn is None:
        fn = styler.applymap
    return fn(color_pct, subset=cols)


# ============================================================
#  实时行情：工具函数 + 限流加载（Tab4）
# ============================================================

def _now_bj() -> datetime:
    return datetime.now(_BJ)


def is_market_open(now: datetime | None = None) -> bool:
    """交易日 9:30-11:30 / 13:00-15:00 视为盘中；周末停刷（节假日留后续精确判断）"""
    now = now or _now_bj()
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return (9 * 60 + 30 <= t <= 11 * 60 + 30) or (13 * 60 <= t <= 15 * 60)


def _time_progress(now: datetime | None = None) -> float:
    """当日已交易分钟 / 240（全天 240 分钟），用于放量基准折算；非交易时段返回 1.0"""
    now = now or _now_bj()
    if now.weekday() >= 5:
        return 1.0
    t = now.hour * 60 + now.minute
    if 9 * 60 + 30 <= t <= 11 * 60 + 30:
        return (t - (9 * 60 + 30)) / 240.0
    if 13 * 60 <= t <= 15 * 60:
        return (120 + (t - 13 * 60)) / 240.0
    return 1.0


@st.cache_data(ttl=3600)
def load_ff_latest():
    """最近已收盘交易日资金流快照（只取 trade_date.max() 一天），供实时 vs 昨日对比"""
    ff = load_fund_flow_cache()
    if ff.empty:
        return pd.DataFrame()
    last = ff["trade_date"].max()
    return ff[ff["trade_date"] == last]


@st.cache_data(ttl=3600)
def build_index_map_cached():
    """成分股集合（全部约 147 个指数，权重文件相对稳定，可用 cache_data）"""
    all_indices = [c for c, _ in discover_all_indices()]
    return build_index_map(all_indices)


def _aggregate_yesterday(ff_latest: pd.DataFrame, index_map: dict) -> dict:
    """按指数聚合昨日快照主力净流入 → {index_code: 昨日主力净流入(元)}"""
    if ff_latest is None or ff_latest.empty:
        return {}
    flow_map = dict(zip(ff_latest["stock_code"], ff_latest["main_net_amount"].fillna(0)))
    yday = {}
    for code, stocks in index_map.items():
        vals = [flow_map.get(s) for s in stocks]
        present = [v for v in vals if v is not None]
        yday[code] = float(sum(present)) if present else 0.0
    return yday


def load_ssindex_close() -> pd.DataFrame:
    """上证指数日线收盘（session_state 当日缓存一次，与 rt_hist 模式一致）。

    注：app.py 顶部每次 rerun 清 st.cache_data，故不可用 cache_data；失败也缓存空表避免反复请求。
    """
    if "ssindex_close" in st.session_state:
        return st.session_state["ssindex_close"]
    proxies = get_proxies()
    df = fetch_index_kline_close(index_secid("000001.SH"), days=500, proxies=proxies)
    st.session_state["ssindex_close"] = df   # 失败也缓存（空表），与 rt_hist 一致
    return df


def _realtime_load_all() -> dict:
    """一次性拉取实时数据：指数行情 → 市场情绪 → 成分股资金流 → 内存聚合 → 昨日对比。

    返回 dict: {ts, quotes, sentiment, flow, agg, yday, hist, error}
    """
    import time as _t
    try:
        index_map, all_codes = build_index_map_cached()
        proxies = get_proxies()
        quotes = fetch_index_quotes(FOCUS_INDICES, proxies=proxies)
        flow = fetch_stock_flow_realtime(all_codes, proxies=proxies)
        agg = aggregate_flow_realtime(flow, index_map)
        yday = _aggregate_yesterday(load_ff_latest(), index_map)
        # 放量基准：指数近 5 日成交额（当日 session 缓存一次，不重复拉）；含两市指数供成交额对比
        if "rt_hist" not in st.session_state:
            st.session_state["rt_hist"] = fetch_daily_amount_history(
                [index_secid(c) for c in FOCUS_INDICES] + ["1.000001", "0.399106"],
                proxies=proxies)
        return {
            "ts": _now_bj().strftime("%Y-%m-%d %H:%M:%S"),
            "quotes": quotes,
            "sentiment": fetch_market_sentiment(quotes, proxies=proxies),
            "flow": flow,
            "agg": agg,
            "yday": yday,
            "hist": st.session_state.get("rt_hist", {}),
            "error": None,
        }
    except Exception as e:
        return {"ts": _now_bj().strftime("%Y-%m-%d %H:%M:%S"),
                "quotes": pd.DataFrame(), "sentiment": {}, "flow": pd.DataFrame(),
                "agg": pd.DataFrame(), "yday": {}, "hist": {}, "error": str(e)}


def rt_load() -> dict | None:
    """限流拉取：距上次 >= REFRESH_MS 或不存在时重拉，否则复用 session_state（60s 内不重复请求）"""
    import time as _t
    now = _t.time()
    if "rt_data" in st.session_state and now - st.session_state.get("rt_last_ts", 0) < REFRESH_MS / 1000:
        return st.session_state["rt_data"]
    with st.spinner("拉取实时行情..."):
        data = _realtime_load_all()
    if data is not None:
        st.session_state["rt_data"] = data
        st.session_state["rt_last_ts"] = now
    return data


# ── 全局样式 ──
st.markdown("""
<style>
    .block-container { padding-top: 2rem; }
    section[data-testid="stSidebar"] { display: none; }
    .main h1 { font-size: 20px; font-weight: 600; letter-spacing: -0.3px; color: #333; }
    .main h3 { font-size: 16px; font-weight: 600; letter-spacing: -0.2px; color: #333; margin-bottom: 8px; }
    .section-gap { margin: 24px 0 0 0; }
    .rank-card {
        background: #fff; border-radius: 8px; padding: 12px 16px; margin: 5px 0;
        border: 1px solid #E8EAED; transition: all 0.2s ease;
    }
    .rank-card:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
    .rank-title { font-size: 13px; font-weight: 600; color: #333; }
    .rank-badge { float: right; font-size: 14px; font-weight: 700; }
    .rank-meta { font-size: 11px; color: #999; margin-top: 4px; }
    .metric-card {
        background: #fff; border-radius: 8px; padding: 14px 16px;
        border: 1px solid #E8EAED; text-align: center;
    }
    .metric-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; }
    .metric-value { font-size: 22px; font-weight: 700; margin: 4px 0; }
    .metric-sub { font-size: 11px; color: #AAA; }
    .period-btn { display: inline-block; padding: 4px 12px; border-radius: 4px; font-size: 12px;
                  font-weight: 500; cursor: pointer; margin-right: 4px; }
    .period-btn.active { background: #333; color: #fff; }
    .period-btn.inactive { background: #f0f0f0; color: #666; }
    .stat-col { text-align: center; padding: 8px 4px; }
    .stat-name { font-size: 11px; color: #999; margin-bottom: 2px; }
    .stat-val { font-size: 15px; font-weight: 600; }
</style>
""", unsafe_allow_html=True)


# ── 数据 ──
results = load_aggregated()
margin_all = load_margin_history()
etf_scale = load_etf_scale_data()
etf_nav = _load_etf_nav_cache()
# 金额 = 份额 × 单位净值（净值缺失的日期 amount 为 NaN，金额口径下自动跳过）
if not etf_scale.empty and not etf_nav.empty:
    etf_scale = compute_etf_amount(etf_scale, etf_nav)

st.markdown("""<h1>A股资金监控看板</h1>""", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["A股指数融资额", "ETF份额监控", "资金流向", "实时行情"])

# ============================================================
#  Tab 1: A股指数融资额（原有全部内容）
# ============================================================
with tab1:
    # ── 全市场融资概览 ──
    overview = compute_market_overview()
    if overview is not None:
        st.markdown("### 全市场融资概览")
        daily = overview["daily"]
        bal_txt = fmt_amount(overview["balance"])
        buy_txt = fmt_amount(overview["buy"])
        latest_str = overview["latest_date"].strftime("%Y-%m-%d")

        m1, m2, m3, m4, m5 = st.columns(5, gap="medium")
        with m1:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">全市场融资余额</div>
            <div class="metric-value" style="font-size:19px">{bal_txt}</div>
            <div class="metric-sub">{latest_str}</div></div>""", unsafe_allow_html=True)
        for col, label, v in [(m2, "较5日前", overview["chg5"]),
                              (m3, "较15日前", overview["chg15"]),
                              (m4, "较30日前", overview["chg30"])]:
            with col:
                if v is None:
                    txt, color = "N/A", "#AAA"
                else:
                    txt, color = f"{v:+.2f}%", pct_color(v)
                st.markdown(f"""<div class="metric-card"><div class="metric-label">{label}</div>
                <div class="metric-value" style="font-size:19px;color:{color}">{txt}</div>
                <div class="metric-sub">融资余额变化</div></div>""", unsafe_allow_html=True)
        with m5:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">最新融资买入额</div>
            <div class="metric-value" style="font-size:19px">{buy_txt}</div>
            <div class="metric-sub">{latest_str}</div></div>""", unsafe_allow_html=True)

        # 全市场融资余额历史曲线 + 每日买入额（副轴）+ 上证指数（第三轴）
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(
            x=daily.index, y=daily["total_balance"] / 1e8,
            name="全市场融资余额", line=dict(width=2.5, color="#185FA5"),
            hovertemplate="%{x|%Y-%m-%d}<br>余额 %{y:,.0f}亿<extra></extra>",
        ), secondary_y=False)
        fig.add_trace(go.Bar(
            x=daily.index, y=daily["total_buy"] / 1e8,
            name="每日融资买入额", marker_color="rgba(226,74,74,0.30)",
            hovertemplate="%{x|%Y-%m-%d}<br>买入 %{y:,.0f}亿<extra></extra>",
        ), secondary_y=True)
        # ── 上证指数收盘叠加（第三轴 y3，与 y2 右轴错开不重叠）──
        ss_series = None
        ss_close = load_ssindex_close()
        if ss_close is not None and not ss_close.empty:
            ss_series = ss_close.set_index("date")["close"].reindex(daily.index)
        if ss_series is not None and ss_series.notna().any():
            fig.add_trace(go.Scatter(
                x=ss_series.index, y=ss_series.values,
                name="上证指数", line=dict(width=1.3, color="#FFA940"),
                yaxis="y3",
                hovertemplate="%{x|%Y-%m-%d}<br>上证 %{y:,.1f}<extra></extra>",
            ))
        fig.update_layout(
            height=360, margin=dict(l=0, r=0, t=0, b=0), bargap=0.4,
            legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                        font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
            hovermode="x unified",
            xaxis=dict(type="date", tickformat="%m-%d", showgrid=True, gridcolor="#f0f0f0",
                       showline=False, tickfont=dict(size=11, color="#888")),
            yaxis=dict(title="融资余额(亿元)", showgrid=True, gridcolor="#f0f0f0",
                       showline=False, tickfont=dict(size=11, color="#888")),
            yaxis2=dict(title="买入额(亿元)", showgrid=False, showline=False,
                        tickfont=dict(size=11, color="#AAA"),
                        overlaying="y", side="right", position=1.0, anchor="free"),
            yaxis3=dict(title="上证指数", showgrid=False, showline=False,
                        tickfont=dict(size=11, color="#AAA"),
                        overlaying="y", side="right", position=0.86, anchor="free"),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Microsoft YaHei, PingFang SC, sans-serif"),
        )
        st.plotly_chart(fig, use_container_width=True)

    if not results:
        st.warning("数据生成中，请稍候几分钟后再刷新。首次部署需在 GitHub Actions 手动触发一次「每日数据更新」。")
    else:
        # ── 周期 ──
        period = st.radio("周期", ["5日", "15日", "30日"], horizontal=True, index=0, label_visibility="collapsed", key="tab1_period")
        lookback = {"5日": 5, "15日": 15, "30日": 30}[period]
        periods = {"5日": 5, "15日": 15, "30日": 30}

        # ── 排名 ──
        st.markdown(f"### 融资余额变化排行（近{period}）")
        changes = compute_changes(results, lookback)
        top_up, top_down = changes.head(5), changes.tail(5)

        c1, c2 = st.columns(2, gap="medium")
        for col, data, label, icon in [(c1, top_up, "增幅最大", "📈"), (c2, top_down, "降幅最大", "📉")]:
            with col:
                st.caption(f"{icon} {label}")
                for _, r in data.iterrows():
                    v = r["融资变化%"]
                    buy = r["融资买入(亿)"]
                    bal = r["融资余额(亿)"]
                    st.markdown(f"""
                    <div class="rank-card">
                        <div class="rank-title">{r['指数']}
                            <span class="rank-badge" style="color:{pct_color(v)}">{fmt_pct(v)}</span>
                        </div>
                        <div class="rank-meta">余额 {bal:,.0f}亿 &nbsp;·&nbsp; 买入 {buy:,.0f}亿</div>
                    </div>""", unsafe_allow_html=True)

        with st.expander(f"查看全部 {len(changes)} 个指数排名"):
            st.dataframe(changes, use_container_width=True, hide_index=True,
                         column_config={"融资变化%": st.column_config.NumberColumn(format="%+.2f%%")})

        # ── 历史趋势 ──
        st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
        st.markdown("### 历史趋势")

        all_names = sorted([(c, df["index_name"].iloc[0]) for c, df in results.items()], key=lambda x: x[1])
        sel_names = st.multiselect("选择指数", [n for _, n in all_names],
                                   default=["沪深300", "中证500", "创业板指", "科创50"],
                                   key="tab1_hist_idx")

        if sel_names:
            sel_codes = [c for c, n in all_names if n in sel_names]

            # 多周期统计
            crows = []
            for code in sel_codes:
                df = results[code].sort_values("trade_date")
                bal = df["total_rz_balance"].dropna()
                if bal.empty:
                    continue
                cv = float(bal.iloc[-1])
                row = {"指数": df["index_name"].iloc[0]}
                for lb, d in periods.items():
                    if len(bal) > d:
                        pv = float(bal.iloc[-(d + 1)])
                        row[lb] = round((cv - pv) / pv * 100, 2) if pv else None
                    else:
                        row[lb] = None
                crows.append(row)

            # 统计卡片
            st.caption("多周期变化")
            mcols = st.columns(len(sel_names))
            for i, row in enumerate(crows):
                with mcols[i]:
                    st.markdown(f"**{row['指数']}**")
                    for lb in ["5日", "15日", "30日"]:
                        v = row.get(lb)
                        if v is None:
                            st.markdown(f"<span style='font-size:12px;color:#AAA'>{lb}: N/A</span>",
                                        unsafe_allow_html=True)
                        else:
                            st.markdown(
                                f"<span style='font-size:12px'>{lb}: "
                                f"<span style='color:{pct_color(v)};font-weight:600'>{v:+.2f}%</span></span>",
                                unsafe_allow_html=True)

            # 图表控制
            cl, cr = st.columns([3, 1])
            with cl:
                chart = st.radio("指标", ["余额", "余额+买入(双轴)"], horizontal=True, index=0,
                                 label_visibility="collapsed", key="tab1_chart_metric")
            with cr:
                normalize = st.checkbox("归一化(起点=100)", value=True, key="tab1_normalize")

            if chart == "余额+买入(双轴)":
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                for code in sel_codes:
                    df = results[code].sort_values("trade_date")
                    n = df["index_name"].iloc[0]
                    y_bal = df["total_rz_balance"] / 1e8
                    if normalize:
                        y_bal = y_bal / y_bal.iloc[0] * 100
                    fig.add_trace(go.Scatter(x=df["trade_date"], y=y_bal, mode="lines",
                                             name=f"{n}-余额", line=dict(width=2)), secondary_y=False)
                    if "total_rz_buy" in df.columns:
                        y_buy = df["total_rz_buy"] / 1e8
                        if normalize:
                            y_buy = y_buy / y_buy.iloc[0] * 100
                        fig.add_trace(go.Scatter(x=df["trade_date"], y=y_buy, mode="lines",
                                                 name=f"{n}-买入", line=dict(width=1, dash="dot")),
                                      secondary_y=True)
                fig.update_yaxes(title_text="余额(起点=100)" if normalize else "余额(亿元)", secondary_y=False)
                fig.update_yaxes(title_text="买入(起点=100)" if normalize else "买入(亿元)", secondary_y=True)
            else:
                fig = go.Figure()
                for code in sel_codes:
                    df = results[code].sort_values("trade_date")
                    n = df["index_name"].iloc[0]
                    y = df["total_rz_balance"] / 1e8
                    if normalize:
                        y = y / y.iloc[0] * 100
                    fig.add_trace(go.Scatter(x=df["trade_date"], y=y, mode="lines", name=n,
                                             line=dict(width=2), hovertemplate="%{y:.1f}<extra>" + n + "</extra>"))
                fig.update_yaxes(title_text="融资余额(起点=100)" if normalize else "融资余额(亿元)")

            fig.update_layout(
                height=480, margin=dict(l=0, r=0, t=0, b=0),
                legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                            font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
                hovermode="x unified",
                xaxis=dict(type="date", tickformat="%m-%d", showgrid=True, gridcolor="#f0f0f0",
                           showline=False, tickfont=dict(size=11, color="#888")),
                yaxis=dict(showgrid=True, gridcolor="#f0f0f0", showline=False,
                           tickfont=dict(size=11, color="#888")),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Microsoft YaHei, PingFang SC, sans-serif"),
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── 下钻 ──
        st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
        st.markdown("### 成分股下钻")

        drill_name = st.selectbox("选择指数查看成分股融资变化", [n for _, n in all_names], key="tab1_drill_idx")
        drill_code = next((c for c, n in all_names if n == drill_name), None)

        if drill_code and not margin_all.empty:
            try:
                weights = load_index_weights(drill_code)
                if not weights.empty:
                    weights["code_num"] = weights["stock_code"].apply(lambda x: x.split(".")[0])
                    stocks = set(weights["code_num"])
                    names = dict(zip(weights["code_num"], weights["stock_name"]))
                    detail = margin_all[margin_all["stock_code"].isin(stocks)].copy()

                    detail_sorted = detail.sort_values(["stock_code", "trade_date"])
                    latest_mask = detail_sorted.groupby("stock_code")["trade_date"].transform("max")
                    to_latest = detail_sorted[detail_sorted["trade_date"] == latest_mask].copy()
                    to_latest["名称"] = to_latest["stock_code"].map(names)
                    to_latest["融资余额(亿)"] = to_latest["rzye"] / 1e8
                    to_latest["当日买入(亿)"] = to_latest["rzmre"].fillna(0) / 1e8 if "rzmre" in to_latest.columns else 0

                    stock_stats = {}
                    for sc, gp in detail_sorted.groupby("stock_code"):
                        gp = gp.sort_values("trade_date")
                        if len(gp) < 2:
                            continue
                        cv = gp["rzye"].iloc[-1]
                        entry = {}
                        for lb, d in periods.items():
                            if len(gp) > d:
                                window = gp.iloc[-(d + 1):]
                                pv = gp["rzye"].iloc[-(d + 1)]
                                if pd.notna(pv) and pv != 0:
                                    entry[f"{lb}变化%"] = round((cv - pv) / pv * 100, 2)
                                buy_sum = window["rzmre"].iloc[1:].fillna(0).sum() if "rzmre" in gp.columns else 0
                                entry[f"{lb}买入(亿)"] = round(buy_sum / 1e8, 1)
                        if entry:
                            stock_stats[sc] = entry

                    stats_df = pd.DataFrame.from_dict(stock_stats, orient="index")
                    to_latest = to_latest.set_index("stock_code").join(stats_df).reset_index()
                    to_latest.sort_values("rzye", ascending=False, inplace=True)

                    cols = ["名称", "融资余额(亿)", "当日买入(亿)"]
                    for lb in ["5日", "15日", "30日"]:
                        cols.append(f"{lb}变化%")
                        cols.append(f"{lb}买入(亿)")
                    cols = [c for c in cols if c in to_latest.columns]

                    st.caption(f"共 {len(to_latest)} 只成分股")
                    st.dataframe(to_latest[cols].rename(columns={"stock_code": "代码"}),
                                 use_container_width=True, hide_index=True,
                                 column_config={
                                     "融资余额(亿)": st.column_config.NumberColumn(format="%.1f"),
                                     "当日买入(亿)": st.column_config.NumberColumn(format="%.1f"),
                                     "5日变化%": st.column_config.NumberColumn(format="%+.2f%%"),
                                     "15日变化%": st.column_config.NumberColumn(format="%+.2f%%"),
                                     "30日变化%": st.column_config.NumberColumn(format="%+.2f%%"),
                                     "5日买入(亿)": st.column_config.NumberColumn(format="%.1f"),
                                     "15日买入(亿)": st.column_config.NumberColumn(format="%.1f"),
                                     "30日买入(亿)": st.column_config.NumberColumn(format="%.1f"),
                                 })
                else:
                    st.caption("未找到该指数成分股数据")
            except Exception as e:
                st.caption(f"加载失败: {e}")

# ============================================================
#  Tab 2: ETF 份额变化
# ============================================================
with tab2:
    if etf_scale is None or etf_scale.empty:
        st.info("暂无 ETF 数据，请先运行 python run.py（会自动拉取沪深两市 ETF 份额/净值）")
    else:
        # 口径切换：份额（默认）/ 金额（= 份额 × 单位净值）
        koujing = st.radio("口径", ["份额", "金额"], horizontal=True, index=0, label_visibility="collapsed", key="tab2_koujing")
        if koujing == "金额" and "amount" not in etf_scale.columns:
            st.warning("暂无净值数据（金额 = 份额 × 净值），请先运行 python run.py 拉取净值")
            koujing = "份额"
        value_col = "amount" if koujing == "金额" else "total_share"
        unit = "亿元" if koujing == "金额" else "亿份"
        qty_name = "规模" if koujing == "金额" else "份额"
        metric_name = f"国家队ETF总{qty_name}"

        st.markdown("### 国家队 ETF 监控")
        st.caption("池内为中央汇金系（汇金投资/汇金资管）2025 年报披露重仓的宽基 ETF；份额为沪深交易所每日公布，金额 = 份额 × 单位净值")

        nt_codes = [c for c in NATIONAL_TEAM_ETF if c in set(etf_scale["fund_code"])]
        if not nt_codes:
            st.warning("池内 ETF 暂无数据，请先运行 python run.py 拉取份额数据")
        else:
            stats = compute_nt_pool_stats(etf_scale, nt_codes, value_col)
            if stats is None:
                st.warning("暂无数据")
            else:
                daily = stats["daily"]
                latest_str = daily.index[-1].strftime("%Y-%m-%d")

                # ── 总量指标卡 ──
                n1, n2, n3, n4, n5 = st.columns(5, gap="medium")
                with n1:
                    st.markdown(f"""<div class="metric-card"><div class="metric-label">{metric_name}</div>
                    <div class="metric-value" style="font-size:19px">{stats['latest']/1e8:,.0f}{unit}</div>
                    <div class="metric-sub">{latest_str}</div></div>""", unsafe_allow_html=True)
                d1 = stats["d1"]
                d1_txt = "N/A" if d1 is None else f"{d1/1e8:+,.2f}{unit}"
                d1_col = "#AAA" if d1 is None else pct_color(d1)
                with n2:
                    st.markdown(f"""<div class="metric-card"><div class="metric-label">当日净变化</div>
                    <div class="metric-value" style="font-size:19px;color:{d1_col}">{d1_txt}</div>
                    <div class="metric-sub">{qty_name}增减({unit})</div></div>""", unsafe_allow_html=True)
                for col, label, v in [(n3, "较5日前", stats["chg5"]),
                                      (n4, "较20日前", stats["chg20"]),
                                      (n5, "较60日前", stats["chg60"])]:
                    with col:
                        if v is None:
                            txt, color = "N/A", "#AAA"
                        else:
                            txt, color = f"{v:+.2f}%", pct_color(v)
                        st.markdown(f"""<div class="metric-card"><div class="metric-label">{label}</div>
                        <div class="metric-value" style="font-size:19px;color:{color}">{txt}</div>
                        <div class="metric-sub">总{qty_name}变化</div></div>""", unsafe_allow_html=True)

                # ── 总量历史 + 每日净变化 ──
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Scatter(
                    x=daily.index, y=daily / 1e8, name=metric_name,
                    line=dict(width=2.5, color="#185FA5"),
                    hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}" + unit + "<extra></extra>",
                ), secondary_y=False)
                net = daily.diff() / 1e8
                bar_colors = [RED if (v is not None and v > 0) else GREEN for v in net]
                fig.add_trace(go.Bar(
                    x=daily.index, y=net, name=f"每日净变化({unit})",
                    marker_color=bar_colors,
                    hovertemplate="%{x|%Y-%m-%d}<br>净变化 %{y:+.2f}" + unit + "<extra></extra>",
                ), secondary_y=True)
                fig.update_layout(
                    height=360, margin=dict(l=0, r=0, t=0, b=0), bargap=0.4,
                    legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                                font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
                    hovermode="x unified",
                    xaxis=dict(type="date", tickformat="%m-%d", showgrid=True, gridcolor="#f0f0f0",
                               showline=False, tickfont=dict(size=11, color="#888")),
                    yaxis=dict(title=f"{qty_name}({unit})", showgrid=True, gridcolor="#f0f0f0",
                               showline=False, tickfont=dict(size=11, color="#888"),
                               tickformat=",.1f"),
                    yaxis2=dict(title=f"净变化({unit})", showgrid=False, showline=False,
                                tickfont=dict(size=11, color="#AAA"), tickformat="+,.2f"),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Microsoft YaHei, PingFang SC, sans-serif"),
                )
                st.plotly_chart(fig, use_container_width=True)

                # ── 池内明细 ──
                st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
                st.markdown("### 池内明细")
                detail = compute_nt_detail(etf_scale, nt_codes, value_col, unit)
                if not detail.empty:
                    latest_lbl = f"最新{unit}"
                    sort_opts = [c for c in [latest_lbl, f"20日变化({unit})", f"5日变化({unit})", f"60日变化({unit})", f"1日变化({unit})"]
                                 if c in detail.columns]
                    if sort_opts:
                        sort_by = st.selectbox("排序方式", sort_opts, index=0, key="tab2_detail_sort")
                        # 全部字段统一降序：份额/规模从大到小、变化从多到少
                        detail_sorted = detail.sort_values(sort_by, ascending=False, na_position="last")
                        chg_cols = [c for c in [f"1日变化({unit})", f"5日变化({unit})", f"20日变化({unit})", f"60日变化({unit})"]
                                    if c in detail_sorted.columns]
                        styled = style_color(detail_sorted, chg_cols)
                        col_cfg = {c: st.column_config.NumberColumn(format="%+.2f") for c in chg_cols}
                        col_cfg[latest_lbl] = st.column_config.NumberColumn(format="%.2f")
                        st.dataframe(styled, use_container_width=True, hide_index=True, column_config=col_cfg)

                # ── 变化曲线 ──
                st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
                st.markdown(f"### {qty_name}变化曲线")
                latest_row = etf_scale[etf_scale["trade_date"] == etf_scale["trade_date"].max()]
                top10_codes = (latest_row[latest_row["fund_code"].isin(nt_codes)]
                               .sort_values(value_col, ascending=False).head(10)["fund_code"].tolist())
                nt_labels = [f"{c} {NATIONAL_TEAM_ETF[c]}" for c in nt_codes]
                default_labels = [f"{c} {NATIONAL_TEAM_ETF[c]}" for c in top10_codes if c in nt_codes]
                sel_labels = st.multiselect(f"选择 ETF（默认{qty_name}前10大）", nt_labels, default=default_labels, key="tab2_etf_sel")
                sel_codes = [lbl.split(" ")[0] for lbl in sel_labels]
                cl2, cr2 = st.columns([3, 1])
                with cl2:
                    enorm = st.checkbox("归一化(起点=100)", value=True,
                                        help="绝对值差异大，归一化更易比较变化幅度", key="tab2_normalize")
                with cr2:
                    st.caption(f"数据截至 {latest_str}")
                if sel_codes:
                    fig = go.Figure()
                    for code in sel_codes:
                        sub = etf_scale[etf_scale["fund_code"] == code].sort_values("trade_date")
                        if sub.empty:
                            continue
                        name = NATIONAL_TEAM_ETF.get(code, sub["fund_name"].iloc[-1])
                        y = sub[value_col] / 1e8
                        if enorm:
                            y = y / y.iloc[0] * 100
                        fig.add_trace(go.Scatter(x=sub["trade_date"], y=y, mode="lines", name=name,
                                                 line=dict(width=2),
                                                 hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra>" + name + "</extra>"))
                    fig.update_layout(
                        height=440, margin=dict(l=0, r=0, t=0, b=0),
                        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                                    font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
                        hovermode="x unified",
                        xaxis=dict(type="date", tickformat="%m-%d", showgrid=True, gridcolor="#f0f0f0",
                                   showline=False, tickfont=dict(size=11, color="#888")),
                        yaxis=dict(title=(f"{qty_name}(起点=100)" if enorm else f"{qty_name}({unit})"),
                                   showgrid=True, gridcolor="#f0f0f0", showline=False,
                                   tickfont=dict(size=11, color="#888")),
                        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(family="Microsoft YaHei, PingFang SC, sans-serif"),
                    )
                    st.plotly_chart(fig, use_container_width=True)

        # ── 全市场排行（发现工具）──
        with st.expander("全市场 ETF 份额变化排行（供发现参考）"):
            etf_changes = compute_etf_changes_multi(etf_scale)
            if etf_changes.empty:
                st.caption("数据不足")
            else:
                sort_opts_all = [c for c in ["最新份额(亿份)", "20日变化%", "5日变化%", "1日变化%"]
                                 if c in etf_changes.columns]
                if sort_opts_all:
                    sort_all = st.selectbox("排序方式", sort_opts_all, index=1, key="tab2_all_sort")
                    asc_all = sort_all == "最新份额(亿份)"
                    etf_changes = etf_changes.sort_values(sort_all, ascending=asc_all, na_position="last")
                pct_cols_all = [c for c in ["1日变化%", "5日变化%", "20日变化%"] if c in etf_changes.columns]
                styled_all = style_color(etf_changes, pct_cols_all)
                cfg_all = {c: st.column_config.NumberColumn(format="%+.2f%%") for c in pct_cols_all}
                if "最新份额(亿份)" in etf_changes.columns:
                    cfg_all["最新份额(亿份)"] = st.column_config.NumberColumn(format="%.2f")
                st.dataframe(styled_all, use_container_width=True, hide_index=True, column_config=cfg_all)

# ============================================================
#  Tab 3: 资金流向（指数成分股主力净流入）
# ============================================================
with tab3:
    if not results:
        st.info("暂无聚合数据，请先运行 python run.py 生成数据")
    else:
        # 检查聚合结果是否含资金流向字段
        has_flow = any("total_main_net" in df.columns for df in results.values())
        if not has_flow:
            st.warning("聚合数据暂不含资金流向字段，请运行 python run.py 重新聚合（含资金流向）")
        else:
            st.markdown("### 指数成分股资金流向")
            st.caption("主力净流入 = 超大单 + 大单净流入（东方财富口径）；数据为交易日收盘后快照，负值表示净流出")
            with st.expander("关于资金流向数据口径"):
                st.markdown("""
- **主力净流入** = 超大单净流入 + 大单净流入，负值表示净流出
- **超大单**：单笔成交金额 ≥ 100 万元（或 ≥ 50 万股）
- **大单**：单笔 20~100 万元
- **中单**：单笔 4~20 万元
""")

            fperiod = st.radio("周期", ["当日", "5日", "20日"], horizontal=True, index=0, label_visibility="collapsed", key="tab3_period")
            flb = {"当日": 1, "5日": 5, "20日": 20}[fperiod]
            flow = compute_flow_changes(results, flb)

            if flow.empty:
                st.warning("暂无资金流向数据")
            else:
                # ── 指标卡 ──
                latest_date = None
                for df in results.values():
                    if "total_main_net" in df.columns and not df.empty:
                        latest_date = df.sort_values("trade_date")["trade_date"].iloc[-1]
                        break
                latest_str = pd.to_datetime(latest_date).strftime("%Y-%m-%d") if latest_date is not None else "-"
                top_in = flow.head(3)
                top_out = flow.tail(3).iloc[::-1]
                f1, f2, f3, f4 = st.columns(4, gap="medium")
                with f1:
                    st.markdown(f"""<div class="metric-card"><div class="metric-label">净流入第一</div>
                    <div class="metric-value" style="font-size:16px">{top_in.iloc[0]['指数']}</div>
                    <div class="metric-sub" style="color:{RED}">+{top_in.iloc[0][f'{flb}日主力净流入(亿)']:.1f}亿</div></div>""", unsafe_allow_html=True)
                with f2:
                    st.markdown(f"""<div class="metric-card"><div class="metric-label">净流入合计</div>
                    <div class="metric-value" style="font-size:19px">{flow[flow[f'{flb}日主力净流入(亿)'] > 0][f'{flb}日主力净流入(亿)'].sum():,.0f}亿</div>
                    <div class="metric-sub">{len(flow)} 个指数</div></div>""", unsafe_allow_html=True)
                with f3:
                    st.markdown(f"""<div class="metric-card"><div class="metric-label">净流出第一</div>
                    <div class="metric-value" style="font-size:16px">{top_out.iloc[0]['指数']}</div>
                    <div class="metric-sub" style="color:{GREEN}">{top_out.iloc[0][f'{flb}日主力净流入(亿)']:.1f}亿</div></div>""", unsafe_allow_html=True)
                with f4:
                    st.markdown(f"""<div class="metric-card"><div class="metric-label">数据日期</div>
                    <div class="metric-value" style="font-size:19px">{latest_str}</div>
                    <div class="metric-sub">收盘后快照</div></div>""", unsafe_allow_html=True)

                # ── 排行 ──
                st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
                st.markdown(f"### 主力净流入排行（{fperiod}）")
                fc1, fc2 = st.columns(2, gap="medium")
                for col, data, label, icon in [(fc1, flow.head(8), "净流入 TOP8", "📈"),
                                               (fc2, flow.tail(8).iloc[::-1], "净流出 TOP8", "📉")]:
                    with col:
                        st.caption(f"{icon} {label}")
                        for _, r in data.iterrows():
                            v = r[f"{flb}日主力净流入(亿)"]
                            st.markdown(f"""
                            <div class="rank-card">
                                <div class="rank-title">{r['指数']}
                                    <span class="rank-badge" style="color:{pct_color(v)}">{v:+.2f}亿</span>
                                </div>
                                <div class="rank-meta">代码 {r['代码']}</div>
                            </div>""", unsafe_allow_html=True)

                with st.expander(f"查看全部 {len(flow)} 个指数的资金流向（{fperiod}）"):
                    flow_mx = compute_flow_multi(results, flb)
                    mx_label = "当日" if flb == 1 else f"{flb}日"
                    mx_cols = [c for c in [f"{mx_label}主力(亿)", f"{mx_label}中单(亿)", f"{mx_label}小单(亿)"]
                               if c in flow_mx.columns]
                    styled_mx = style_color(flow_mx, mx_cols)
                    st.dataframe(styled_mx, use_container_width=True, hide_index=True,
                                 column_config={c: st.column_config.NumberColumn(format="%+.2f") for c in mx_cols})

                # ── 历史趋势 ──
                st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
                st.markdown("### 主力净流入历史趋势")
                # 资金流快照积累天数提示
                flow_days = 0
                for df in results.values():
                    if "total_main_net" in df.columns:
                        flow_days = int(df["total_main_net"].notna().sum())
                        break
                if flow_days < 5:
                    st.caption(f"资金流向数据自启用日起开始积累（当前 {flow_days} 个交易日），历史趋势将随每日更新逐步完整")
                flow_names = sorted([(c, df["index_name"].iloc[0]) for c, df in results.items()
                                     if "total_main_net" in df.columns], key=lambda x: x[1])
                sel_fnames = st.multiselect("选择指数", [n for _, n in flow_names],
                                            default=["沪深300", "中证500", "创业板指", "科创50"],
                                            key="tab3_flow_idx")
                if sel_fnames:
                    sel_fcodes = [c for c, n in flow_names if n in sel_fnames]
                    fig = go.Figure()
                    for code in sel_fcodes:
                        df = results[code].sort_values("trade_date")
                        n = df["index_name"].iloc[0]
                        y = df["total_main_net"] / 1e8
                        fig.add_trace(go.Scatter(x=df["trade_date"], y=y, mode="lines", name=n,
                                                 line=dict(width=2),
                                                 hovertemplate="%{x|%Y-%m-%d}<br>%{y:+.1f}亿<extra>" + n + "</extra>"))
                    fig.add_hline(y=0, line_dash="dot", line_color="#BBB")
                    fig.update_layout(
                        height=440, margin=dict(l=0, r=0, t=0, b=0),
                        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                                    font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
                        hovermode="x unified",
                        xaxis=dict(type="date", tickformat="%m-%d", showgrid=True, gridcolor="#f0f0f0",
                                   showline=False, tickfont=dict(size=11, color="#888")),
                        yaxis=dict(title="主力净流入(亿元)", showgrid=True, gridcolor="#f0f0f0",
                                   showline=False, tickfont=dict(size=11, color="#888")),
                        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(family="Microsoft YaHei, PingFang SC, sans-serif"),
                    )
                    st.plotly_chart(fig, use_container_width=True)

# ============================================================
#  Tab 4: 实时行情
# ============================================================
with tab4:
    st.markdown("### 实时行情")
    st.caption("数据源：东方财富实时接口（延迟数秒）；盘中自动刷新，非交易时段展示最近收盘数据")
    auto = st.toggle("实时自动刷新（60 秒）", value=is_market_open(), key="rt_auto")
    if auto:
        st_autorefresh(interval=REFRESH_MS, key="rt_auto_refresh")
    if not is_market_open():
        st.caption("当前为非交易时段，自动刷新已暂停，展示最近数据")

    rt = rt_load()
    if rt is None or rt.get("error"):
        st.warning(f"实时数据获取失败，请稍后刷新：{rt.get('error') if rt else '未知错误'}")
    else:
        ts, sent = rt["ts"], rt["sentiment"] or {}
        quotes, agg, yday, hist = rt["quotes"], rt["agg"], rt["yday"], rt["hist"]

        # ── 市场情绪指标卡 ──
        c1, c2, c3, c4 = st.columns(4, gap="medium")
        with c1:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">上涨家数</div>
            <div class="metric-value" style="font-size:19px;color:{RED}">{sent.get('up', 0):,}</div>
            <div class="metric-sub">全市场</div></div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">下跌家数</div>
            <div class="metric-value" style="font-size:19px;color:{GREEN}">{sent.get('down', 0):,}</div>
            <div class="metric-sub">全市场</div></div>""", unsafe_allow_html=True)
        with c3:
            dr = sent.get('up', 0) / max(sent.get('down', 0), 1)
            st.markdown(f"""<div class="metric-card"><div class="metric-label">涨跌比</div>
            <div class="metric-value" style="font-size:19px">{dr:.2f}</div>
            <div class="metric-sub">上涨/下跌</div></div>""", unsafe_allow_html=True)
        with c4:
            cur_amt = float(sent.get("amount", 0) or 0)
            h1, h2 = hist.get("1.000001", []) or [], hist.get("0.399106", []) or []
            if cur_amt > 0 and len(h1) >= 2 and len(h2) >= 2:
                y_same = (h1[-2] + h2[-2]) * _time_progress()   # 昨日两市成交额 × 当日时间进度
                diff = (cur_amt - y_same) / 1e8
                cmp_txt = f"较昨日同时段 {diff:+,.0f}亿"
                cmp_color = pct_color(diff)      # 放量红 / 缩量绿
            else:
                cmp_txt, cmp_color = "较昨日同时段 -", "#AAA"
            st.markdown(f"""<div class="metric-card"><div class="metric-label">两市成交额</div>
            <div class="metric-value" style="font-size:19px">{cur_amt/1e8:,.0f}亿</div>
            <div class="metric-sub" style="line-height:1.7">{ts}
            <div style="color:{cmp_color}">{cmp_txt}</div></div></div>""", unsafe_allow_html=True)

        # ── 指数实时行情 + 放量提示 ──
        st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
        st.markdown("### 指数实时行情")
        if quotes.empty:
            st.caption("暂无指数行情数据")
        else:
            q = quotes[quotes["code"].isin([c.split(".")[0] for c in FOCUS_INDICES])].copy()
            if not q.empty:
                q["指数"] = q["code"].map({c.split(".")[0]: FOCUS_NAMES.get(c, c) for c in FOCUS_INDICES})
                q["点位"] = q["price"]
                q["涨跌幅%"] = q["pct_chg"]
                q["成交额(亿)"] = (q["amount"] / 1e8).round(0)
                # 放量：实时成交额 vs 近5日同时段均值
                prog = max(_time_progress(), 0.05)
                q["量比"] = q.apply(lambda r: round(
                    r["amount"] / (sum(hist.get(index_secid(r["code"]), [0])) / max(len(hist.get(index_secid(r["code"]), [0])), 1) * prog), 2)
                    if hist.get(index_secid(r["code"])) and r["amount"] else None, axis=1)
                q["放量"] = q["量比"].apply(lambda x: "放量" if x is not None and x >= VOLUME_RATIO else "")
                tbl = q[["指数", "点位", "涨跌幅%", "成交额(亿)", "量比", "放量"]]
                st.dataframe(style_color(tbl, ["涨跌幅%"]), use_container_width=True, hide_index=True,
                             column_config={
                                 "点位": st.column_config.NumberColumn(format="%.2f"),
                                 "涨跌幅%": st.column_config.NumberColumn(format="%+.2f%%"),
                                 "成交额(亿)": st.column_config.NumberColumn(format="%.0f"),
                                 "量比": st.column_config.NumberColumn(format="%.2f"),
                             })

        # ── 指数成分股实时资金流排行（全部指数，主力/中单/小单） + vs 昨日 ──
        st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
        st.markdown("### 指数成分股资金流向（实时）")
        kw = st.text_input("搜索指数", key="rt_search",
                           placeholder="输入指数名称或代码，如 沪深300 / 000300").strip()
        if agg.empty:
            st.caption("暂无实时资金流数据（非交易时段接口可能为空）")
        else:
            name_map = dict(discover_all_indices())   # 全部指数名称（内部已含宽基名）
            a = agg.copy()
            a["指数"] = a["index_code"].map(lambda c: name_map.get(c, c))
            a["主力(亿)"] = (a["main_net"] / 1e8).round(2)
            a["中单(亿)"] = (a["mid_net"] / 1e8).round(2)
            a["小单(亿)"] = (a["small_net"] / 1e8).round(2)
            a["昨日主力(亿)"] = a["index_code"].map(lambda c: round(yday.get(c, 0) / 1e8, 2))
            a["边际(亿)"] = (a["主力(亿)"] - a["昨日主力(亿)"]).round(2)
            a = a.sort_values("主力(亿)", ascending=False)
            # ── 搜索过滤：仅作用于展开表格；TOP 卡片保持全量 a ──
            if kw:
                a_search = a[a["指数"].str.contains(kw, case=False, na=False, regex=False)
                            | a["index_code"].str.contains(kw, case=False, na=False, regex=False)]
            else:
                a_search = a

            fc1, fc2 = st.columns(2, gap="medium")
            for col, data, label, icon in [(fc1, a.head(5), "净流入 TOP5", "📈"),
                                           (fc2, a.tail(5).iloc[::-1], "净流出 TOP5", "📉")]:
                with col:
                    st.caption(f"{icon} {label}")
                    for _, r in data.iterrows():
                        v, mid, sm = r["主力(亿)"], r["中单(亿)"], r["小单(亿)"]
                        y, delta = r["昨日主力(亿)"], r["边际(亿)"]
                        st.markdown(f"""
                        <div class="rank-card">
                            <div class="rank-title">{r['指数']}
                                <span class="rank-badge" style="color:{pct_color(v)}">{v:+.2f}亿</span>
                            </div>
                            <div class="rank-meta">中单 {mid:+.2f}亿 · 小单 {sm:+.2f}亿</div>
                            <div class="rank-meta">昨日 {y:+.2f}亿 · 边际 <span style="color:{pct_color(delta)}">{delta:+.2f}亿</span></div>
                        </div>""", unsafe_allow_html=True)

            if a_search.empty:
                st.caption("未找到匹配指数")
            else:
                with st.expander(f"查看全部 {len(a)} 个指数实时资金流（匹配 {len(a_search)} 个）"):
                    fc = ["指数", "主力(亿)", "中单(亿)", "小单(亿)", "昨日主力(亿)", "边际(亿)"]
                    st.dataframe(style_color(a_search[fc], fc[1:]), use_container_width=True, hide_index=True,
                                 column_config={c: st.column_config.NumberColumn(format="%+.2f") for c in fc[1:]})
            st.caption("注：实时为盘中估算；对比的\"昨日\"为最近已收盘交易日快照，权威记录以日更数据为准")

st.markdown("""
<div style='text-align:center;padding:32px 0 16px 0;color:#bbb;font-size:11px;border-top:1px solid #eee;margin-top:32px'>
    A股资金监控看板 · 指数成分股融资余额汇总 · ETF份额监控 · 资金流向 · 实时行情
</div>
""", unsafe_allow_html=True)
