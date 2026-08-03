"""A股资金变化看板 —— 融资融券指数汇总"""

import sys; sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

import streamlit as st
st.cache_data.clear()
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import discover_all_indices, AGGREGATED_DIR, MARGIN_DIR
from index_loader import load_index_weights

st.set_page_config(page_title="A股融资变化看板", page_icon="💰", layout="wide", initial_sidebar_state="collapsed")

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


def compute_changes(dfs, lb):
    rows = []
    for code, df in dfs.items():
        name = df["index_name"].iloc[0]
        df_s = df.sort_values("trade_date")
        if len(df_s) < lb + 1:
            continue
        cur, prev = df_s.iloc[-1], df_s.iloc[-(lb + 1)]
        cv = cur["total_rz_balance"]
        pv = prev["total_rz_balance"]
        if pd.isna(cv) or pd.isna(pv) or pv == 0:
            continue
        cb = cur.get("total_rz_buy", 0) or 0
        pb = prev.get("total_rz_buy", 0) or 0
        rows.append({
            "指数": name, "代码": code,
            "融资变化%": round((cv - pv) / pv * 100, 2),
            "融资余额(亿)": round(cv / 1e8, 1),
            "融资买入(亿)": round(cb / 1e8, 1) if pd.notna(cb) else 0,
        })
    return pd.DataFrame(rows).sort_values("融资变化%", ascending=False)


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


def fmt_amount(v: float) -> str:
    """金额格式化：>=1万亿显示万亿，否则显示亿"""
    if v is None or (isinstance(v, float) and (v != v)):
        return "N/A"
    if v >= 1e12:
        return f"{v / 1e12:.2f}万亿"
    return f"{v / 1e8:,.0f}亿"


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

st.markdown("""<h1>A股指数融资变化看板</h1>""", unsafe_allow_html=True)

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

    # 全市场融资余额历史曲线 + 每日买入额（副轴）
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
                    tickfont=dict(size=11, color="#AAA")),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Microsoft YaHei, PingFang SC, sans-serif"),
    )
    st.plotly_chart(fig, use_container_width=True)

if not results:
    st.warning("数据生成中，请稍候几分钟后再刷新。首次部署需在 GitHub Actions 手动触发一次「每日数据更新」。")
    st.stop()

# ── 周期 ──
period = st.radio("周期", ["5日", "15日", "30日"], horizontal=True, index=0, label_visibility="collapsed")
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
                           default=["沪深300", "中证500", "创业板指", "科创50"])

if sel_names:
    sel_codes = [c for c, n in all_names if n in sel_names]

    # 多周期统计
    crows = []
    for code in sel_codes:
        df = results[code].sort_values("trade_date")
        cv = df.iloc[-1]["total_rz_balance"]
        row = {"指数": df["index_name"].iloc[0]}
        for lb, d in periods.items():
            if len(df) > d:
                pv = df.iloc[-(d + 1)]["total_rz_balance"]
                row[lb] = round((cv - pv) / pv * 100, 2) if pd.notna(pv) and pv else None
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
                         label_visibility="collapsed")
    with cr:
        normalize = st.checkbox("归一化(起点=100)", value=True)

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

drill_name = st.selectbox("选择指数查看成分股融资变化", [n for _, n in all_names])
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

st.markdown("""
<div style='text-align:center;padding:32px 0 16px 0;color:#bbb;font-size:11px;border-top:1px solid #eee;margin-top:32px'>
    A股资金变化看板 · 指数成分股融资余额汇总
</div>
""", unsafe_allow_html=True)
