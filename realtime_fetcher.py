"""实时行情拉取模块 —— 东方财富实时接口（指数/个股/ETF/市场情绪）

数据源（免费，近零延迟）：
    push2delay.eastmoney.com/api/qt/ulist.np/get   本地/受限网络降级用
    push2.eastmoney.com/api/qt/ulist.np/get        云端公网直连（默认）
    push2his.eastmoney.com/api/qt/stock/kline/get  放量基准（近 N 日成交额）

代理约定：不硬编码；如需本地代理可 export RT_PROXY=<http代理地址>，
         云端不设置即直连（requests 默认走系统代理）。

所有实时数据仅用于页面展示，不落盘、不进 st.cache_data（由 app.py 的 session 限流控制）。
"""

import os
import time

import pandas as pd
import requests

URL_DELAY = "https://push2delay.eastmoney.com/api/qt/ulist.np/get"
URL_PUSH = "https://push2.eastmoney.com/api/qt/ulist.np/get"
URL_KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}
BATCH_SIZE = 300
FIELDS_QUOTE = "f2,f3,f6,f12,f13,f14,f104,f105"
FIELDS_FLOW = "f2,f3,f6,f12,f14,f62,f66,f72,f78,f84,f184"
RT_PROXY_ENV = "RT_PROXY"


def get_proxies() -> dict | None:
    """代理：优先 os.environ[RT_PROXY]；无则 None（requests 自动走系统代理，云端直连）"""
    p = os.environ.get(RT_PROXY_ENV, "").strip()
    if not p:
        return None
    return {"http": p, "https": p}


def stock_secid(stock_code: str) -> str:
    """个股 '600519.SH'/'600519' → '1.600519'；6/9 开头沪(1)，0/3 开头深(0)"""
    c = str(stock_code).split(".")[0].zfill(6)
    return f"1.{c}" if c.startswith(("6", "9")) else f"0.{c}"


def index_secid(index_code: str) -> str:
    """指数 → 东财 secid：
       399xxx(.SZ) → 0.399xxx；93xxxx(.CSI 中证2000) → 2.93xxxx；
       其余(000xxx.SH/.CSI) → 1.000xxx
    """
    c = str(index_code).split(".")[0].zfill(6)
    if c.startswith("399"):
        return f"0.{c}"
    if c.startswith("93"):
        return f"2.{c}"
    return f"1.{c}"


def etf_secid(fund_code6: str) -> str:
    """ETF '510300'→'1.510300'；'159915'→'0.159915'（5/6 开头沪，其余深）"""
    c = str(fund_code6).zfill(6)
    return f"1.{c}" if c.startswith(("5", "6")) else f"0.{c}"


def _ulist_get(secids: list[str], fields: str, proxies: dict | None = None,
               timeout: int = 30) -> list[dict]:
    """调一次 ulist 接口，返回 diff 列表；失败抛异常（由上层降级）"""
    url = URL_PUSH if proxies is None else URL_DELAY
    params = {
        "fltt": "2", "invt": "2", "secids": ",".join(secids), "fields": fields,
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=timeout, proxies=proxies)
    r.raise_for_status()
    return (r.json().get("data") or {}).get("diff") or []


# ============================================================
#  指数实时行情 + 市场情绪
# ============================================================

def fetch_index_quotes(index_codes: list[str], proxies: dict | None = None) -> pd.DataFrame:
    """拉指数实时行情 + 市场情绪（合并 1 次 ulist 请求）。

    返回 DataFrame 列：code(f12), name(f14), price(f2), pct_chg(f3),
                       amount(f6, 元), up_count(f104), down_count(f105)
    """
    # 上证综指 + 深证综指 提供 f104/f105（涨跌家数）与两市成交额
    secids = [index_secid(c) for c in index_codes] + ["1.000001", "0.399106"]
    rows = _ulist_get(secids, FIELDS_QUOTE, proxies=proxies)
    out = []
    for x in rows:
        out.append({
            "code": str(x.get("f12", "")).zfill(6),
            "name": str(x.get("f14", "")),
            "price": pd.to_numeric(x.get("f2"), errors="coerce"),
            "pct_chg": pd.to_numeric(x.get("f3"), errors="coerce"),
            "amount": pd.to_numeric(x.get("f6"), errors="coerce"),
            "up_count": pd.to_numeric(x.get("f104"), errors="coerce"),
            "down_count": pd.to_numeric(x.get("f105"), errors="coerce"),
        })
    return pd.DataFrame(out)


def fetch_market_sentiment(quotes: pd.DataFrame | None = None,
                           proxies: dict | None = None) -> dict:
    """市场情绪：上涨/下跌家数、两市总成交额。

    优先从已拉取的 quotes 派生（代码 000001/399106）；否则单独拉一次。
    """
    if quotes is None or quotes.empty:
        q = fetch_index_quotes([], proxies=proxies)
    else:
        q = quotes
    sent = {"up": 0, "down": 0, "amount": 0.0}
    sub = q[q["code"].isin(["000001", "399106"])]
    if sub.empty:
        return sent
    sent["up"] = int(sub["up_count"].fillna(0).sum())
    sent["down"] = int(sub["down_count"].fillna(0).sum())
    sent["amount"] = float(sub["amount"].fillna(0).sum())
    return sent


# ============================================================
#  成分股实时资金流（分批 ulist）
# ============================================================

def fetch_stock_flow_realtime(stock_codes: list[str],
                              proxies: dict | None = None,
                              batch_size: int = BATCH_SIZE) -> pd.DataFrame:
    """成分股实时资金流（对外 1 次调用，内部自动分批串行 ulist）。

    返回 DataFrame 列：
        stock_code(6位), stock_name, price, pct_chg, amount(元),
        main_net_amount, super_net_amount, big_net_amount,
        mid_net_amount, small_net_amount, main_net_ratio
    数值列 to_numeric(errors="coerce")（停牌/无数据返回 "-" → NaN）。
    任一批失败：打印异常，保留已成功批次，不抛错。
    """
    all_rows = []
    codes = list(dict.fromkeys(str(c).split(".")[0].zfill(6) for c in stock_codes))
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        try:
            rows = _ulist_get([stock_secid(c) for c in batch], FIELDS_FLOW, proxies=proxies)
        except Exception as e:
            print(f"  [实时资金流] 批次 {i // batch_size + 1} 失败: {e}")
            time.sleep(1)
            continue
        for x in rows:
            all_rows.append({
                "stock_code": str(x.get("f12", "")).zfill(6),
                "stock_name": str(x.get("f14", "")),
                "price": x.get("f2"),
                "pct_chg": x.get("f3"),
                "amount": x.get("f6"),
                "main_net_amount": x.get("f62"),
                "super_net_amount": x.get("f66"),
                "big_net_amount": x.get("f72"),
                "mid_net_amount": x.get("f78"),
                "small_net_amount": x.get("f84"),
                "main_net_ratio": x.get("f184"),
            })
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    for col in ["price", "pct_chg", "amount", "main_net_amount", "super_net_amount",
                "big_net_amount", "mid_net_amount", "small_net_amount", "main_net_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ============================================================
#  放量基准：近 N 日成交额（日 K 线）
# ============================================================

def fetch_daily_amount_history(secids: list[str], days: int = 5,
                               proxies: dict | None = None) -> dict:
    """拉近 days 个交易日的日线成交额（kline 一次 1 个标的）。

    返回 {secid: [近 days 日成交额(元), 旧→新]}；失败标的返回空列表。
    """
    result = {}
    for secid in secids:
        try:
            params = {
                "secid": secid, "klt": "101", "fqt": "0",
                "beg": "0", "end": "20500101", "lmt": str(days),
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "ut": "b2884a393a59ad64002292a3e90d46a5",
            }
            url = URL_KLINE if proxies is None else URL_KLINE
            r = requests.get(url, params=params, headers=HEADERS, timeout=20, proxies=proxies)
            klines = ((r.json().get("data") or {}).get("klines")) or []
            amounts = []
            for line in klines:
                parts = line.split(",")
                # klines 行: 日期,开,收,高,低,成交量,成交额,...
                if len(parts) >= 7:
                    amounts.append(pd.to_numeric(parts[6], errors="coerce"))
            result[secid] = [a for a in amounts if pd.notna(a)][-days:]
        except Exception as e:
            print(f"  [放量基准] {secid} 失败: {e}")
            result[secid] = []
        time.sleep(0.2)
    return result


# ============================================================
#  成分股集合 + 实时聚合（内存，不落盘）
# ============================================================

def build_index_map(index_codes: list[str]) -> tuple[dict, list[str]]:
    """组装指数→成分股集合。

    Returns:
        index_map: {index_code: sorted(6位成分股列表)}
        all_codes: 全部去重股票代码（6位）
    """
    from index_loader import load_index_weights
    index_map, all_set = {}, set()
    for code in index_codes:
        try:
            w = load_index_weights(code)
        except Exception as e:
            print(f"  [成分股] {code} 权重加载失败: {e}")
            continue
        stocks = sorted({str(c).split(".")[0].zfill(6) for c in w["stock_code"].dropna()})
        if stocks:
            index_map[code] = stocks
            all_set.update(stocks)
    return index_map, sorted(all_set)


def aggregate_flow_realtime(flow_df: pd.DataFrame, index_map: dict) -> pd.DataFrame:
    """按指数维度聚合实时主力净流入（内存 dict 求和，不做历史 merge）。

    flow_df: fetch_stock_flow_realtime 结果
    index_map: {index_code: [6位成分股]}

    返回 DataFrame 列：
        index_code, main_net(元), stock_count(有数据成分股数)
    """
    if flow_df is None or flow_df.empty:
        return pd.DataFrame()
    flow_map = dict(zip(flow_df["stock_code"], flow_df["main_net_amount"].fillna(0)))
    rows = []
    for code, stocks in index_map.items():
        vals = [flow_map.get(s) for s in stocks]
        present = [v for v in vals if v is not None]
        rows.append({
            "index_code": code,
            "main_net": float(sum(present)) if present else 0.0,
            "stock_count": len(present),
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    # 自测（本地：RT_PROXY 需先 export）
    from config import FOCUS_INDICES, FOCUS_NAMES
    px = get_proxies()
    print("代理:", px or "直连/系统代理")

    q = fetch_index_quotes(FOCUS_INDICES, proxies=px)
    print("\n指数行情:")
    print(q[["code", "name", "price", "pct_chg", "amount"]].to_string(index=False))

    sent = fetch_market_sentiment(q, proxies=px)
    print(f"\n市场情绪: 上涨 {sent['up']} / 下跌 {sent['down']} / 成交额 {sent['amount']/1e8:.0f}亿")

    imap, codes = build_index_map(FOCUS_INDICES)
    print(f"\n成分股集合: {len(imap)} 个指数, {len(codes)} 只去重股票")

    flow = fetch_stock_flow_realtime(codes[:600], proxies=px)  # 自测只拉前 600 只
    print(f"\n实时资金流: {len(flow)} 只 | 示例:")
    print(flow.head(3)[["stock_code", "stock_name", "price", "main_net_amount"]].to_string(index=False))

    agg = aggregate_flow_realtime(flow, imap)
    print("\n实时聚合(前5):")
    print(agg.head(5).to_string(index=False))
