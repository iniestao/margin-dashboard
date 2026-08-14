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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
BATCH_SIZE = 400  # 每批 secids 上限（实测 300 只约 1.1s；400 批约 4495 只成分股 / 12 批）
FIELDS_QUOTE = "f2,f3,f5,f6,f12,f13,f14,f104,f105"   # f5=成交量(手) 供量比计算
FIELDS_FLOW = "f2,f3,f6,f12,f14,f62,f66,f72,f78,f84,f184"
RT_PROXY_ENV = "RT_PROXY"

# ── 并发拉取配置（可通过环境变量调整；东财限流风险时调低）──
RT_FLOW_WORKERS = int(os.environ.get("RT_FLOW_WORKERS", "4"))   # 成分股分批并发度
RT_FLOW_RETRIES = int(os.environ.get("RT_FLOW_RETRIES", "2"))   # 每批失败重试次数

_local = threading.local()          # 每线程独立 Session（keep-alive，避免跨线程共享）

def _get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


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


def _request_json(url: str, params: dict, proxies: dict | None, timeout: int) -> dict:
    r = _get_session().get(url, params=params, headers=HEADERS, timeout=timeout, proxies=proxies)
    r.raise_for_status()
    return r.json()


def _ulist_get(secids: list[str], fields: str, proxies: dict | None = None,
               timeout: int = 30) -> list[dict]:
    """调一次 ulist 接口，返回 diff 列表；失败抛异常（由上层降级）。

    域名自动降级：先 push2（云端直连），失败切 push2delay（本地代理/网络受限场景），
    避免依赖 RT_PROXY 设置。
    """
    params = {
        "fltt": "2", "invt": "2", "secids": ",".join(secids), "fields": fields,
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }
    last_err = None
    for url in (URL_PUSH, URL_DELAY):
        try:
            data = _request_json(url, params, proxies, timeout)
            return (data.get("data") or {}).get("diff") or []
        except Exception as e:
            last_err = e
    raise last_err


# ============================================================
#  指数实时行情 + 市场情绪
# ============================================================

def fetch_index_quotes(index_codes: list[str], proxies: dict | None = None) -> pd.DataFrame:
    """拉指数实时行情 + 市场情绪（合并 1 次 ulist 请求）。

    返回 DataFrame 列：code(f12), name(f14), price(f2), pct_chg(f3),
                       volume(f5, 手), amount(f6, 元), up_count(f104), down_count(f105)
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
            "volume": pd.to_numeric(x.get("f5"), errors="coerce"),
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
                              batch_size: int = BATCH_SIZE,
                              max_workers: int | None = None,
                              max_retries: int | None = None) -> pd.DataFrame:
    """成分股实时资金流（对外 1 次调用，内部自动分批**并发** ulist，默认 4 线程）。

    返回 DataFrame 列：
        stock_code(6位), stock_name, price, pct_chg, amount(元),
        main_net_amount, super_net_amount, big_net_amount,
        mid_net_amount, small_net_amount, main_net_ratio
    数值列 to_numeric(errors="coerce")（停牌/无数据返回 "-" → NaN）。
    任一批重试后仍失败：打印异常，保留已成功批次，不抛错（返回顺序不敏感）。
    """
    max_workers = max_workers or RT_FLOW_WORKERS
    max_retries = max_retries if max_retries is not None else RT_FLOW_RETRIES
    codes = list(dict.fromkeys(str(c).split(".")[0].zfill(6) for c in stock_codes))
    batches = [codes[i:i + batch_size] for i in range(0, len(codes), batch_size)]

    def _fetch_batch(idx: int, batch: list[str]):
        secids = [stock_secid(c) for c in batch]
        last_err = None
        for attempt in range(max_retries + 1):
            try:
                return idx, _ulist_get(secids, FIELDS_FLOW, proxies=proxies), None
            except Exception as e:
                last_err = e
                if attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))   # 退避 0.5s / 1s
        return idx, [], last_err

    all_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_fetch_batch, i, b) for i, b in enumerate(batches)]
        for fut in as_completed(futures):
            idx, rows, err = fut.result()
            if err is not None:
                print(f"  [实时资金流] 批次 {idx + 1} 失败(重试 {max_retries} 次后): {err}")
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
                               proxies: dict | None = None,
                               max_workers: int = 6) -> dict:
    """拉近 days 个交易日的日线成交额（kline，多标的**并发**，默认 6 线程）。

    返回 {secid: [近 days 日成交额(元), 旧→新]}；失败标的返回空列表。
    """

    def _fetch(secid: str):
        try:
            params = {
                "secid": secid, "klt": "101", "fqt": "0",
                "beg": "0", "end": "20500101", "lmt": str(days),
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "ut": "b2884a393a59ad64002292a3e90d46a5",
            }
            r = _get_session().get(URL_KLINE, params=params, headers=HEADERS,
                                   timeout=20, proxies=proxies)
            klines = ((r.json().get("data") or {}).get("klines")) or []
            amounts = []
            for line in klines:
                parts = line.split(",")
                # klines 行: 日期,开,收,高,低,成交量,成交额,...
                if len(parts) >= 7:
                    amounts.append(pd.to_numeric(parts[6], errors="coerce"))
            return secid, [a for a in amounts if pd.notna(a)][-days:]
        except Exception as e:
            print(f"  [放量基准] {secid} 失败: {e}")
            return secid, []

    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_fetch, s) for s in secids]
        for fut in as_completed(futures):
            secid, amounts = fut.result()
            result[secid] = amounts
    return result


def fetch_index_kline_close(secid: str, days: int = 500,
                            proxies: dict | None = None) -> pd.DataFrame:
    """拉指数日线收盘点位（复用 kline/get 接口，用于 Tab1 叠加上证指数走势）。

    返回 DataFrame(date(datetime, 旧→新), close(float))；失败返回空 DataFrame(columns=["date","close"])。
    """
    params = {
        "secid": secid, "klt": "101", "fqt": "0",
        "beg": "0", "end": "20500101", "lmt": str(days),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }
    try:
        r = requests.get(URL_KLINE, params=params, headers=HEADERS, timeout=20, proxies=proxies)
        klines = ((r.json().get("data") or {}).get("klines")) or []
        rows = []
        for line in klines:
            parts = line.split(",")          # 日期,开,收,高,低,成交量,成交额,...
            if len(parts) < 3:
                continue
            d = pd.to_datetime(parts[0], errors="coerce")   # "YYYY-MM-DD"
            c = pd.to_numeric(parts[2], errors="coerce")    # 收盘点位
            if pd.notna(d) and pd.notna(c):
                rows.append((d, c))
        return (pd.DataFrame(rows, columns=["date", "close"])
                  .sort_values("date").reset_index(drop=True))
    except Exception as e:
        print(f"  [指数K线] {secid} 失败: {e}")
        return pd.DataFrame(columns=["date", "close"])


def _sina_index_symbol(secid: str) -> str:
    """东财 secid → 新浪 symbol：1.000001→sh000001、0.399006→sz399006、2.93xxxx→sh93xxxx"""
    market, code = secid.split(".")
    if market == "0":
        return f"sz{code}"
    return f"sh{code}"


def fetch_index_volume_history(secids: list[str], days: int = 5,
                               max_workers: int = 6) -> dict:
    """指数近 N 日历史**成交量（手）**（新浪源，公网稳定；东财 push2his 常被限制）。

    返回 {secid: [近 days 日成交量(手), 旧→新]}；失败标的返回空列表。
    新浪 volume 单位为股 → 统一转手（/100），与东财实时 f5（手）口径一致。
    """

    def _fetch(secid: str):
        try:
            import akshare as ak
            sym = _sina_index_symbol(secid)
            k = ak.stock_zh_index_daily(symbol=sym)
            vols = pd.to_numeric(k["volume"], errors="coerce").dropna()
            return secid, [float(v) / 100.0 for v in vols.tail(days)]   # 股 → 手
        except Exception as e:
            print(f"  [量比基准] {secid} 失败: {e}")
            return secid, []

    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_fetch, s) for s in secids]
        for fut in as_completed(futures):
            secid, vols = fut.result()
            result[secid] = vols
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
    """按指数维度聚合实时 主力/中单/小单 净流入（内存 dict 求和，不做历史 merge）。

    flow_df: fetch_stock_flow_realtime 结果
    index_map: {index_code: [6位成分股]}

    返回 DataFrame 列：
        index_code, main_net(元), mid_net(元), small_net(元), stock_count(有数据成分股数)
    """
    if flow_df is None or flow_df.empty:
        return pd.DataFrame()
    main_map = dict(zip(flow_df["stock_code"], flow_df["main_net_amount"].fillna(0)))
    mid_map = dict(zip(flow_df["stock_code"], flow_df["mid_net_amount"].fillna(0)))
    small_map = dict(zip(flow_df["stock_code"], flow_df["small_net_amount"].fillna(0)))
    rows = []
    for code, stocks in index_map.items():
        m = [main_map.get(s) for s in stocks]
        md = [mid_map.get(s) for s in stocks]
        sm = [small_map.get(s) for s in stocks]
        pm = [v for v in m if v is not None]
        p_mid = [v for v in md if v is not None]
        p_sm = [v for v in sm if v is not None]
        rows.append({
            "index_code": code,
            "main_net": float(sum(pm)) if pm else 0.0,
            "mid_net": float(sum(p_mid)) if p_mid else 0.0,
            "small_net": float(sum(p_sm)) if p_sm else 0.0,
            "stock_count": len(pm),
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
