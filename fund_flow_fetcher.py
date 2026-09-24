"""个股资金流向拉取模块 —— 东方财富资金流向（data.eastmoney.com/zjlx）

数据源：push2delay.eastmoney.com/api/qt/clist/get（东财资金流排行接口，免费）
口径：主力/超大单/大单/中单/小单净流入（按单笔成交金额分档推算，行业标准口径）
     超大单 >100万或50万股、大单 20-100万、中单 4-20万、小单 <4万

缓存：每个交易日一个快照 data/fund_flow/ff_YYYYMMDD.parquet（全市场 ~5200 只）
列：trade_date, stock_code, stock_name, main_net_amount, super_net_amount,
    big_net_amount, mid_net_amount, small_net_amount, main_net_ratio

说明：接口无日期参数，返回"最新已收盘交易日"的完整快照；凌晨/早间拉取即昨日数据。
"""

import time
from datetime import datetime

import requests
import pandas as pd

from config import FUND_FLOW_DIR

URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
# 备用域名：主域名返回空响应/非 JSON（云端限流常见）时自动切换
URL_FALLBACKS = ["https://push2.eastmoney.com/api/qt/clist/get"]
# 快照整轮拉取上限（秒）：正常约 1-2 分钟，超过即中止——防云端跑批因接口异常无限挂起
SNAPSHOT_DEADLINE_SEC = 600
# 全市场完整快照下限：正常 ~5200 只；低于此值说明接口返回残缺数据（total 也会被一起骗小，
# 光靠 len(df) >= total 拦不住），必须拒绝缓存，否则残缺数据会永久留在缓存里（快照"已缓存即跳过"）
MIN_FULL_UNIVERSE = 3000
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}
# 沪深A股全部（主板+创业板+科创板）
FS_ALL = "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2"
FIELDS = "f12,f14,f62,f66,f72,f78,f84,f184"


def _snapshot_date() -> str:
    """返回快照对应的交易日 YYYYMMDD（最近一个已收盘交易日）"""
    from data_fetcher import _get_trading_dates
    dates = _get_trading_dates(10)  # 降序，dates[0] 最近
    if not dates:
        return datetime.now().strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")
    if dates[0] == today and datetime.now().hour < 15:
        # 今天尚未收盘，资金流快照是上一交易日的
        return dates[1] if len(dates) > 1 else dates[0]
    return dates[0]


def _fetch_page(pn: int, pz: int = 100, proxies: dict | None = None) -> tuple:
    """拉取一页，返回 (rows, total)。

    云端偶发空响应/非 JSON（限流），逐个域名尝试；全部失败才抛异常（带诊断信息），
    由上层做有限次退避重试——**绝不在此处无限重试**。
    """
    params = {
        "pn": str(pn), "pz": str(pz), "po": "1", "np": "1",
        "fltt": "2", "invt": "2", "fid": "f62",
        "fs": FS_ALL, "fields": FIELDS,
    }
    last_err = None
    for url in [URL] + URL_FALLBACKS:
        host = url.split("/")[2]
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30, proxies=proxies)
            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code} @{host}")
            if not r.text.strip():
                raise ValueError(f"空响应 @{host}")
            data = (r.json().get("data") or {})
            return (data.get("diff") or []), (data.get("total") or 0)
        except Exception as e:
            last_err = e
    raise ValueError(f"全部域名失败: {str(last_err)[:100]}")


def fetch_fund_flow_snapshot(proxies: dict | None = None) -> pd.DataFrame:
    """拉取全市场个股资金流向快照并缓存（当日已缓存则直接读取）"""
    FUND_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    date_str = _snapshot_date()
    cache = FUND_FLOW_DIR / f"ff_{date_str}.parquet"
    if cache.exists():
        cached = pd.read_parquet(cache)
        if len(cached) >= MIN_FULL_UNIVERSE:
            print(f">>> 资金流向 {date_str} 已缓存 ({len(cached)} 只)")
            return cached
        # 残缺缓存（历史回补只写成分股子集）→ 不当作已缓存，强制重拉全市场
        print(f"⚠️ 资金流向 {date_str} 缓存残缺（{len(cached)} 只 < {MIN_FULL_UNIVERSE}），强制重拉全市场")

    all_rows, total, pn = [], 0, 1
    max_pages = 200          # 全市场约 53 页，防死循环
    max_tries = 4            # 每页最多尝试 4 次（含首次），退避 2/4/8 秒
    deadline = time.time() + SNAPSHOT_DEADLINE_SEC     # 整轮上限，防云端跑批无限挂起（曾挂 3 小时）
    failed_pages = []
    while pn <= max_pages:
        rows, last_err = None, None
        for attempt in range(1, max_tries + 1):
            try:
                rows, total = _fetch_page(pn, proxies=proxies)
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < max_tries:
                    wait = 2 ** attempt
                    print(f"  [资金流向] 第 {pn} 页失败({attempt}/{max_tries}): {str(e)[:80]}，{wait}s 后重试")
                    time.sleep(wait)
        if rows is None:
            failed_pages.append(pn)
            print(f"  [资金流向] 第 {pn} 页暂放弃（已试 {max_tries} 次）: {str(last_err)[:80]}")
            pn += 1
        else:
            all_rows.extend(rows)
            if len(all_rows) >= total or not rows:
                break
            pn += 1
            time.sleep(0.2)
        if time.time() > deadline:
            print(f"  [资金流向] 整轮超过 {SNAPSHOT_DEADLINE_SEC//60} 分钟上限，中止（已拉到 {len(all_rows)} 只，失败 {len(failed_pages)} 页）")
            break

    # 二次补拉：接口抖动多为瞬时，对失败页再各试一轮（仍有上限，不会无限挂）
    if failed_pages and time.time() < deadline:
        print(f"  [资金流向] 二次补拉 {len(failed_pages)} 个失败页: {failed_pages}")
        still_failed = []
        for pn2 in failed_pages:
            rows2 = None
            for attempt in range(1, max_tries + 1):
                try:
                    rows2, total = _fetch_page(pn2, proxies=proxies)
                    break
                except Exception:
                    if attempt < max_tries:
                        time.sleep(2 ** attempt)
            if rows2:
                all_rows.extend(rows2)
                print(f"  [资金流向] 第 {pn2} 页补拉成功（+{len(rows2)} 只）")
            else:
                still_failed.append(pn2)
            if time.time() > deadline:
                print(f"  [资金流向] 补拉超时中止，剩余未补: {still_failed + failed_pages[failed_pages.index(pn2)+1:]}")
                break
        failed_pages = still_failed

    if not all_rows:
        print(f">>> 资金流向 {date_str}: 无数据")
        return pd.DataFrame()
    if total and len(all_rows) < total:
        print(f"⚠️ 资金流向 {date_str}: 仅拉到 {len(all_rows)}/{total} 只（不完整）")

    df = pd.DataFrame([{
        "trade_date": date_str,
        "stock_code": str(x.get("f12", "")).zfill(6),
        "stock_name": str(x.get("f14", "")),
        "main_net_amount": x.get("f62"),
        "super_net_amount": x.get("f66"),
        "big_net_amount": x.get("f72"),
        "mid_net_amount": x.get("f78"),
        "small_net_amount": x.get("f84"),
        "main_net_ratio": x.get("f184"),
    } for x in all_rows])
    df = df.dropna(subset=["stock_code"])
    # 东财对无数据（如停牌）字段返回 "-"，统一转 NaN
    for col in ["main_net_amount", "super_net_amount", "big_net_amount",
                "mid_net_amount", "small_net_amount", "main_net_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 不完整（远少于 total）或有整页失败时不缓存，避免污染后续聚合
    if total and len(df) < total * 0.9:
        print(f"⚠️ 资金流向 {date_str} 数据不完整（{len(df)}/{total}），本次不缓存")
        return df
    if failed_pages:
        print(f"⚠️ 资金流向 {date_str}: {len(failed_pages)} 页补拉后仍失败 {failed_pages}（{len(df)}/{total}），本次不缓存，下次跑批自动重试")
        return df
    # 残缺保护：接口异常时 total 会一起被报小（实测曾返回 84/107 只且 total 同值），
    # 上面的比率校验拦不住，必须按全市场绝对下限再兜一层
    if len(df) < MIN_FULL_UNIVERSE:
        print(f"⚠️ 资金流向 {date_str} 仅 {len(df)} 只（低于全市场下限 {MIN_FULL_UNIVERSE}，total={total} 不可信），本次不缓存，下次跑批自动重试")
        return df
    df.to_parquet(cache, index=False)
    print(f">>> 资金流向 {date_str}: {len(df)} 只 (total={total})")
    return df


def load_fund_flow_cache(days: int = 30) -> pd.DataFrame:
    """读取已缓存的资金流向快照（用于聚合/看板）。

    只加载最近 days 个交易日（默认 30，覆盖看板全部 20 日窗口），并降精度存储
    （字符串列 category、数值列 float32）——云端 1GB 内存下防止 OOM 白屏。
    days=0 表示不限制（crowd 清单降级等场景）。
    """
    files = sorted(FUND_FLOW_DIR.glob("ff_*.parquet"))
    if days and days > 0:
        files = files[-days:]          # 文件名 ff_YYYYMMDD 排序即日期序
    if not files:
        return pd.DataFrame()
    num_cols = ["main_net_amount", "super_net_amount", "big_net_amount",
                "mid_net_amount", "small_net_amount", "main_net_ratio"]
    dfs = []
    for f in files:
        d = pd.read_parquet(f)
        for c in num_cols:
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce").astype("float32")
        for c in ("stock_code", "stock_name", "ts_code"):
            if c in d.columns:
                d[c] = d[c].astype("category")
        dfs.append(d)
    df = pd.concat(dfs, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    # aggregator 按 ts_code 过滤成分股（6位代码可直接匹配），补上兼容列
    if "stock_code" in df.columns:
        df["ts_code"] = df["stock_code"]
    return df.dropna(subset=["trade_date"])


# ============================================================
#  历史回补：东财个股资金流历史（daykline），首次运行拉过去 N 个交易日
#  说明：快照接口只有当日，历史需逐只拉取（成分股子集），仅补缺失日期
# ============================================================

_HIST_URL = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"


def load_name_map() -> dict:
    """从全市场清单缓存读 6 位代码 -> 名称 映射（历史回补补名称用）。

    历史回补接口只返回净额，不带名称；若留空，明细/看板会显示"只有代码没有名称"。
    """
    from config import STOCK_UNIVERSE_CSV
    try:
        if not STOCK_UNIVERSE_CSV.exists():
            return {}
        u = pd.read_csv(STOCK_UNIVERSE_CSV, dtype={"stock_code": str})
        c6 = u["stock_code"].astype(str).str.split(".").str[0].str.zfill(6)
        return {k: v for k, v in zip(c6, u["stock_name"].astype(str)) if v and v != "nan"}
    except Exception as e:
        print(f"  [资金流向] 清单名称映射读取失败（{str(e)[:60]}），回补文件名称将为空")
        return {}


def _market_id(stock_code: str) -> int:
    """6/9 开头=沪市(1)，其余=深市(0)"""
    return 1 if str(stock_code).startswith(("6", "9")) else 0


def _fetch_stock_flow_history(stock_code: str, proxies: dict | None = None) -> pd.DataFrame:
    """拉取单只股票的历史资金流（全量日线）"""
    params = {
        "lmt": "0", "klt": "101",
        "secid": f"{_market_id(stock_code)}.{stock_code}",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    r = requests.get(_HIST_URL, params=params, headers=HEADERS, timeout=30, proxies=proxies)
    data = (r.json().get("data") or {})
    klines = data.get("klines") or []
    rows = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        rows.append({
            "trade_date": parts[0].replace("-", ""),
            "stock_code": stock_code,
            "main_net_amount": parts[1],
            "small_net_amount": parts[2],
            "mid_net_amount": parts[3],
            "big_net_amount": parts[4],
            "super_net_amount": parts[5],
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ["main_net_amount", "small_net_amount", "mid_net_amount",
                "big_net_amount", "super_net_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def fetch_fund_flow_history(stock_codes: list[str], dates: list[str],
                            proxies: dict | None = None,
                            workers: int = 8, max_codes: int | None = None,
                            overwrite_dates: list[str] | None = None,
                            tries: int = 3) -> dict:
    """对成分股逐只回补历史资金流，写入缺失日期的 ff_YYYYMMDD.parquet

    首次运行把过去 N 个交易日补齐（与其他数据一致）；已有快照的日期跳过。
    overwrite_dates: 即使已缓存也重拉（用于修复残缺缓存，如历史回补只写了成分股子集的日子）。
    tries: 单只股票的最大尝试次数——东财 push2his 走代理失败率较高（实测约 70%），
           必须重试，否则回补只能成功百来只（这正是 9/22、9/23 快照残缺的成因）。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    FUND_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    cached = {f.stem.split("_")[1] for f in FUND_FLOW_DIR.glob("ff_*.parquet")}
    missing_dates = sorted(set(dates) - cached)
    # 强制重拉的日期（修复残缺缓存）
    if overwrite_dates:
        forced = sorted(set(dates) & set(overwrite_dates))
        if forced:
            print(f">>> 资金流向：强制重拉指定日期 {forced}")
            missing_dates = sorted(set(missing_dates) | set(forced))
    # 排除当日（T 日盘中数据不完整，快照由 fetch_fund_flow_snapshot 在数据完整后单独写入）
    today_str = datetime.now().strftime("%Y%m%d")
    skipped_today = [d for d in missing_dates if d >= today_str]
    if skipped_today:
        missing_dates = [d for d in missing_dates if d < today_str]
        print(f">>> 资金流向历史回补：跳过当日/未来日 {skipped_today}（盘中数据不完整）")
    if not missing_dates:
        print(f">>> 资金流向历史缓存完整（{len(cached)} 个交易日），无需回补")
        return {}
    if max_codes:
        stock_codes = stock_codes[:max_codes]

    print(f">>> 资金流向历史回补：{len(stock_codes)} 只成分股 × {len(missing_dates)} 个缺失交易日（{missing_dates[-1]}~{missing_dates[0]}）")

    # 并行拉取每只股票的历史
    daily_rows = {d: [] for d in missing_dates}
    done = 0
    failed = 0

    def _worker(code):
        last_err = None
        for attempt in range(1, tries + 1):
            try:
                d = _fetch_stock_flow_history(code, proxies)
                if d is not None and not d.empty:
                    return code, d
                last_err = "返回空"
            except Exception as e:
                last_err = e
            if attempt < tries:
                time.sleep(0.5 * attempt)
        return code, None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_worker, c) for c in stock_codes]
        for fut in as_completed(futs):
            code, hist = fut.result()
            done += 1
            if done % 200 == 0:
                print(f"    回补进度: {done}/{len(stock_codes)}（失败 {failed}）")
            if hist is None or hist.empty:
                failed += 1
                continue
            for _, r in hist.iterrows():
                d = str(r["trade_date"])
                if d in daily_rows:
                    daily_rows[d].append({
                        "stock_code": code,
                        "main_net_amount": r["main_net_amount"],
                        "super_net_amount": r["super_net_amount"],
                        "big_net_amount": r["big_net_amount"],
                        "mid_net_amount": r["mid_net_amount"],
                        "small_net_amount": r["small_net_amount"],
                    })

    written = 0
    name_map = load_name_map()
    skipped_full = []
    for d, rows in daily_rows.items():
        if not rows:
            continue
        out = FUND_FLOW_DIR / f"ff_{d}.parquet"
        # 绝不覆盖已有的完整快照：回补只有成分股子集，覆盖会让全市场数据退化成子集
        if out.exists():
            try:
                if len(pd.read_parquet(out)) >= MIN_FULL_UNIVERSE:
                    skipped_full.append(d)
                    continue
            except Exception:
                pass
        df = pd.DataFrame(rows)
        df.insert(0, "trade_date", d)
        df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
        df["stock_name"] = df["stock_code"].map(name_map).fillna("")   # 补名称，避免"只有代码没名称"
        df["main_net_ratio"] = None
        df.to_parquet(out, index=False)
        written += 1
    if skipped_full:
        print(f">>> 已有完整快照、跳过覆盖的日期: {skipped_full}")
    print(f">>> 资金流向历史回补完成：写入 {written} 个交易日（失败 {failed} 只 / 共 {len(stock_codes)} 只）")
    return {"written": written, "dates": missing_dates, "failed": failed}


if __name__ == "__main__":
    # 自测：拉取当日资金流向快照
    df = fetch_fund_flow_snapshot()
    if not df.empty:
        print("\n最新快照（主力净流入 TOP5）:")
        top = df.sort_values("main_net_amount", ascending=False).head(5)
        for _, r in top.iterrows():
            print(f"  {r['stock_code']} {r['stock_name']}: 主力净流入 {r['main_net_amount']/1e8:+.2f} 亿")
        print("\n缓存文件:", sorted(FUND_FLOW_DIR.glob("*.parquet")))
