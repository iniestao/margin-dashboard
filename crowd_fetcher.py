"""全市场成交集中度（拥挤度）数据模块。

数据源：
    - 历史个股日线成交额：腾讯新端点 proxy.finance.qq.com/.../newfqkline/get（免费，无限制）
    - 当日全市场成交额：东财实时 clist（push2delay，1 请求全市场）
    - 全市场代码清单：akshare stock_zh_a_spot_em → 东财 clist → fund_flow 快照并集（逐级降级）

落盘：
    data/crowd/amount_conc_hist.csv   —— 每日"前5%成交额占比"标量序列（trade_date, top5_pct, total_amount, top5_amount, stock_count）
    data/crowd/stock_universe.csv     —— 全市场代码清单缓存

代理约定：不硬编码；经 get_proxies()/RT_PROXY 环境变量。
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests

URL_QQ_KLINE = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
URL_CLIST = "https://push2delay.eastmoney.com/api/qt/clist/get"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}
QQ_CNT = 640                      # 腾讯单请求最多行数（实测 640 个交易日）
CROWD_WORKERS = int(os.environ.get("CROWD_WORKERS", "12"))   # 回补并发度
CROWD_RETRIES = int(os.environ.get("CROWD_RETRIES", "3"))
CROWD_FAIL_RATIO_OK = 0.05        # 单批次失败股票占比阈值，超过则不写文件
# 沪深A股过滤（与 fund_flow_fetcher.FS_ALL 一致）：主板+创业板+科创板，不含北交所
FS_A = "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2"
FIELDS_AMOUNT = "f12,f14,f6"      # 代码/名称/成交额(元)

_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def _qq_symbol(code: str) -> str:
    """6 位股票代码 → 腾讯 symbol：6/9 开头 sh，0/3 开头 sz（沪深 A 股）"""
    c = str(code).split(".")[0].zfill(6)
    return f"sh{c}" if c.startswith(("6", "9")) else f"sz{c}"


def _snapshot_date() -> str:
    """最近一个已收盘交易日 YYYYMMDD"""
    from fund_flow_fetcher import _snapshot_date as _ff_date
    return _ff_date()


def fetch_qq_kline_amount(code: str, proxies: dict | None = None) -> pd.DataFrame:
    """腾讯个股日线成交额（单请求返回最近 QQ_CNT 个交易日）。

    返回 DataFrame(trade_date(datetime, 旧→新), amount(元))；失败抛异常由上层重试。
    """
    sym = _qq_symbol(code)
    start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")
    params = {"param": f"{sym},day,{start},{end},{QQ_CNT},qfq"}
    r = _get_session().get(URL_QQ_KLINE, params=params, headers=HEADERS, timeout=20, proxies=proxies)
    r.raise_for_status()
    data = ((r.json().get("data") or {}).get(sym)) or {}
    klines = data.get("qfqday") or data.get("day") or []
    rows = []
    for line in klines:
        # 行 10 字段：idx0=日期、idx7=换手率、idx8=成交额(万元)
        if len(line) < 9:
            continue
        d = pd.to_datetime(line[0], errors="coerce")
        a = pd.to_numeric(line[8], errors="coerce")
        if pd.notna(d) and pd.notna(a):
            rows.append((d, a * 10000.0))   # 万元 → 元
    if not rows:
        raise ValueError(f"{sym} 无 kline 数据")
    return (pd.DataFrame(rows, columns=["trade_date", "amount"])
              .sort_values("trade_date").reset_index(drop=True))


def fetch_stock_universe(proxies: dict | None = None) -> pd.DataFrame:
    """全市场沪深 A 股清单（含当日成交额），列 stock_code/stock_name/amount(元)。

    首选 akshare spot_em（东财实时，1 请求）；失败降级东财 clist 分页；再降级 fund_flow 快照并集（无成交额）。
    完整性校验：少于 3000 只视为残缺清单（会导致占比虚高），继续尝试下一来源。
    """
    try:
        import akshare as ak
        spot = ak.stock_zh_a_spot_em()
        spot = spot[["代码", "名称", "成交额"]].rename(
            columns={"代码": "stock_code", "名称": "stock_name", "成交额": "amount"})
        spot["stock_code"] = spot["stock_code"].astype(str).str.zfill(6)
        spot["amount"] = pd.to_numeric(spot["amount"], errors="coerce")
        spot = spot[spot["stock_code"].str.match(r"^(6|0|3)\d{5}$")]
        if len(spot) >= 3000:
            return spot.reset_index(drop=True)
        print(f"  [清单] ⚠️ akshare spot_em 仅 {len(spot)} 只（<3000），视为残缺，降级 clist")
    except Exception as e:
        print(f"  [清单] akshare spot_em 失败，降级 clist: {e}")
    # 降级：东财 clist 分页
    try:
        rows_all, total = [], 0
        for pn in range(1, 120):
            params = {"pn": pn, "pz": 100, "po": "1", "np": "1", "fltt": "2", "invt": "2",
                      "fid": "f6", "fs": FS_A, "fields": FIELDS_AMOUNT}
            r = _get_session().get(URL_CLIST, params=params, headers=HEADERS, timeout=30, proxies=proxies)
            data = r.json().get("data") or {}
            rows_all.extend(data.get("diff") or [])
            total = data.get("total") or 0
            if len(rows_all) >= total or not data.get("diff"):
                break
        if rows_all:
            df = pd.DataFrame([{
                "stock_code": str(x.get("f12", "")).zfill(6),
                "stock_name": str(x.get("f14", "")),
                "amount": pd.to_numeric(x.get("f6"), errors="coerce"),
            } for x in rows_all])
            df = df[df["stock_code"].str.match(r"^(6|0|3)\d{5}$")]
            if len(df) >= 3000:
                return df.reset_index(drop=True)
            print(f"  [清单] ⚠️ clist 分页仅得 {len(df)} 只（<3000），视为残缺")
    except Exception as e:
        print(f"  [清单] clist 降级失败: {e}")
    # 再降级：fund_flow 快照并集（仅清单，无成交额）
    from config import FUND_FLOW_DIR
    codes = set()
    for f in sorted(FUND_FLOW_DIR.glob("ff_*.parquet")):
        try:
            codes.update(pd.read_parquet(f, columns=["stock_code"])["stock_code"].astype(str))
        except Exception:
            continue
    codes = {c.zfill(6) for c in codes if c.zfill(6)[0] in ("6", "0", "3")}
    df = pd.DataFrame({"stock_code": sorted(codes), "stock_name": "", "amount": float("nan")})
    if len(df) < 3000:
        raise RuntimeError(f"全市场清单严重残缺（仅 {len(df)} 只），拒绝返回以防占比失真")
    return df


def load_stock_universe() -> pd.DataFrame:
    """读清单缓存 CSV；不存在返回空 DataFrame(columns=[stock_code, stock_name, as_of_date])"""
    from config import STOCK_UNIVERSE_CSV
    if STOCK_UNIVERSE_CSV.exists():
        return pd.read_csv(STOCK_UNIVERSE_CSV)
    return pd.DataFrame(columns=["stock_code", "stock_name", "as_of_date"])


def compute_top5_pct(amount_series: pd.Series) -> dict:
    """全市场前 5% 股票成交额占比：成交额前 top_n(=5%) 只合计 / 全市场成交额 ×100。

    返回 {"top5_pct": float, "total_amount": float, "top5_amount": float, "stock_count": int}
    """
    s = pd.to_numeric(amount_series, errors="coerce").dropna()
    s = s[s > 0]
    if s.empty:
        return {"top5_pct": float("nan"), "total_amount": 0.0, "top5_amount": 0.0, "stock_count": 0}
    total = float(s.sum())
    top_n = max(1, int(len(s) * 0.05))
    top5 = float(s.sort_values(ascending=False).head(top_n).sum())
    return {"top5_pct": top5 / total * 100 if total > 0 else float("nan"),
            "total_amount": total, "top5_amount": top5, "stock_count": int(len(s))}


def _top5_detail_rows(amount_series: pd.Series, name_map: dict | None = None) -> list[dict]:
    """返回当日前 5% 股票的明细行（rank 从 1 起，1=成交额最大）。

    行: {"stock_code", "stock_name", "amount", "rank", "share_pct"(占全市场%)}
    """
    s = pd.to_numeric(amount_series, errors="coerce").dropna()
    s = s[s > 0]
    if s.empty:
        return []
    total = float(s.sum())
    top_n = max(1, int(len(s) * 0.05))
    top = s.sort_values(ascending=False).head(top_n)
    name_map = name_map or {}
    rows = []
    for rank, (code, amt) in enumerate(top.items(), start=1):
        rows.append({
            "stock_code": str(code).zfill(6),
            "stock_name": str(name_map.get(str(code).zfill(6), name_map.get(str(code), ""))),
            "amount": float(amt),
            "rank": rank,
            "share_pct": round(float(amt) / total * 100, 3) if total > 0 else 0.0,
        })
    return rows


def _trading_dates(days: int) -> list:
    """近 days 个交易日（datetime，升序）"""
    from data_fetcher import _get_trading_dates
    dts = _get_trading_dates(days)
    return [pd.to_datetime(d) for d in sorted(dts)]


def backfill_amount_conc(days: int = 120, workers: int | None = None,
                         proxies: dict | None = None) -> dict:
    """历史回补：拉全市场个股成交额 → 逐日算前 5% 占比 → 落盘 CSV（断点续传）。

    返回 {"backfilled_dates": int, "total_dates": int, "failed_stocks": int, "total_stocks": int}
    """
    from config import CROWD_DIR, AMOUNT_CONC_CSV, STOCK_UNIVERSE_CSV
    CROWD_DIR.mkdir(parents=True, exist_ok=True)
    workers = workers or CROWD_WORKERS

    target_dates = _trading_dates(days)
    existing = set()
    if AMOUNT_CONC_CSV.exists():
        existing = set(pd.to_datetime(pd.read_csv(AMOUNT_CONC_CSV)["trade_date"]))
    missing = [d for d in target_dates if d not in existing]
    if not missing:
        print(f">>> 拥挤度回补：{days} 日已全部完成，跳过")
        return {"backfilled_dates": 0, "total_dates": len(target_dates),
                "failed_stocks": 0, "total_stocks": 0}

    # 全市场清单
    uni = fetch_stock_universe(proxies=proxies)
    codes = uni["stock_code"].astype(str).tolist()
    # 写清单缓存
    uni_out = uni[["stock_code", "stock_name"]].copy()
    uni_out["as_of_date"] = datetime.now().strftime("%Y-%m-%d")
    uni_out.to_csv(STOCK_UNIVERSE_CSV, index=False)

    print(f">>> 拥挤度回补：{len(missing)} 日缺失，{len(codes)} 只股票，并发 {workers}（约 20-40 分钟）")
    t0 = time.time()
    # 并发拉每只股票 → 按日累积 {date: {code: amount}}
    day_amounts: dict = {d: {} for d in missing}
    failed = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_qq_kline_amount, c, proxies): c for c in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            done += 1
            if done % 500 == 0:
                print(f"  已处理 {done}/{len(codes)}（{time.time()-t0:.0f}s）")
            try:
                df = fut.result()
                for _, r in df.iterrows():
                    d = r["trade_date"].normalize()
                    if d in day_amounts:
                        day_amounts[d][code] = float(r["amount"])
            except Exception as e:
                failed += 1
                if failed <= 3:
                    print(f"  [回补] {code} 失败: {str(e)[:80]}")

    # 逐日算占比 + 前5%明细（单日股票数过少的天直接剔除，防残缺数据污染）
    new_rows = []
    detail_rows = []
    name_map = dict(zip(uni["stock_code"].astype(str).str.zfill(6), uni["stock_name"].astype(str)))
    for d in sorted(day_amounts):
        amts = pd.Series(day_amounts[d])
        row = compute_top5_pct(amts)
        if row["stock_count"] < 3000:
            print(f"  [回补] ⚠️ {d.strftime('%Y-%m-%d')} 仅 {row['stock_count']} 只有效，跳过该日")
            continue
        new_rows.append({"trade_date": d.strftime("%Y-%m-%d"), **row})
        for dr in _top5_detail_rows(amts, name_map):
            detail_rows.append({"trade_date": d.strftime("%Y-%m-%d"), **dr})
    df_new = pd.DataFrame(new_rows)

    # 失败占比校验后写盘
    fail_ratio = failed / max(len(codes), 1)
    if fail_ratio > CROWD_FAIL_RATIO_OK:
        print(f"⚠️ 回补失败率 {fail_ratio:.1%} 超阈值 {CROWD_FAIL_RATIO_OK:.1%}，本批不写 CSV")
        return {"backfilled_dates": 0, "total_dates": len(target_dates),
                "failed_stocks": failed, "total_stocks": len(codes)}

    if AMOUNT_CONC_CSV.exists():
        old = pd.read_csv(AMOUNT_CONC_CSV)
        df_new = pd.concat([old, df_new], ignore_index=True)
    df_new = df_new.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    df_new.to_csv(AMOUNT_CONC_CSV, index=False)
    # 前5%明细落盘（追加/合并）
    _save_top5_detail(detail_rows)
    print(f"✅ 拥挤度回补完成：新增 {len(new_rows)} 日，失败 {failed}/{len(codes)}，耗时 {time.time()-t0:.0f}s")
    return {"backfilled_dates": len(new_rows), "total_dates": len(target_dates),
            "failed_stocks": failed, "total_stocks": len(codes)}


def _save_top5_detail(detail_rows: list[dict]) -> None:
    """前5%明细落盘 data/crowd/top5_daily.csv（按 (trade_date, rank) 合并去重）"""
    from config import CROWD_DIR, TOP5_DETAIL_CSV
    if not detail_rows:
        return
    CROWD_DIR.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame(detail_rows)
    if TOP5_DETAIL_CSV.exists():
        old = pd.read_csv(TOP5_DETAIL_CSV)
        df_new = pd.concat([old, df_new], ignore_index=True)
    df_new = (df_new.sort_values(["trade_date", "rank"])
                   .drop_duplicates(["trade_date", "rank"], keep="last"))
    df_new.to_csv(TOP5_DETAIL_CSV, index=False)


def update_amount_conc_daily(proxies: dict | None = None) -> bool:
    """每日增量：当日全市场成交额（东财实时 1 请求）→ 前 5% 占比 → append CSV。

    返回 True 表示新增了当日记录；CSV 已含当日则返回 False。
    """
    from config import CROWD_DIR, AMOUNT_CONC_CSV
    CROWD_DIR.mkdir(parents=True, exist_ok=True)
    target = _snapshot_date()
    hist = load_amount_conc_hist()
    if not hist.empty and hist["trade_date"].max().strftime("%Y%m%d") >= target:
        return False
    uni = fetch_stock_universe(proxies=proxies)
    if uni.empty or uni["amount"].isna().all():
        print("⚠️ 当日成交额获取失败，跳过增量")
        return False
    if int(uni["amount"].notna().sum()) < 3000:
        print(f"⚠️ 当日仅 {int(uni['amount'].notna().sum())} 只有效成交额（<3000），清单残缺，拒绝落盘以防占比失真")
        return False
    date_str = pd.to_datetime(target).strftime("%Y-%m-%d")
    row = {"trade_date": date_str, **compute_top5_pct(uni["amount"])}
    df_new = pd.DataFrame([row])
    if AMOUNT_CONC_CSV.exists():
        old = pd.read_csv(AMOUNT_CONC_CSV)
        df_new = pd.concat([old, df_new], ignore_index=True)
    df_new = df_new.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    df_new.to_csv(AMOUNT_CONC_CSV, index=False)
    # 当日前5%明细落盘
    name_map = dict(zip(uni["stock_code"].astype(str).str.zfill(6), uni["stock_name"].astype(str)))
    detail = [{"trade_date": date_str, **dr} for dr in _top5_detail_rows(uni["amount"], name_map)]
    _save_top5_detail(detail)
    print(f"✅ 拥挤度增量更新：{date_str} 前5%占比 {row['top5_pct']:.1f}%")
    return True


def ensure_crowd_history(proxies: dict | None = None) -> None:
    """更新流程入口：CSV 空/最新日期落后/明细缺失 → 回补；否则每日增量。供 run.py / cloud_update.py 复用。"""
    hist = load_amount_conc_hist()
    detail = load_top5_daily()
    try:
        latest_target = pd.to_datetime(_snapshot_date())
    except Exception:
        latest_target = None
    # 明细缺失（空 或 日期数 < 占比序列）→ 触发回补重建明细
    detail_incomplete = True
    if not hist.empty:
        detail_incomplete = (detail.empty
                             or len(detail["trade_date"].unique()) < len(hist["trade_date"].unique()))
    if hist.empty or latest_target is None or hist["trade_date"].max().normalize() < latest_target or detail_incomplete:
        backfill_amount_conc(days=int(os.environ.get("CROWD_LOOKBACK", "120")), proxies=proxies)
    else:
        update_amount_conc_daily(proxies=proxies)


def load_amount_conc_hist() -> pd.DataFrame:
    """读前 5% 占比历史序列；返回 DataFrame(trade_date(datetime 升序), top5_pct, total_amount, top5_amount, stock_count)；缺失返回空。"""
    from config import AMOUNT_CONC_CSV
    if not AMOUNT_CONC_CSV.exists():
        return pd.DataFrame(columns=["trade_date", "top5_pct", "total_amount", "top5_amount", "stock_count"])
    df = pd.read_csv(AMOUNT_CONC_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.sort_values("trade_date").reset_index(drop=True)


def load_top5_daily() -> pd.DataFrame:
    """读前 5% 明细；返回 DataFrame(trade_date(datetime), stock_code, stock_name, amount, rank, share_pct)；缺失返回空。"""
    from config import TOP5_DETAIL_CSV
    if not TOP5_DETAIL_CSV.exists():
        return pd.DataFrame(columns=["trade_date", "stock_code", "stock_name", "amount", "rank", "share_pct"])
    df = pd.read_csv(TOP5_DETAIL_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.sort_values(["trade_date", "rank"]).reset_index(drop=True)


if __name__ == "__main__":
    # 自测：单只股票成交额 + 占比计算
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
    df1 = fetch_qq_kline_amount("600519")
    print(f"600519 日线 {len(df1)} 行，尾 3 日成交额(亿)：{[round(x/1e8,1) for x in df1['amount'].tail(3)]}")
    s = pd.Series([100, 50, 30, 20, 10, 5, 3, 2, 1, 1])
    print("compute_top5_pct 测试:", compute_top5_pct(s))
