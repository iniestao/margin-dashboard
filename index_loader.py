"""指数数据加载器：ETF 映射 + 指数成分股权重"""

import pandas as pd
from pathlib import Path
from config import INDEX_WEIGHT_DIR, ETF_LIST_PATH, FOCUS_INDICES


def load_etf_mapping() -> pd.DataFrame:
    """加载 ETF → 指数 映射表"""
    for enc in ["utf-8", "gbk", "gb2312", "gb18030"]:
        try:
            df = pd.read_csv(ETF_LIST_PATH, dtype=str, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        df = pd.read_csv(ETF_LIST_PATH, dtype=str, encoding="utf-8", errors="replace")
    df.columns = ["etf_code", "etf_name", "index_code"]
    return df


def load_index_weights(index_code: str) -> pd.DataFrame:
    """加载单个指数的成分股权重。

    从文件名匹配，如 000300.SH → 000300.SH_20260729.csv
    """
    files = list(INDEX_WEIGHT_DIR.glob(f"{index_code}_*.csv"))
    if not files:
        # 有些文件后缀不完全是 .SH/.SZ，比如 .CSI
        files = list(INDEX_WEIGHT_DIR.glob(f"{index_code}.*_*.csv"))
        # 也试下 CSI 后缀的处理——index code like "000510.CSI"
        if not files:
            raise FileNotFoundError(f"指数 {index_code} 的权重文件未找到")

    filepath = files[0]
    # 尝试多种编码（兼容外部工具导出的 CSV）
    for enc in ["utf-8", "gbk", "gb2312", "gb18030"]:
        try:
            df = pd.read_csv(filepath, dtype=str, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        df = pd.read_csv(filepath, dtype=str, encoding="utf-8", errors="replace")

    # 标准化列名（兼容中文）
    col_map = {}
    for c in df.columns:
        if "日期" in c or "date" in c.lower():
            col_map[c] = "date"
        elif "代码" in c or "code" in c.lower():
            col_map[c] = "stock_code"
        elif "名称" in c or "name" in c.lower():
            col_map[c] = "stock_name"
        elif "权重" in c or "weight" in c.lower():
            col_map[c] = "weight_pct"

    df = df.rename(columns=col_map)
    df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
    return df


def load_all_index_weights(index_codes: list[str]) -> dict[str, pd.DataFrame]:
    """批量加载多个指数的成分股权重"""
    result = {}
    for code in index_codes:
        try:
            result[code] = load_index_weights(code)
        except FileNotFoundError:
            print(f"  [跳过] 指数 {code} 无权重文件")
    return result


def get_all_available_indices() -> list[str]:
    """扫描文件夹，返回所有权重文件对应的指数代码"""
    codes = []
    for f in INDEX_WEIGHT_DIR.glob("*.csv"):
        if f.name == "etf_list.csv":
            continue
        try:
            # 文件名格式: 000300.SH_20260729.csv
            # 取第一个 _ 之前的部分作为 index_code
            index_code = f.stem.split("_")[0]
            codes.append(index_code)
        except Exception:
            continue
    return sorted(set(codes))


def get_unique_stocks(index_weights: dict[str, pd.DataFrame]) -> list[str]:
    """从所有已加载的指数中提取去重后的成分股代码列表"""
    all_stocks = set()
    for idx_code, df in index_weights.items():
        all_stocks.update(df["stock_code"].unique())
    return sorted(all_stocks)
