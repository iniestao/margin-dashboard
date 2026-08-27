"""全局配置——路径、常量、默认参数"""

from pathlib import Path

# 项目根目录
ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"

# 缓存目录
MARGIN_DIR = DATA_DIR / "margin"
FUND_FLOW_DIR = DATA_DIR / "fund_flow"
MARKET_CAP_DIR = DATA_DIR / "market_cap"
AGGREGATED_DIR = DATA_DIR / "aggregated"
ETF_SCALE_DIR = DATA_DIR / "etf_scale"
ETF_NAV_DIR = DATA_DIR / "etf_nav"
CROWD_DIR = DATA_DIR / "crowd"
AMOUNT_CONC_CSV = CROWD_DIR / "amount_conc_hist.csv"
TOP5_DETAIL_CSV = CROWD_DIR / "top5_daily.csv"
STOCK_UNIVERSE_CSV = CROWD_DIR / "stock_universe.csv"
CROWD_LOOKBACK = 120          # 首次回补天数

# 用户提供的指数数据路径（相对路径，随项目移植）
INDEX_WEIGHT_DIR = ROOT / "指数权重"
ETF_LIST_PATH = INDEX_WEIGHT_DIR / "etf_list.csv"

# AKShare 融资融券数据日期范围（近100个交易日）
LOOKBACK_DAYS = 100

# 每次分批拉取的股票数量（防止 API 限流）
BATCH_SIZE = 20

# 请求间隔（秒）
REQUEST_INTERVAL = 0.5

# ============ 默认展示的宽基指数 ============
FOCUS_INDICES = [
    "000300.SH", "000905.SH", "000852.SH", "932000.CSI",
    "399006.SZ", "000688.SH", "000016.SH", "000510.CSI",
    "000906.SH", "399303.SZ",
]

FOCUS_NAMES = {
    "000300.SH": "沪深300", "000905.SH": "中证500", "000852.SH": "中证1000",
    "932000.CSI": "中证2000", "399006.SZ": "创业板指", "000688.SH": "科创50",
    "000016.SH": "上证50", "000510.CSI": "中证A500", "000906.SH": "中证800",
    "399303.SZ": "国证2000",
}

# ============ 全量指数（运行时自动扫描） ============
def discover_all_indices():
    """扫描指数权重目录，返回所有可用指数代码及其名称"""
    from index_loader import get_all_available_indices, load_etf_mapping
    codes = get_all_available_indices()
    etf = load_etf_mapping()
    name_map = {row["index_code"]: row["etf_name"] for _, row in etf.iterrows()}
    # 补充宽基名称
    name_map.update(FOCUS_NAMES)
    return [(c, name_map.get(c, c)) for c in sorted(codes)]
