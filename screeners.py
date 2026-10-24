# -*- coding: utf-8 -*-
"""
A 股多系统选股筛选器

系统一 · 洗盘回踩（列出全部）
  非ST | 剔除北交所 | 流通市值100-300亿
  近20日内出现过涨停 | 今日收阴线 | 今日缩量
  收盘价靠近10日均线(±3%) | 10日均线向上

系统二 · 放量突破（列出全部）
  涨幅3%-5% | 量比>1 | 换手5%-10% | 流通市值50-200亿
  成交量持续放大 | 上方无压力 | 分时价>成交均价

系统三 · 洗盘回踩最优一只（独立筛选，条件同系统一，只保留评分最高的一只）

系统四 · 放量突破最优一只（独立筛选，条件同系统二，只保留评分最高的一只）

系统五 · 强势多头突破（列出全部）
  涨幅3%-5% | 量比>1.5 | 换手5%-10% | 流通市值50-200亿
  当日量>前5日均量1.5倍 | 距20日高≤3%且上方无放量滞涨(套牢区)
  分时价>成交均价 | MA5>MA10>MA20 | 收盘位于全天振幅上方70% | 所属板块强于大盘

输出：~/Downloads/股票筛选/stock-screen_YYYYMMDD_HHMMSS.html
"""

import json, os, re, ssl, threading, time, urllib.request, html as _html
from datetime import datetime
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import certifi
import pandas as pd

# ============================================================
# 参数
# ============================================================
UA = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 Chrome/151 Safari/537.36",
    "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://quote.eastmoney.com/", "Connection": "close",
}
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

# ------------------------------------------------------------
# 网络：默认直连、忽略系统代理
#   部分电脑开着本地代理(如 Clash/V2Ray 127.0.0.1:7890)，urllib 会自动读取
#   Windows 系统代理设置；一旦代理未启动或线路不通，所有请求都会失败，
#   程序便会误报“没有网络连接”。A股行情接口国内直连即可，故默认不走代理。
#   如需强制走系统代理：设置环境变量 YZ_USE_PROXY=1
# ------------------------------------------------------------
USE_SYSTEM_PROXY = os.environ.get("YZ_USE_PROXY", "0").strip().lower() in ("1", "true", "yes")

def _make_opener(use_proxy):
    handlers = [urllib.request.HTTPSHandler(context=SSL_CONTEXT)]
    if not use_proxy:
        handlers.insert(0, urllib.request.ProxyHandler({}))  # 空字典 = 禁用代理
    return urllib.request.build_opener(*handlers)

# 直连优先，失败再回退“系统代理”，兼顾两种环境
_OPENERS = ([_make_opener(True), _make_opener(False)] if USE_SYSTEM_PROXY
            else [_make_opener(False), _make_opener(True)])

def _open(req, timeout=15):
    last = None
    for op in _OPENERS:
        try:
            return op.open(req, timeout=timeout)
        except Exception as e:
            last = e
    raise last or RuntimeError("open failed")

def _intraday_progress():
    """今日已交易时间占全日比例（0-1）。收盘后/非交易日返回 1.0。
       用于盘中把“累计成交量”折算为“预计全日成交量”。"""
    now = datetime.now()
    if now.weekday() >= 5:
        return 1.0
    t = now.hour * 60 + now.minute
    if t >= 15 * 60:          return 1.0
    if t <= 9 * 60 + 30:      return 0.0
    if t <= 11 * 60 + 30:     elapsed = t - (9 * 60 + 30)
    elif t < 13 * 60:         elapsed = 120
    else:                     elapsed = 120 + (t - 13 * 60)
    return max(0.05, min(1.0, elapsed / 240))

def net_hint():
    """打印网络诊断信息，便于排查“没有网络连接”类问题。"""
    try:
        proxies = urllib.request.getproxies()
    except Exception:
        proxies = {}
    print("  网络诊断：")
    print(f"    系统检测到的代理: {proxies or '无'}")
    print(f"    本程序策略: {'使用系统代理' if USE_SYSTEM_PROXY else '直连（已忽略系统代理）'}")
    if proxies and not USE_SYSTEM_PROXY:
        print("    （若直连仍不通，可临时设环境变量 YZ_USE_PROXY=1 改走代理后重试）")

S1_MV_MIN, S1_MV_MAX = 100.0, 300.0
S1_ZT_DAYS           = 20
S1_MA10_NEAR         = 3.0

S2_CHG_MIN, S2_CHG_MAX   = 3.0, 5.0
S2_VOL_RATIO_MIN         = 1.0
S2_TURN_MIN, S2_TURN_MAX = 5.0, 10.0
S2_MV_MIN, S2_MV_MAX     = 50.0, 200.0
S2_DIST_HIGH20           = 3.0

# ---- 系统五 · 强势多头突破 ----
S5_CHG_MIN, S5_CHG_MAX   = 3.0, 5.0     # 当日涨幅 3%-5%
S5_VOL_RATIO_MIN         = 1.5          # 量比 > 1.5
S5_TURN_MIN, S5_TURN_MAX = 5.0, 10.0    # 换手率 5%-10%
S5_MV_MIN, S5_MV_MAX     = 50.0, 200.0  # 流通市值 50-200亿
S5_VOL_MULT              = 1.5          # 当日量 > 前5日均量 × 1.5
S5_DIST_HIGH20           = 3.0          # 距20日高 ≤ 3%
S5_TRAP_WIN              = 20           # 套牢区检查窗口（近20日）
S5_TRAP_VOL_MULT         = 1.5          # 放量滞涨：当日量 ≥ 前5日均量 × 1.5
S5_TRAP_GAIN_MAX         = 2.0          # 且当日涨幅 < 2%（含下跌）视为滞涨
S5_RANGE_POS_MIN         = 70.0         # 收盘位于全天振幅上方70%区域
S5_BLOCK_DIFF_MIN        = 0.0          # 所属板块涨幅需 > 大盘涨幅

KLINE_N     = 80
MIN_KLINES  = 22
PAGE_SIZE   = 100
MAX_WORKERS = 6
KLINE_SLEEP = 0.04

# ============================================================
# 工具
# ============================================================
def board_of(code):
    if code.startswith(("688", "689")): return "科创板"
    if code.startswith(("300", "301", "302")): return "创业板"
    return "主板"

def limit_pct(code):
    return 0.20 if code.startswith(("688", "689", "300", "301", "302")) else 0.10

def is_limit_up(close, prev_close, code):
    if prev_close <= 0: return False
    return (close - prev_close) / prev_close >= limit_pct(code) * 0.95

def http_json(url, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA, method="GET")
            with _open(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last = e
            if i < retries - 1: time.sleep(0.5 * (i + 1))
    raise last or RuntimeError("HTTP failed")

# ============================================================
# 全市场列表
# ============================================================
LIST_HOSTS = [
    "https://push2delay.eastmoney.com/api/qt/clist/get?",
    "https://push2.eastmoney.com/api/qt/clist/get?",
]

def fetch_list_page(pn):
    qs = (
        f"pn={pn}&pz={PAGE_SIZE}&po=1&np=1&fltt=2&invt=2&fid=f3"
        "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
        "&fields=f2,f3,f5,f6,f8,f10,f12,f14,f15,f16,f17,f18,f21,f100"
    )
    last = None
    for h in LIST_HOSTS:
        try: return http_json(h + qs)
        except Exception as e: last = e
    raise last or RuntimeError("列表接口失败")

def build_all_stocks():
    stocks, seen = [], set()
    last_err = None
    print("扫描全市场……")
    for pn in range(1, 60):
        try:
            d = fetch_list_page(pn)
        except Exception as e:
            last_err = e
            print(f"\n第{pn}页请求失败: {type(e).__name__}: {str(e)[:80]}")
            break
        data = d.get("data") or {}
        rows = data.get("diff") or []
        if not rows:
            if pn == 1:
                print(f"\n第1页返回空数据（接口可能有变动或返回异常）")
            break

        for x in rows:
            code = str(x.get("f12") or "")
            name = str(x.get("f14") or "")
            if not code or code in seen: continue
            seen.add(code)
            if "ST" in name.upper() or "退" in name: continue
            if code.startswith(("8", "4", "92")): continue

            price   = x.get("f2")
            change  = x.get("f3")
            vol     = x.get("f5")
            amt     = x.get("f6")
            turn    = x.get("f8")
            vr      = x.get("f10")
            high    = x.get("f15")
            low     = x.get("f16")
            open_   = x.get("f17")
            circ_mv = x.get("f21")
            ind     = x.get("f100")

            if not all(isinstance(v, (int, float)) for v in
                       (price, change, vol, amt, turn, vr, circ_mv)):
                continue

            avg_price = amt / (vol * 100) if vol > 0 else 0
            # 今日阴线（收盘<开盘）；数据缺失时为 None，不参与前置过滤
            is_yin = None
            if isinstance(open_, (int, float)) and open_ > 0:
                is_yin = price < open_
            # 收盘位于全天振幅中的位置（0-100），用于系统五“上方70%区域”
            pos_in_range = None
            if all(isinstance(v, (int, float)) for v in (high, low)) and high > low:
                pos_in_range = (price - low) / (high - low) * 100

            stocks.append({
                "code": code, "name": name, "board": board_of(code),
                "price": price,
                "change_pct": change,
                "vol_ratio": vr,
                "turnover": turn,
                "circ_mv_yi": round(circ_mv / 1e8, 2),
                "avg_price": round(avg_price, 3),
                "above_avg": round((price / avg_price - 1) * 100, 2) if avg_price > 0 else 0,
                "high": high, "low": low, "open": open_,
                "is_yin": is_yin,
                "pos_in_range": round(pos_in_range, 2) if pos_in_range is not None else None,
                "industry": str(ind) if ind not in (None, "-") else "",
            })

        total = int(data.get("total") or 0)
        print(f"  第{pn:02d}页：累计 {len(stocks)} 只", end="\r", flush=True)
        if pn * PAGE_SIZE >= total: break
        time.sleep(0.05)
    print()
    if not stocks:
        print("⚠️ 未获取到任何股票数据。")
        if last_err:
            print(f"   最后一次错误：{type(last_err).__name__}: {str(last_err)[:120]}")
        net_hint()
    return stocks

# ============================================================
# K线
# ============================================================
def kline_eastmoney(code, n):
    secid = f"1.{code}" if code.startswith("6") else f"0.{code}"
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get?"
        f"secid={secid}&ut=fa5fd1943c7b386f172d6893dbfba10b"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57"
        f"&klt=101&fqs=1&end=20500101&lmt={n}"
    )
    d = http_json(url, retries=2)
    out = []
    for item in (d.get("data") or {}).get("klines") or []:
        p = item.split(",")
        if len(p) < 7: continue
        try:
            out.append({
                "date": p[0], "open": float(p[1]), "close": float(p[2]),
                "high": float(p[3]), "low": float(p[4]), "vol": float(p[5]) * 100,
            })
        except Exception: pass
    return out

def kline_tencent(code, n):
    sym = ("sh" if code.startswith("6") else "sz") + code
    last = None
    for host in (
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?",
        "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get?",
    ):
        try:
            d = http_json(f"{host}param={sym},day,,,{n},qfq", retries=2)
            node = (d.get("data") or {}).get(sym) or {}
            out = []
            for b in node.get("qfqday") or node.get("day") or []:
                if len(b) >= 6:
                    out.append({
                        "date": b[0], "open": float(b[1]) if len(b) > 1 else 0,
                        "close": float(b[2]), "high": float(b[3]),
                        "low": float(b[4]), "vol": float(b[5]) * 100,
                    })
            if out: return out
        except Exception as e: last = e
    raise last or RuntimeError("腾讯K线为空")

_EA_FAILS = 0
# K线缓存：系统一/二/五 共享，同一只股票只请求一次（减少请求量）
_KLINE_CACHE, _KLINE_LOCK = {}, threading.Lock()

def fetch_kline(code, n=KLINE_N):
    """取K线；带进程内缓存，命中缓存不再发请求、不 sleep。"""
    global _EA_FAILS
    with _KLINE_LOCK:
        hit = _KLINE_CACHE.get(code)
    if hit is not None:
        return hit

    bars = []
    if _EA_FAILS < 5:
        try:
            bars = kline_eastmoney(code, n)
            if bars: _EA_FAILS = 0
        except Exception:
            _EA_FAILS += 1
    if not bars:
        bars = kline_tencent(code, n)

    time.sleep(KLINE_SLEEP)
    with _KLINE_LOCK:
        _KLINE_CACHE[code] = bars
    return bars

# ============================================================
# 洗盘回踩检查（系统一 & 系统三 共用）
# ============================================================
def check_pullback(cand):
    code = cand["code"]
    try:
        bars = fetch_kline(code, KLINE_N)
    except Exception as e:
        return {**cand, "err": f"K线获取失败|{str(e)[:40]}"}
    if len(bars) < MIN_KLINES:
        return {**cand, "err": f"K线不足{MIN_KLINES}根"}

    today, prev = bars[-1], bars[-2]
    if today["close"] >= today["open"]:
        return {**cand, "err": "今日非阴线"}
    if today["vol"] >= prev["vol"]:
        return {**cand, "err": "今日未缩量"}

    if len(bars) < 11:
        return {**cand, "err": "K线不足算10日线"}
    closes = [b["close"] for b in bars]
    ma10_t = sum(closes[-10:]) / 10
    ma10_y = sum(closes[-11:-1]) / 10
    ma10_5ago = sum(closes[-15:-5]) / 10
    if ma10_t <= ma10_y:
        return {**cand, "err": "10日线未向上"}
    dist_ma10 = abs(today["close"] - ma10_t) / ma10_t * 100
    if dist_ma10 > S1_MA10_NEAR:
        return {**cand, "err": f"距10日线{dist_ma10:.1f}%"}

    ma10_slope_pct = (ma10_t - ma10_5ago) / ma10_5ago * 100

    found_zt, zt_date, zt_days_ago = False, "", 0
    for i in range(-min(S1_ZT_DAYS, len(bars) - 1), 0):
        if i - 1 < -len(bars): break
        if is_limit_up(bars[i]["close"], bars[i-1]["close"], code):
            found_zt = True
            zt_date = bars[i]["date"]
            zt_days_ago = -i
            break
    if not found_zt:
        return {**cand, "err": f"近{S1_ZT_DAYS}日无涨停"}

    body = abs(today["open"] - today["close"])
    total_range = today["high"] - today["low"]
    body_ratio = body / total_range * 100 if total_range > 0 else 0

    return {
        **cand,
        "close": round(today["close"], 2),
        "open": round(today["open"], 2),
        "ma10": round(ma10_t, 2),
        "dist_ma10": round(dist_ma10, 2),
        "ma10_slope_pct": round(ma10_slope_pct, 2),
        "vol_today": int(today["vol"]),
        "vol_yesterday": int(prev["vol"]),
        "vol_shrink_pct": round((1 - today["vol"] / prev["vol"]) * 100, 2),
        "zt_date": zt_date,
        "zt_days_ago": zt_days_ago,
        "body_ratio": round(body_ratio, 2),
        "date": today["date"],
    }

# ============================================================
# 放量突破检查（系统二 & 系统四 共用）
# ============================================================
def check_breakout(cand):
    code = cand["code"]
    try:
        bars = fetch_kline(code, KLINE_N)
    except Exception as e:
        return {**cand, "err": f"K线获取失败|{str(e)[:40]}"}
    if len(bars) < MIN_KLINES:
        return {**cand, "err": f"K线不足{MIN_KLINES}根"}

    vols = [b["vol"] for b in bars]
    ma5_v  = sum(vols[-5:]) / 5
    ma10_v = sum(vols[-10:]) / 10
    if ma5_v <= ma10_v:
        return {**cand, "err": "5日均量未超10日均量"}

    if len(bars) < 21:
        return {**cand, "err": "K线不足21根"}
    close = bars[-1]["close"]
    high20 = max(b["high"] for b in bars[-21:-1])
    dist_h20 = (high20 - close) / close * 100
    if dist_h20 > S2_DIST_HIGH20:
        return {**cand, "err": f"距20日高{dist_h20:.1f}%"}

    closes = [b["close"] for b in bars]
    ma10_t = sum(closes[-10:]) / 10
    ma10_y = sum(closes[-11:-1]) / 10
    if ma10_t <= ma10_y:
        return {**cand, "err": "10日线未向上"}

    return {
        **cand,
        "close": round(close, 2),
        "ma5_vol": int(ma5_v),
        "ma10_vol": int(ma10_v),
        "vol_amplify": round((ma5_v / ma10_v - 1) * 100, 2),
        "ma10": round(ma10_t, 2),
        "dist_high20": round(dist_h20, 2),
        "date": bars[-1]["date"],
    }

# ============================================================
# 系统五 · 板块 / 大盘数据（各 1 次请求，全程复用）
# ============================================================
_BLOCK_GAIN, _BLOCK_LUT = {}, {}
_INDEX_GAIN, _INDEX_NAME = 0.0, "上证指数"

def _norm_block(s):
    """板块名归一化：去掉首尾空白与罗马数字后缀（Ⅰ/Ⅱ/Ⅲ），便于个股与板块名匹配。"""
    return re.sub(r"[\sⅠⅡⅢⅣⅤ]+$", "", str(s or "")).strip()

def fetch_block_gain():
    """拉取东财全部行业板块当日涨幅（接口每页最多100条，分页取全，约5次请求，仅一次）。"""
    global _BLOCK_GAIN, _BLOCK_LUT
    total, pn = None, 1
    while pn <= 10:
        qs = (f"pn={pn}&pz=100&po=1&np=1&fltt=2&invt=2&fid=f3"
              "&fs=m:90+t:2&fields=f12,f14,f3")
        d = None
        for h in LIST_HOSTS:      # 主机回退，抗偶发连接重置
            try:
                d = http_json(h + qs, retries=3); break
            except Exception as e:
                print(f"  行业板块第{pn}页 {h.split('/')[2]} 失败：{e}")
        if d is None:
            break
        data = d.get("data") or {}
        rows = data.get("diff") or []
        if pn == 1:
            total = int(data.get("total") or 0)
        for b in rows:
            nm, chg = b.get("f14"), b.get("f3")
            if nm and isinstance(chg, (int, float)):
                _BLOCK_GAIN[str(nm)] = float(chg)
                _BLOCK_LUT[_norm_block(nm)] = float(chg)
        if not rows or (total and pn * 100 >= total):
            break
        pn += 1
    return _BLOCK_GAIN

def fetch_index_gain():
    """拉取大盘（上证指数）当日涨幅（1 次请求，带主机回退）。"""
    global _INDEX_GAIN, _INDEX_NAME
    qs = "fltt=2&invt=2&secids=1.000001&fields=f3,f14"
    d = None
    for h in ("https://push2.eastmoney.com/api/qt/ulist.np/get?",
              "https://push2delay.eastmoney.com/api/qt/ulist.np/get?"):
        try:
            d = http_json(h + qs, retries=3); break
        except Exception as e:
            print(f"  大盘指数 {h.split('/')[2]} 失败：{e}")
    if d:
        for x in (d.get("data") or {}).get("diff") or []:
            if isinstance(x.get("f3"), (int, float)):
                _INDEX_GAIN = float(x["f3"])
                _INDEX_NAME = str(x.get("f14") or "上证指数")
                break
    return _INDEX_GAIN

def block_gain_of(industry):
    """取个股所属行业的当日涨幅；匹配不到返回 None。"""
    if not industry:
        return None
    if industry in _BLOCK_GAIN:
        return _BLOCK_GAIN[industry]
    return _BLOCK_LUT.get(_norm_block(industry))

def _find_trap_zone(bars):
    """简化A：近 S5_TRAP_WIN 日内（不含今日）是否存在「放量滞涨」K线。
       放量滞涨 = 当日量 ≥ 前5日均量 × 1.5，且当日涨幅 < 2%（含下跌）。
       存在即认为上方有密集套牢区，返回该K线日期；否则返回空串。"""
    end = len(bars) - 1
    start = max(5, end - S5_TRAP_WIN)
    for i in range(start, end):
        a5 = sum(bars[j]["vol"] for j in range(i - 5, i)) / 5
        if a5 <= 0:
            continue
        if bars[i]["vol"] >= a5 * S5_TRAP_VOL_MULT:
            pc = bars[i - 1]["close"]
            gain = (bars[i]["close"] - pc) / pc * 100 if pc > 0 else 0
            if gain < S5_TRAP_GAIN_MAX:
                return bars[i]["date"]
    return ""

# ============================================================
# 系统五 · 强势多头突破检查
# ============================================================
def check_s5(cand):
    code = cand["code"]

    # 条件10：所属板块强于大盘（列表数据即可，无需K线，放在最前以省请求）
    ind = cand.get("industry") or ""
    bg = block_gain_of(ind)
    if bg is not None and not (bg > _INDEX_GAIN + S5_BLOCK_DIFF_MIN):
        return {**cand, "err": f"板块弱于大盘|{ind}({bg:+.2f}%)≤{_INDEX_NAME}({_INDEX_GAIN:+.2f}%)"}

    try:
        bars = fetch_kline(code, KLINE_N)
    except Exception as e:
        return {**cand, "err": f"K线获取失败|{str(e)[:40]}"}
    if len(bars) < MIN_KLINES:
        return {**cand, "err": f"K线不足{MIN_KLINES}根"}

    today = bars[-1]
    closes = [b["close"] for b in bars]
    vols = [b["vol"] for b in bars]

    # 条件8：均线多头排列 MA5 > MA10 > MA20
    ma5  = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    if not (ma5 > ma10 > ma20):
        return {**cand, "err": f"均线非多头({ma5:.2f}/{ma10:.2f}/{ma20:.2f})"}

    # 条件5：当日预计成交量 > 前5日均量 × 1.5（盘中按交易进度折算为全日量）
    avg5 = sum(vols[-6:-1]) / 5
    vol_mult_raw = today["vol"] / avg5 if avg5 > 0 else 0
    prog = _intraday_progress()
    vol_mult = vol_mult_raw / prog if prog > 0 else vol_mult_raw
    if vol_mult <= S5_VOL_MULT:
        return {**cand, "err": f"预计量能不足({vol_mult:.2f}×前5日均量)"}

    # 条件6a：距20日高 ≤ 3%
    high20 = max(b["high"] for b in bars[-21:-1])
    dist_h20 = (high20 - today["close"]) / today["close"] * 100
    if dist_h20 > S5_DIST_HIGH20:
        return {**cand, "err": f"距20日高{dist_h20:.1f}%"}

    # 条件6b：上方无明显密集套牢区（简化A：近20日无放量滞涨）
    trap = _find_trap_zone(bars)
    if trap:
        return {**cand, "err": f"上方放量滞涨|{trap}"}

    ma5_ma20_gap = (ma5 / ma20 - 1) * 100 if ma20 > 0 else 0

    return {
        **cand,
        "close": round(today["close"], 2),
        "ma5": round(ma5, 2), "ma10": round(ma10, 2), "ma20": round(ma20, 2),
        "ma5_ma20_gap": round(ma5_ma20_gap, 2),
        "vol_mult": round(vol_mult, 2),
        "vol_mult_raw": round(vol_mult_raw, 2),
        "vol_progress": round(prog, 2),
        "dist_high20": round(dist_h20, 2),
        "block_gain": None if bg is None else round(bg, 2),
        "index_gain": round(_INDEX_GAIN, 2),
        "block_alpha": None if bg is None else round(bg - _INDEX_GAIN, 2),
        "date": today["date"],
    }

# ============================================================
# 评分函数
# ============================================================
def score_pullback(r):
    """洗盘回踩评分（系统三用，总分100）"""
    score = 0
    reasons = []

    d = abs(r.get("dist_ma10", 99))
    s = max(0, 30 - d * 10)
    score += s
    reasons.append(f"距10日线 {d:.2f}%（{s:.1f}分）")

    shr = r.get("vol_shrink_pct", 0)
    if 20 <= shr <= 50:    s = 25
    elif 10 <= shr < 20:   s = 18
    elif 50 < shr <= 70:   s = 18
    else:                  s = 8
    score += s
    reasons.append(f"缩量 {shr:.1f}%（{s}分）")

    days = r.get("zt_days_ago", 99)
    if 3 <= days <= 10:    s = 20
    elif 11 <= days <= 15: s = 14
    elif days in (1, 2):   s = 12
    else:                  s = 6
    score += s
    reasons.append(f"涨停距今 {days} 个交易日（{s}分）")

    slope = r.get("ma10_slope_pct", 0)
    s = min(15, max(0, slope * 2.5))
    score += s
    reasons.append(f"10日线斜率 {slope:+.2f}%（{s:.1f}分）")

    mv = r.get("circ_mv_yi", 0)
    if 100 <= mv <= 200:   s = 10
    elif 200 < mv <= 250:  s = 7
    else:                  s = 4
    score += s
    reasons.append(f"流通市值 {mv:.0f}亿（{s}分）")

    return round(score, 2), reasons

def score_breakout(r):
    """放量突破评分（系统四用，总分100）"""
    score = 0
    reasons = []

    # 1. 距20日高（30分）—— 越贴近前高越好
    d = abs(r.get("dist_high20", 99))
    s = max(0, 30 - d * 8)
    score += s
    reasons.append(f"距20日高 {d:.2f}%（{s:.1f}分）")

    # 2. 放量幅度（25分）—— 5日均量/10日均量 - 1
    amp = r.get("vol_amplify", 0)
    if amp >= 50:   s = 25
    elif amp >= 30: s = 20
    elif amp >= 15: s = 15
    elif amp >= 5:  s = 10
    else:           s = 5
    score += s
    reasons.append(f"放量幅度 +{amp:.1f}%（{s}分）")

    # 3. 量比（15分）—— 越高越活跃
    vr = r.get("vol_ratio", 0)
    if vr >= 2.5:   s = 15
    elif vr >= 2.0: s = 12
    elif vr >= 1.5: s = 9
    else:           s = 5
    score += s
    reasons.append(f"量比 {vr:.2f}（{s}分）")

    # 4. 换手率（15分）—— 6-8% 最健康
    turn = r.get("turnover", 0)
    if 6 <= turn <= 8:    s = 15
    elif 5 <= turn < 6:   s = 11
    elif 8 < turn <= 10:  s = 11
    else:                 s = 6
    score += s
    reasons.append(f"换手率 {turn:.2f}%（{s}分）")

    # 5. 流通市值（15分）—— 50-150亿弹性更好
    mv = r.get("circ_mv_yi", 0)
    if 50 <= mv <= 150:   s = 15
    elif 150 < mv <= 200: s = 10
    else:                 s = 5
    score += s
    reasons.append(f"流通市值 {mv:.0f}亿（{s}分）")

    return round(score, 2), reasons

def score_s5(r):
    """系统五评分（系统五用，总分100）"""
    score, reasons = 0.0, []

    # 1. 涨幅（10分）
    chg = r.get("change_pct", 0)
    s = min(10.0, max(0.0, (chg - S5_CHG_MIN) / (S5_CHG_MAX - S5_CHG_MIN) * 10))
    score += s
    reasons.append(f"涨幅 {chg:+.2f}%（{s:.1f}分）")

    # 2. 量比（15分）
    vr = r.get("vol_ratio", 0)
    if vr >= 2.5:    s = 15
    elif vr >= 2.0:  s = 12
    elif vr >= 1.8:  s = 9
    else:            s = 6
    score += s
    reasons.append(f"量比 {vr:.2f}（{s}分）")

    # 3. 换手率（10分）
    turn = r.get("turnover", 0)
    if 6 <= turn <= 8:                        s = 10
    elif (5 <= turn < 6) or (8 < turn <= 10): s = 7
    else:                                     s = 4
    score += s
    reasons.append(f"换手率 {turn:.2f}%（{s}分）")

    # 4. 流通市值（10分）
    mv = r.get("circ_mv_yi", 0)
    if 50 <= mv <= 150:   s = 10
    elif 150 < mv <= 200: s = 7
    else:                 s = 4
    score += s
    reasons.append(f"流通市值 {mv:.0f}亿（{s}分）")

    # 5. 较前5日均量倍数（15分）
    vm = r.get("vol_mult", 0)
    if vm >= 2.0:    s = 15
    elif vm >= 1.8:  s = 12
    elif vm >= 1.6:  s = 9
    else:            s = 5
    score += s
    reasons.append(f"预计量能 {vm:.2f}×前5日均量（{s}分）")

    # 6. 距20日高（15分）
    d = abs(r.get("dist_high20", 99))
    s = max(0.0, 15 - d * 5)
    score += s
    reasons.append(f"距20日高 {d:.2f}%（{s:.1f}分）")

    # 7. 现价高于成交均价（5分）
    aa = r.get("above_avg", 0)
    s = 5 if aa >= 1 else (3 if aa >= 0.3 else 2)
    score += s
    reasons.append(f"现价高于均价 {aa:+.2f}%（{s}分）")

    # 8. 均线多头强度 MA5 相对 MA20（10分）
    gap = r.get("ma5_ma20_gap", 0)
    if gap >= 5:    s = 10
    elif gap >= 3:  s = 7
    else:           s = 4
    score += s
    reasons.append(f"MA5较MA20 {gap:+.2f}%（{s}分）")

    # 9. 日内位置（5分）
    pos = r.get("pos_in_range")
    if pos is None:    s = 3
    elif pos >= 85:    s = 5
    else:              s = 3
    score += s
    reasons.append(f"日内位置 {pos if pos is not None else '—'}%（{s}分）")

    # 10. 板块跑赢大盘（5分）
    ba = r.get("block_alpha")
    if ba is None:   s = 2
    elif ba >= 3:    s = 5
    elif ba >= 1.5:  s = 3
    else:            s = 2
    score += s
    reasons.append(f"板块跑赢大盘 {'—' if ba is None else f'{ba:+.2f}'}%（{s}分）")

    return round(score, 2), reasons

def pick_best(records, score_func):
    if not records: return None
    scored = []
    for r in records:
        sc, reasons = score_func(r)
        scored.append({**r, "best_score": sc, "best_reasons": reasons})
    scored.sort(key=lambda x: x["best_score"], reverse=True)
    return scored[0]

# ============================================================
# 并行检查
# ============================================================
def run_checks(pool, check_func, sys_name):
    results, err_counter, err_samples = [], Counter(), {}
    total = len(pool)
    print(f"  检查 {total} 只……")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(check_func, c): c for c in pool}
        for i, fut in enumerate(as_completed(futs), 1):
            cand = futs[fut]
            try: r = fut.result()
            except Exception as e: r = {**cand, "err": f"异常|{str(e)[:40]}"}
            if "err" not in r:
                results.append(r)
            else:
                key = r["err"].split("|")[0][:22]
                err_counter[key] += 1
                err_samples.setdefault(key, f"{cand['code']} {cand['name']} -> {r['err']}")
            if i % 30 == 0 or i == total:
                print(f"    {i}/{total} - 通过 {len(results)}", end="\r")
    print(f"\n  {sys_name} 通过: {len(results)} 只")
    if err_counter:
        print(f"  {sys_name} 过滤原因 Top 5：")
        for k, v in err_counter.most_common(5):
            print(f"    [{v:>3}只] {k}  例：{err_samples[k]}")
    return results

# ============================================================
# HTML
# ============================================================
def esc(v): return _html.escape("" if v is None else str(v))

def fmt(v, dec=2, suffix=""):
    if v is None or v == "": return "—"
    try: return f"{float(v):,.{dec}f}{suffix}"
    except Exception: return esc(v)

def fmt_pct(v, dec=2):
    if v is None or v == "": return "—"
    try: return f"{float(v):+.{dec}f}%"
    except Exception: return esc(v)

def render_hero_pick(best, theme, title):
    """渲染最优推荐大卡片。theme: 'gold' 或 'red'"""
    if not best:
        return f'<div class="empty">{title}：今日无符合条件的股票，暂无最优推荐</div>'
    reason_items = "".join(f'<li>{esc(x)}</li>' for x in best.get("best_reasons", []))

    if theme == "gold":
        metrics = f"""
            <div class="metric"><div class="m-k">收盘</div><div class="m-v">{fmt(best.get('close'))}</div></div>
            <div class="metric"><div class="m-k">10日线</div><div class="m-v">{fmt(best.get('ma10'))}</div></div>
            <div class="metric"><div class="m-k">距10日线</div><div class="m-v hi-blue">{fmt_pct(best.get('dist_ma10'))}</div></div>
            <div class="metric"><div class="m-k">10日线斜率</div><div class="m-v hi-green">{fmt_pct(best.get('ma10_slope_pct'))}</div></div>
            <div class="metric"><div class="m-k">缩量幅度</div><div class="m-v hi-green">-{fmt(best.get('vol_shrink_pct'))}%</div></div>
            <div class="metric"><div class="m-k">涨停距今</div><div class="m-v hi-gold">{esc(best.get('zt_days_ago'))} 天</div></div>
            <div class="metric"><div class="m-k">流通市值</div><div class="m-v">{fmt(best.get('circ_mv_yi'), 0, ' 亿')}</div></div>
            <div class="metric"><div class="m-k">涨停日</div><div class="m-v hi-gold">{esc(best.get('zt_date'))}</div></div>
        """
    else:  # red
        metrics = f"""
            <div class="metric"><div class="m-k">现价</div><div class="m-v">{fmt(best.get('price'))}</div></div>
            <div class="metric"><div class="m-k">涨幅</div><div class="m-v hi-red">{fmt_pct(best.get('change_pct'))}</div></div>
            <div class="metric"><div class="m-k">量比</div><div class="m-v">{fmt(best.get('vol_ratio'))}</div></div>
            <div class="metric"><div class="m-k">换手率</div><div class="m-v">{fmt(best.get('turnover'), 2, '%')}</div></div>
            <div class="metric"><div class="m-k">流通市值</div><div class="m-v">{fmt(best.get('circ_mv_yi'), 0, ' 亿')}</div></div>
            <div class="metric"><div class="m-k">溢价均线</div><div class="m-v hi-green">{fmt_pct(best.get('above_avg'))}</div></div>
            <div class="metric"><div class="m-k">放量幅度</div><div class="m-v hi-green">+{fmt(best.get('vol_amplify'))}%</div></div>
            <div class="metric"><div class="m-k">距20日高</div><div class="m-v hi-blue">{fmt_pct(best.get('dist_high20'))}</div></div>
        """

    return f"""
    <div class="hero-pick hero-pick-{theme}">
        <div class="hero-pick-badge">{title}</div>
        <div class="hero-pick-body">
            <div class="pick-code">{esc(best['code'])}</div>
            <div class="pick-name">{esc(best['name'])}</div>
            <div class="pick-board">{esc(best['board'])}</div>
            <div class="pick-score">
                <span class="score-num">{fmt(best.get('best_score'), 1)}</span>
                <span class="score-unit">分</span>
            </div>
        </div>
        <div class="pick-metrics">{metrics}</div>
        <div class="pick-reasons">
            <div class="reason-title">评分明细（总分 100）</div>
            <ul class="reason-list">{reason_items}</ul>
        </div>
    </div>
    """

def write_html(s1_results, s2_results, s5_results, best_s1, best_s2, out_path, scan_cnt):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    best3_block = render_hero_pick(best_s1, "gold", "系统三 · 洗盘回踩最优一只")
    best4_block = render_hero_pick(best_s2, "red",  "系统四 · 放量突破最优一只")

    def s1_card(r):
        return f"""
        <div class="card card-s1">
            <div class="card-head">
                <div class="code">{esc(r['code'])}</div>
                <div class="name">{esc(r['name'])}</div>
                <div class="badge">{esc(r['board'])}</div>
            </div>
            <div class="kv-grid">
                <div><span class="k">收盘</span><span class="v">{fmt(r.get('close'))}</span></div>
                <div><span class="k">10日线</span><span class="v">{fmt(r.get('ma10'))}</span></div>
                <div><span class="k">距10日线</span><span class="v hi-blue">{fmt_pct(r.get('dist_ma10'))}</span></div>
                <div><span class="k">流通市值</span><span class="v">{fmt(r.get('circ_mv_yi'), 0, ' 亿')}</span></div>
                <div><span class="k">缩量幅度</span><span class="v hi-green">-{fmt(r.get('vol_shrink_pct'))}%</span></div>
                <div><span class="k">涨停日</span><span class="v hi-gold">{esc(r.get('zt_date'))}</span></div>
            </div>
        </div>
        """

    def s2_card(r):
        return f"""
        <div class="card card-s2">
            <div class="card-head">
                <div class="code">{esc(r['code'])}</div>
                <div class="name">{esc(r['name'])}</div>
                <div class="badge">{esc(r['board'])}</div>
            </div>
            <div class="kv-grid">
                <div><span class="k">涨幅</span><span class="v hi-red">{fmt_pct(r.get('change_pct'))}</span></div>
                <div><span class="k">量比</span><span class="v">{fmt(r.get('vol_ratio'))}</span></div>
                <div><span class="k">换手率</span><span class="v">{fmt(r.get('turnover'), 2, '%')}</span></div>
                <div><span class="k">流通市值</span><span class="v">{fmt(r.get('circ_mv_yi'), 0, ' 亿')}</span></div>
                <div><span class="k">溢价均线</span><span class="v hi-green">{fmt_pct(r.get('above_avg'))}</span></div>
                <div><span class="k">放量幅度</span><span class="v hi-green">+{fmt(r.get('vol_amplify'))}%</span></div>
                <div><span class="k">距20日高</span><span class="v hi-blue">{fmt_pct(r.get('dist_high20'))}</span></div>
                <div><span class="k">收盘</span><span class="v">{fmt(r.get('close'))}</span></div>
            </div>
        </div>
        """

    s1_cards = "".join(s1_card(r) for r in s1_results) or \
        '<div class="empty">系统一：暂无符合条件的股票</div>'
    s2_cards = "".join(s2_card(r) for r in s2_results) or \
        '<div class="empty">系统二：暂无符合条件的股票</div>'

    def s5_card(r):
        return f"""
        <div class="card card-s5">
            <div class="card-head">
                <div class="code">{esc(r['code'])}</div>
                <div class="name">{esc(r['name'])}</div>
                <div class="badge">{esc(r['board'])}</div>
                <div class="s5-score">{fmt(r.get('s5_score'), 1)}<span>分</span></div>
            </div>
            <div class="kv-grid">
                <div><span class="k">涨幅</span><span class="v hi-red">{fmt_pct(r.get('change_pct'))}</span></div>
                <div><span class="k">量比</span><span class="v">{fmt(r.get('vol_ratio'))}</span></div>
                <div><span class="k">换手率</span><span class="v">{fmt(r.get('turnover'), 2, '%')}</span></div>
                <div><span class="k">流通市值</span><span class="v">{fmt(r.get('circ_mv_yi'), 0, ' 亿')}</span></div>
                <div><span class="k">预计量能</span><span class="v hi-green">{fmt(r.get('vol_mult'))}× 前5日</span></div>
                <div><span class="k">距20日高</span><span class="v hi-blue">{fmt_pct(r.get('dist_high20'))}</span></div>
                <div><span class="k">均线MA5/10/20</span><span class="v hi-green">{fmt(r.get('ma5'))}/{fmt(r.get('ma10'))}/{fmt(r.get('ma20'))}</span></div>
                <div><span class="k">日内位置</span><span class="v hi-blue">{fmt(r.get('pos_in_range'), 0, '%')}</span></div>
                <div><span class="k">所属板块</span><span class="v hi-gold">{esc(r.get('industry') or '—')}</span></div>
                <div><span class="k">板块跑赢大盘</span><span class="v hi-green">{fmt_pct(r.get('block_alpha'))}</span></div>
            </div>
        </div>
        """

    s5_cards = "".join(s5_card(r) for r in s5_results) or \
        '<div class="empty">系统五：暂无符合条件的股票</div>'

    s1_cols = ["代码", "名称", "板块", "收盘", "10日线", "距10日线",
               "10日线斜率", "流通市值", "今日量", "昨日量", "缩量幅度", "涨停日"]
    s1_rows = ""
    for r in s1_results:
        s1_rows += f"""<tr>
            <td class="mono">{esc(r['code'])}</td>
            <td>{esc(r['name'])}</td>
            <td>{esc(r['board'])}</td>
            <td class="mono">{fmt(r.get('close'))}</td>
            <td class="mono">{fmt(r.get('ma10'))}</td>
            <td class="mono hi-blue">{fmt_pct(r.get('dist_ma10'))}</td>
            <td class="mono hi-green">{fmt_pct(r.get('ma10_slope_pct'))}</td>
            <td class="mono">{fmt(r.get('circ_mv_yi'), 0)}</td>
            <td class="mono">{esc(f"{r.get('vol_today', 0):,}")}</td>
            <td class="mono">{esc(f"{r.get('vol_yesterday', 0):,}")}</td>
            <td class="mono hi-green">{fmt(r.get('vol_shrink_pct'))}%</td>
            <td class="mono hi-gold">{esc(r.get('zt_date'))}</td>
        </tr>"""

    s2_cols = ["代码", "名称", "板块", "现价", "涨幅", "量比", "换手",
               "流通市值", "成交均价", "溢价均线", "放量幅度", "距20日高", "收盘"]
    s2_rows = ""
    for r in s2_results:
        s2_rows += f"""<tr>
            <td class="mono">{esc(r['code'])}</td>
            <td>{esc(r['name'])}</td>
            <td>{esc(r['board'])}</td>
            <td class="mono">{fmt(r.get('price'))}</td>
            <td class="mono hi-red">{fmt_pct(r.get('change_pct'))}</td>
            <td class="mono">{fmt(r.get('vol_ratio'))}</td>
            <td class="mono">{fmt(r.get('turnover'), 2, '%')}</td>
            <td class="mono">{fmt(r.get('circ_mv_yi'), 0)}</td>
            <td class="mono">{fmt(r.get('avg_price'), 3)}</td>
            <td class="mono hi-green">{fmt_pct(r.get('above_avg'))}</td>
            <td class="mono hi-green">+{fmt(r.get('vol_amplify'))}%</td>
            <td class="mono hi-blue">{fmt_pct(r.get('dist_high20'))}</td>
            <td class="mono">{fmt(r.get('close'))}</td>
        </tr>"""

    s5_cols = ["代码", "名称", "板块", "所属行业", "涨幅", "量比", "换手", "流通市值",
               "预计量能倍数", "距20日高", "MA5", "MA10", "MA20", "日内位置",
               "板块涨幅", "大盘涨幅", "跑赢大盘", "评分"]
    s5_rows = ""
    for r in s5_results:
        s5_rows += f"""<tr>
            <td class="mono">{esc(r['code'])}</td>
            <td>{esc(r['name'])}</td>
            <td>{esc(r['board'])}</td>
            <td class="hi-gold">{esc(r.get('industry') or '—')}</td>
            <td class="mono hi-red">{fmt_pct(r.get('change_pct'))}</td>
            <td class="mono">{fmt(r.get('vol_ratio'))}</td>
            <td class="mono">{fmt(r.get('turnover'), 2, '%')}</td>
            <td class="mono">{fmt(r.get('circ_mv_yi'), 0)}</td>
            <td class="mono hi-green">{fmt(r.get('vol_mult'))}</td>
            <td class="mono hi-blue">{fmt_pct(r.get('dist_high20'))}</td>
            <td class="mono">{fmt(r.get('ma5'))}</td>
            <td class="mono">{fmt(r.get('ma10'))}</td>
            <td class="mono">{fmt(r.get('ma20'))}</td>
            <td class="mono hi-blue">{fmt(r.get('pos_in_range'), 0)}</td>
            <td class="mono hi-gold">{fmt_pct(r.get('block_gain'))}</td>
            <td class="mono">{fmt_pct(r.get('index_gain'))}</td>
            <td class="mono hi-green">{fmt_pct(r.get('block_alpha'))}</td>
            <td class="mono hi-gold">{fmt(r.get('s5_score'), 1)}</td>
        </tr>"""

    doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>杨总精选 · {ts}</title>
<style>
* {{ box-sizing:border-box; }}
body {{
    margin:0;
    background:radial-gradient(circle at 12% -10%, #1b3352 0%, #0a1729 45%, #050c15 100%);
    color:#e8eef7;
    font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",Arial,sans-serif;
    min-height:100vh;
    padding:28px 24px 60px;
}}
.wrap {{ max-width:1560px; margin:0 auto; }}

.hero {{
    position:relative;
    background:linear-gradient(135deg,#15294a 0%,#0c1c33 60%,#0a1526 100%);
    border:1px solid #2b4770;
    border-radius:22px;
    padding:34px 38px 30px;
    box-shadow:0 24px 70px rgba(0,0,0,.5), inset 0 1px 0 rgba(255,255,255,.05);
    overflow:hidden;
}}
.hero::before {{
    content:"";
    position:absolute; inset:0;
    background:radial-gradient(circle at 88% 18%, rgba(245,196,81,.18), transparent 45%),
               radial-gradient(circle at 8% 90%, rgba(103,225,255,.12), transparent 40%);
    pointer-events:none;
}}
.hero-title {{
    position:relative;
    font-size:44px; font-weight:900; letter-spacing:6px;
    background:linear-gradient(120deg,#f7d87e 0%,#f5c451 40%,#ffe9a8 60%,#e6b23a 100%);
    -webkit-background-clip:text; background-clip:text; color:transparent;
    text-shadow:0 0 40px rgba(245,196,81,.18);
    margin:0;
}}
.hero-sub {{
    position:relative; margin-top:12px; color:#9fb2c8;
    font-size:13px; letter-spacing:1px;
}}
.hero-sub .sep {{ color:#3a5a80; margin:0 8px; }}
.hero-date {{ position:relative; margin-top:6px; color:#677d99; font-size:12px; }}

.stat-bar {{
    display:grid; grid-template-columns:repeat(5,1fr); gap:14px; margin-top:20px;
}}
.stat {{
    background:linear-gradient(150deg,#13243e,#0d1b2e);
    border:1px solid #25405f; border-radius:14px; padding:16px 18px;
}}
.stat-k {{ color:#8ea1b9; font-size:11.5px; letter-spacing:.5px; }}
.stat-v {{ margin-top:6px; font-size:26px; font-weight:900; color:#f2f6fc; font-feature-settings:"tnum"; }}
.stat-v.gold {{ color:#f5c451; }}
.stat-v.blue {{ color:#67e1ff; }}
.stat-v.green{{ color:#5fe3a1; }}
.stat-v.red  {{ color:#ff7b7b; }}
.stat-v.purple{{ color:#c58cff; }}

/* 系统三 / 系统四 主视觉 */
.hero-pick {{
    margin-top:22px;
    border-radius:20px;
    padding:28px 34px;
    position:relative;
    overflow:hidden;
    box-shadow:0 0 60px rgba(0,0,0,.4), inset 0 1px 0 rgba(255,255,255,.06);
}}
.hero-pick-gold {{
    background:linear-gradient(140deg,#2a1e08 0%,#3d2b0c 30%,#1a1408 100%);
    border:2px solid #f5c451;
    box-shadow:0 0 60px rgba(245,196,81,.18), inset 0 1px 0 rgba(255,255,255,.06);
}}
.hero-pick-gold::before {{
    content:""; position:absolute; top:-50%; right:-20%;
    width:70%; height:200%;
    background:radial-gradient(circle, rgba(245,196,81,.15), transparent 60%);
    pointer-events:none;
}}
.hero-pick-red {{
    background:linear-gradient(140deg,#2e0d0d 0%,#431111 30%,#1a0808 100%);
    border:2px solid #ff7b7b;
    box-shadow:0 0 60px rgba(255,123,123,.18), inset 0 1px 0 rgba(255,255,255,.06);
}}
.hero-pick-red::before {{
    content:""; position:absolute; top:-50%; right:-20%;
    width:70%; height:200%;
    background:radial-gradient(circle, rgba(255,123,123,.15), transparent 60%);
    pointer-events:none;
}}
.hero-pick-badge {{
    display:inline-block;
    font-size:11px; font-weight:900; letter-spacing:2px;
    padding:4px 14px; border-radius:999px;
    margin-bottom:14px;
}}
.hero-pick-gold .hero-pick-badge {{
    background:linear-gradient(135deg,#f5c451,#e6a82a);
    color:#1a1408;
}}
.hero-pick-red .hero-pick-badge {{
    background:linear-gradient(135deg,#ff8b8b,#e64545);
    color:#1a0808;
}}
.hero-pick-body {{
    display:flex; align-items:baseline; gap:18px; flex-wrap:wrap;
    padding-bottom:18px;
    border-bottom:1px dashed rgba(255,255,255,.15);
}}
.pick-code {{
    font-family:"SF Mono",Menlo,Consolas,monospace;
    font-size:38px; font-weight:900; color:#fff; letter-spacing:1px;
}}
.hero-pick-gold .pick-name {{ font-size:22px; color:#f5d78e; font-weight:700; }}
.hero-pick-red  .pick-name {{ font-size:22px; color:#ffb0b0; font-weight:700; }}
.pick-board {{
    font-size:11px; padding:3px 10px; border-radius:999px;
    letter-spacing:.5px;
}}
.hero-pick-gold .pick-board {{
    background:rgba(245,196,81,.15); color:#f5c451;
    border:1px solid rgba(245,196,81,.35);
}}
.hero-pick-red .pick-board {{
    background:rgba(255,123,123,.15); color:#ff7b7b;
    border:1px solid rgba(255,123,123,.35);
}}
.pick-score {{ margin-left:auto; display:flex; align-items:baseline; gap:4px; }}
.hero-pick-gold .score-num {{
    font-size:44px; font-weight:900; color:#f5c451;
    font-feature-settings:"tnum";
    text-shadow:0 0 30px rgba(245,196,81,.5);
}}
.hero-pick-red .score-num {{
    font-size:44px; font-weight:900; color:#ff7b7b;
    font-feature-settings:"tnum";
    text-shadow:0 0 30px rgba(255,123,123,.5);
}}
.score-unit {{ font-size:14px; color:#888; font-weight:700; }}

.pick-metrics {{
    display:grid; grid-template-columns:repeat(4,1fr); gap:12px;
    margin-top:18px;
}}
.metric {{
    background:rgba(0,0,0,.25);
    border:1px solid rgba(255,255,255,.08);
    border-radius:12px;
    padding:12px 14px;
}}
.hero-pick-gold .metric {{ border-color:rgba(245,196,81,.18); }}
.hero-pick-red  .metric {{ border-color:rgba(255,123,123,.18); }}
.m-k {{ color:#a88a4a; font-size:11px; letter-spacing:.5px; }}
.hero-pick-red .m-k {{ color:#b86060; }}
.m-v {{
    margin-top:6px; font-size:19px; font-weight:900; color:#fff;
    font-feature-settings:"tnum";
}}

.pick-reasons {{
    margin-top:20px;
    background:rgba(0,0,0,.28);
    border-radius:12px;
    padding:16px 20px;
}}
.reason-title {{
    font-size:12px; font-weight:700;
    letter-spacing:1px; margin-bottom:10px;
}}
.hero-pick-gold .reason-title {{ color:#f5c451; }}
.hero-pick-red  .reason-title {{ color:#ff7b7b; }}
.reason-list {{
    list-style:none; padding:0; margin:0;
    display:grid; grid-template-columns:1fr 1fr; gap:6px 20px;
    font-size:12.5px;
}}
.reason-list li {{
    color:#cfdae8; padding-left:16px; position:relative;
    line-height:1.6;
}}
.reason-list li::before {{
    content:"◆"; position:absolute; left:0; top:0;
    font-size:9px;
}}
.hero-pick-gold .reason-list li::before {{ color:#f5c451; }}
.hero-pick-red  .reason-list li::before {{ color:#ff7b7b; }}

/* 系统区块 */
.section {{ margin-top:26px; }}
.section-head {{
    display:flex; align-items:baseline; gap:12px;
    padding:0 4px 12px;
    border-bottom:1px solid #1e3350;
    margin-bottom:16px;
}}
.section-title {{ font-size:20px; font-weight:900; letter-spacing:1px; color:#e8eef7; }}
.section-title.s1 {{ color:#67e1ff; }}
.section-title.s2 {{ color:#f5c451; }}
.section-note {{ color:#7f95ae; font-size:12px; letter-spacing:.3px; }}
.section-count {{ margin-left:auto; color:#9fb2c8; font-size:12px; }}
.section-count b {{ color:#f5c451; font-size:15px; margin:0 3px; }}

.grid {{
    display:grid;
    grid-template-columns:repeat(auto-fill,minmax(330px,1fr));
    gap:14px;
}}
.card {{
    background:linear-gradient(160deg,#12233c,#0c1929);
    border-radius:16px; padding:16px 18px;
    position:relative; overflow:hidden;
    transition:transform .18s ease, box-shadow .18s ease;
}}
.card:hover {{ transform:translateY(-3px); box-shadow:0 14px 34px rgba(0,0,0,.4); }}
.card-s1 {{ border:1px solid rgba(103,225,255,.28); }}
.card-s1::before {{
    content:""; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:linear-gradient(180deg,#67e1ff,#2b8fb8);
}}
.card-s2 {{ border:1px solid rgba(245,196,81,.30); }}
.card-s2::before {{
    content:""; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:linear-gradient(180deg,#f5c451,#c89a2c);
}}
.card-s5 {{ border:1px solid rgba(197,140,255,.32); }}
.card-s5::before {{
    content:""; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:linear-gradient(180deg,#c58cff,#7d4fd1);
}}
.card-s5 .badge {{ background:rgba(197,140,255,.1); color:#c58cff;
    border-color:rgba(197,140,255,.25); }}
.s5-score {{
    margin-left:auto; color:#c58cff; font-weight:900; font-size:18px;
    font-feature-settings:"tnum";
}}
.s5-score span {{ font-size:10px; color:#8b7aa8; margin-left:2px; }}
.card-head {{ display:flex; align-items:baseline; gap:10px; margin-bottom:12px; }}
.code {{
    font-family:"SF Mono",Menlo,Consolas,monospace;
    font-size:18px; font-weight:800; color:#fff; letter-spacing:.5px;
}}
.name {{ font-size:14px; color:#b7c6d8; flex:1; }}
.badge {{
    font-size:10px; padding:2px 8px; border-radius:999px;
    background:rgba(145,162,184,.13); color:#9fb2c8;
    border:1px solid rgba(145,162,184,.2); letter-spacing:.5px;
}}
.card-s1 .badge {{ background:rgba(103,225,255,.1); color:#67e1ff;
    border-color:rgba(103,225,255,.25); }}
.card-s2 .badge {{ background:rgba(245,196,81,.1); color:#f5c451;
    border-color:rgba(245,196,81,.25); }}

.kv-grid {{
    display:grid; grid-template-columns:1fr 1fr; gap:8px 16px; font-size:12.5px;
}}
.kv-grid > div {{
    display:flex; justify-content:space-between; align-items:baseline;
    padding:4px 0; border-bottom:1px dashed rgba(255,255,255,.04);
}}
.k {{ color:#7f95ae; }}
.v {{ color:#e8eef7; font-weight:700; font-feature-settings:"tnum"; }}
.hi-gold {{ color:#f5c451; }}
.hi-blue {{ color:#67e1ff; }}
.hi-green {{ color:#5fe3a1; }}
.hi-red {{ color:#ff7b7b; }}

.empty {{
    padding:28px; text-align:center; color:#7f95ae;
    background:rgba(13,27,45,.5); border:1px dashed #2b3f5c;
    border-radius:14px; font-size:13px;
    margin-top:22px;
}}

.table-wrap {{
    margin-top:20px;
    background:rgba(13,27,45,.7);
    border:1px solid #1e3350;
    border-radius:14px;
    overflow:auto; max-height:60vh;
}}
table {{ width:100%; border-collapse:collapse; font-size:12px; min-width:1200px; }}
th {{
    position:sticky; top:0; z-index:2; background:#152945;
    color:#9fb2c8; font-weight:600; padding:10px 8px;
    text-align:center; border-bottom:1px solid #2b4770;
    white-space:nowrap; letter-spacing:.3px; font-size:11.5px;
}}
td {{
    padding:8px; text-align:center;
    border-bottom:1px solid #16273e;
    color:#cfdae8; white-space:nowrap;
}}
tbody tr:hover td {{ background:rgba(103,225,255,.05); }}
td.mono {{ font-family:"SF Mono",Menlo,Consolas,monospace; font-feature-settings:"tnum"; }}
.section.s1 th {{ background:#12324a; border-bottom-color:#1e4c6e; }}
.section.s1 tbody tr:hover td {{ background:rgba(103,225,255,.06); }}
.section.s2 th {{ background:#3a2d10; border-bottom-color:#5c4820; color:#f5d78e; }}
.section.s2 tbody tr:hover td {{ background:rgba(245,196,81,.06); }}
.section.s5 th {{ background:#2e1c45; border-bottom-color:#4a2f70; color:#d7b8ff; }}
.section.s5 tbody tr:hover td {{ background:rgba(197,140,255,.06); }}

.footer {{
    margin-top:40px; color:#4f6580; font-size:11px;
    text-align:center; line-height:1.9; letter-spacing:.3px;
}}
.footer b {{ color:#7f95ae; }}

@media(max-width:900px) {{
    .hero-title {{ font-size:32px; letter-spacing:3px; }}
    .stat-bar {{ grid-template-columns:1fr 1fr; }}
    .pick-metrics {{ grid-template-columns:1fr 1fr; }}
    .reason-list {{ grid-template-columns:1fr; }}
    .pick-code {{ font-size:28px; }}
    .score-num {{ font-size:32px; }}
}}
</style>
</head>
<body>
<div class="wrap">

    <div class="hero">
        <h1 class="hero-title">杨总精选</h1>
        <div class="hero-sub">
            系统三 · 洗盘最优
            <span class="sep">|</span>
            系统四 · 突破最优
            <span class="sep">|</span>
            系统一 · 洗盘回踩
            <span class="sep">|</span>
            系统二 · 放量突破
            <span class="sep">|</span>
            系统五 · 强势突破
            <span class="sep">|</span>
            全市场 {scan_cnt:,} 只
        </div>
        <div class="hero-date">数据时间：{ts}</div>

        <div class="stat-bar">
            <div class="stat">
                <div class="stat-k">系统一 · 洗盘回踩通过</div>
                <div class="stat-v blue">{len(s1_results)}</div>
            </div>
            <div class="stat">
                <div class="stat-k">系统二 · 放量突破通过</div>
                <div class="stat-v gold">{len(s2_results)}</div>
            </div>
            <div class="stat">
                <div class="stat-k">系统三 · 最优评分</div>
                <div class="stat-v green">{fmt(best_s1.get('best_score'), 1) if best_s1 else '—'}</div>
            </div>
            <div class="stat">
                <div class="stat-k">系统四 · 最优评分</div>
                <div class="stat-v red">{fmt(best_s2.get('best_score'), 1) if best_s2 else '—'}</div>
            </div>
            <div class="stat">
                <div class="stat-k">系统五 · 强势突破通过</div>
                <div class="stat-v purple">{len(s5_results)}</div>
            </div>
        </div>
    </div>

    <!-- 系统三 · 洗盘回踩最优一只 -->
    {best3_block}

    <!-- 系统四 · 放量突破最优一只 -->
    {best4_block}

    <!-- 系统一 · 洗盘回踩全部 -->
    <div class="section s1">
        <div class="section-head">
            <div class="section-title s1">系统一 · 洗盘回踩（全部）</div>
            <div class="section-note">非ST｜非北交所｜流通100-300亿｜近20日涨停｜今日阴线+缩量｜靠近10日线｜10日线向上</div>
            <div class="section-count">通过 <b>{len(s1_results)}</b> 只</div>
        </div>
        <div class="grid">{s1_cards}</div>

        <div class="table-wrap">
            <table>
                <thead><tr>{''.join(f'<th>{c}</th>' for c in s1_cols)}</tr></thead>
                <tbody>{s1_rows if s1_rows else '<tr><td colspan="12">暂无数据</td></tr>'}</tbody>
            </table>
        </div>
    </div>

    <!-- 系统二 · 放量突破全部 -->
    <div class="section s2">
        <div class="section-head">
            <div class="section-title s2">系统二 · 放量突破（全部）</div>
            <div class="section-note">涨幅3-5%｜量比&gt;1｜换手5-10%｜流通50-200亿｜5日均量&gt;10日均量｜距20日高≤3%且10日线向上｜分时价&gt;均价</div>
            <div class="section-count">通过 <b>{len(s2_results)}</b> 只</div>
        </div>
        <div class="grid">{s2_cards}</div>

        <div class="table-wrap">
            <table>
                <thead><tr>{''.join(f'<th>{c}</th>' for c in s2_cols)}</tr></thead>
                <tbody>{s2_rows if s2_rows else '<tr><td colspan="13">暂无数据</td></tr>'}</tbody>
            </table>
        </div>
    </div>

    <!-- 系统五 · 强势多头突破全部 -->
    <div class="section s5">
        <div class="section-head">
            <div class="section-title s5">系统五 · 强势多头突破（全部）</div>
            <div class="section-note">涨幅3-5%｜量比&gt;1.5｜换手5-10%｜流通50-200亿｜当日量&gt;前5日均量1.5倍｜距20日高≤3%且无放量滞涨｜分时价&gt;均价｜MA5&gt;MA10&gt;MA20｜收盘位于振幅上方70%｜板块强于大盘</div>
            <div class="section-count">通过 <b>{len(s5_results)}</b> 只</div>
        </div>
        <div class="grid">{s5_cards}</div>

        <div class="table-wrap">
            <table>
                <thead><tr>{''.join(f'<th>{c}</th>' for c in s5_cols)}</tr></thead>
                <tbody>{s5_rows if s5_rows else '<tr><td colspan="18">暂无数据</td></tr>'}</tbody>
            </table>
        </div>
    </div>

    <div class="footer">
        <b>杨总精选</b> · 系统三与系统一条件相同、系统四与系统二条件相同，各自只保留综合评分最高的一只<br>
        系统五为多头突破加强版（按评分排序）· 量化选股仅供参考，请结合实盘风险独立决策 · 数据来源：东方财富公开行情接口 · 生成时间 {ts}
    </div>
</div>
</body>
</html>"""
    Path(out_path).write_text(doc, encoding="utf-8")
    return Path(out_path)

# ============================================================
# 主程序
# ============================================================
def main():
    print("=" * 72)
    print("杨总精选 · A股五系统筛选器")
    print("=" * 72)
    print("系统一 · 洗盘回踩（全部）")
    print("         非ST｜非北交所｜流通100-300亿｜近20日涨停｜今日阴线+缩量")
    print("         ｜靠近10日线(±3%)｜10日线向上")
    print("系统二 · 放量突破（全部）")
    print("         涨幅3-5%｜量比>1｜换手5-10%｜流通50-200亿｜5日均量>10日均量")
    print("         ｜距20日高≤3%且10日线向上｜分时价>均价")
    print("系统三 · 洗盘回踩最优一只（条件同系统一，只保留评分最高的一只）")
    print("系统四 · 放量突破最优一只（条件同系统二，只保留评分最高的一只）")
    print("系统五 · 强势多头突破（全部）")
    print("         涨幅3-5%｜量比>1.5｜换手5-10%｜流通50-200亿｜当日量>前5日均量1.5倍")
    print("         ｜距20日高≤3%且上方无放量滞涨｜分时价>均价｜MA5>MA10>MA20")
    print("         ｜收盘位于全天振幅上方70%｜所属板块强于大盘")
    print("=" * 72)

    print("\n[1/7] 扫描全市场……")
    print(f"  连接方式：{'系统代理' if USE_SYSTEM_PROXY else '直连（忽略系统代理）'}")
    all_stocks = build_all_stocks()
    if not all_stocks:
        print("未获取到任何股票，请检查网络（详见上方网络诊断）")
        return
    print(f"全市场（非ST、非北交所）: {len(all_stocks)} 只")

    # ---- 系统一 ----
    print("\n[2/7] 系统一 · 洗盘回踩（全部）")
    # 前置过滤：流通市值 + 今日阴线（收盘<开盘，列表即可判定），先砍掉一半再拉K线，省请求
    s1_candidates = [
        s for s in all_stocks
        if S1_MV_MIN <= s["circ_mv_yi"] <= S1_MV_MAX
        and s["is_yin"] != False  # noqa: E712 —— 数据缺失(None)时不误杀
    ]
    print(f"  初筛（流通市值 {S1_MV_MIN}-{S1_MV_MAX}亿 + 今日阴线）: {len(s1_candidates)} 只")
    s1_results = run_checks(s1_candidates, check_pullback, "系统一") if s1_candidates else []

    # ---- 系统二 ----
    print("\n[3/7] 系统二 · 放量突破（全部）")
    s2_candidates = [
        s for s in all_stocks
        if S2_CHG_MIN <= s["change_pct"] <= S2_CHG_MAX
        and s["vol_ratio"] > S2_VOL_RATIO_MIN
        and S2_TURN_MIN <= s["turnover"] <= S2_TURN_MAX
        and S2_MV_MIN <= s["circ_mv_yi"] <= S2_MV_MAX
        and s["above_avg"] > 0
    ]
    print(f"  初筛（涨幅/量比/换手/市值/分时价>均价）: {len(s2_candidates)} 只")
    s2_results = run_checks(s2_candidates, check_breakout, "系统二") if s2_candidates else []

    # ---- 系统三 ----
    print("\n[4/7] 系统三 · 洗盘回踩最优一只")
    print(f"  条件同系统一，候选 {len(s1_results)} 只 → 打分挑最优")
    best_s1 = pick_best(s1_results, score_pullback)
    if best_s1:
        print(f"\n  🏆 系统三 最优: {best_s1['code']} {best_s1['name']}  评分 {best_s1['best_score']:.1f}")
        for r in best_s1.get("best_reasons", []):
            print(f"     · {r}")
    else:
        print("  系统三：今日无符合条件股票")

    # ---- 系统四 ----
    print("\n[5/7] 系统四 · 放量突破最优一只")
    print(f"  条件同系统二，候选 {len(s2_results)} 只 → 打分挑最优")
    best_s2 = pick_best(s2_results, score_breakout)
    if best_s2:
        print(f"\n  🏆 系统四 最优: {best_s2['code']} {best_s2['name']}  评分 {best_s2['best_score']:.1f}")
        for r in best_s2.get("best_reasons", []):
            print(f"     · {r}")
    else:
        print("  系统四：今日无符合条件股票")

    # ---- 系统五 ----
    print("\n[6/7] 系统五 · 强势多头突破（全部）")
    print("  拉取板块与大盘数据（各1次请求）……")
    fetch_block_gain()
    fetch_index_gain()
    print(f"  行业板块 {len(_BLOCK_GAIN)} 个；大盘 {_INDEX_NAME} {_INDEX_GAIN:+.2f}%")
    # 列表数据即可判定的条件前置（涨幅/量比/换手/市值/分时价>均价/日内位置），尽量少拉K线
    s5_candidates = [
        s for s in all_stocks
        if S5_CHG_MIN <= s["change_pct"] <= S5_CHG_MAX
        and s["vol_ratio"] > S5_VOL_RATIO_MIN
        and S5_TURN_MIN <= s["turnover"] <= S5_TURN_MAX
        and S5_MV_MIN <= s["circ_mv_yi"] <= S5_MV_MAX
        and s["above_avg"] > 0
        and (s["pos_in_range"] is not None and s["pos_in_range"] >= S5_RANGE_POS_MIN)
    ]
    print(f"  初筛（涨幅/量比/换手/市值/分时价>均价/日内位置）: {len(s5_candidates)} 只")
    s5_results = run_checks(s5_candidates, check_s5, "系统五") if s5_candidates else []
    for r in s5_results:
        sc, rs = score_s5(r)
        r["s5_score"], r["s5_reasons"] = sc, rs
    s5_results.sort(key=lambda x: x.get("s5_score", 0), reverse=True)
    if s5_results:
        top = s5_results[0]
        print(f"\n  🏆 系统五 最优: {top['code']} {top['name']}  评分 {top['s5_score']:.1f}")
        for r in top.get("s5_reasons", []):
            print(f"     · {r}")
    else:
        print("  系统五：今日无符合条件股票")

    # ---- 输出 ----
    print("\n[7/7] 生成 HTML 报告……")
    out_dir = Path.home() / "Downloads" / "股票筛选"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = out_dir / f"stock-screen_{ts}.html"

    if s1_results: s1_results.sort(key=lambda x: x["circ_mv_yi"], reverse=True)
    if s2_results: s2_results.sort(key=lambda x: x["circ_mv_yi"], reverse=True)

    write_html(s1_results, s2_results, s5_results, best_s1, best_s2,
               out_path=html_path, scan_cnt=len(all_stocks))

    if s1_results:
        pd.DataFrame(s1_results).to_csv(out_dir / f"系统一_洗盘回踩_{ts}.csv",
                                        index=False, encoding="utf-8-sig")
    if s2_results:
        pd.DataFrame(s2_results).to_csv(out_dir / f"系统二_放量突破_{ts}.csv",
                                        index=False, encoding="utf-8-sig")
    if best_s1:
        pd.DataFrame([best_s1]).to_csv(out_dir / f"系统三_洗盘最优_{ts}.csv",
                                       index=False, encoding="utf-8-sig")
    if best_s2:
        pd.DataFrame([best_s2]).to_csv(out_dir / f"系统四_突破最优_{ts}.csv",
                                       index=False, encoding="utf-8-sig")
    if s5_results:
        s5_export = [{k: v for k, v in r.items() if k != "s5_reasons"} for r in s5_results]
        pd.DataFrame(s5_export).to_csv(out_dir / f"系统五_强势突破_{ts}.csv",
                                       index=False, encoding="utf-8-sig")

    print(f"\n✅ HTML 报告已保存: {html_path}")
    print(f"   系统一 通过: {len(s1_results)} 只")
    print(f"   系统二 通过: {len(s2_results)} 只")
    if best_s1:
        print(f"   系统三 最优: {best_s1['code']} {best_s1['name']}（{best_s1['best_score']:.1f}分）")
    if best_s2:
        print(f"   系统四 最优: {best_s2['code']} {best_s2['name']}（{best_s2['best_score']:.1f}分）")
    print(f"   系统五 通过: {len(s5_results)} 只")
    print("\n" + "=" * 72)
    print("完成！")
    print("=" * 72)

if __name__ == "__main__":
    main()

