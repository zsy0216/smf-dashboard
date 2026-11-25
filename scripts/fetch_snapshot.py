#!/usr/bin/env python3
"""
SMF Dashboard 数据快照生成器（GitHub Actions 定时运行，生成 data.json 作为前端兜底）。

数据源全部为东方财富公开接口（与前端直连使用同一批接口、同一套口径与公式）：
  - push2.eastmoney.com      实时行情 / 板块与 ETF 主力资金（今日、3日、5日、10日）
  - push2his.eastmoney.com   日频资金流 K 线（累计 20 日）、指数成交额历史、板块涨跌幅
  - datacenter-web.eastmoney.com  融资融券历史（T+1）、龙虎榜席位明细（最近交易日）

口径说明（与页面「数据源配置与指标说明」一致）：
  - 两市成交额 = 上证指数成交额 + 深证综指成交额（不含北交所）
  - 板块「大单净流入」= 主力净额（超大单 + 大单）；表格第三列为超大单净额
  - ETF「净流入」= 二级市场主力净额（f62 系列），不是份额申赎估算；规模 = 总市值
  - 融资净买入 = RPTA_RZRQ_LSHJ 的 RZJME（融资买入额 - 融资偿还额），T+1 披露
  - 龙虎榜为最近一个交易日席位明细聚合（机构专用 / 沪深股通 / 拉萨系 / 其余营业部=游资合计）
  - 情绪评分 = 四维加权：ETF 资金 30% + 杠杆情绪 20% + 板块轮动（广度）25% + 博弈结构 25%
用法：python3 scripts/fetch_snapshot.py [输出路径=data.json]

备选源：东财整轮不可达时自动改用新浪财经公开接口生成备选快照
（个股资金流排行 + 两市成交额为新浪实时口径，其余面板沿用上一版
东财快照并在 source/stamp 中明示）；环境变量 SMF_FORCE_SINA=1 可
强制走新浪路径（验证用）。两源都失败才跳过本轮。
"""
import copy
import json
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests


def _now_cst():
    """北京时间字符串。云端 runner 是 UTC，绝不能用本地时间当更新时间展示。"""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
    except Exception:
        tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

S = requests.Session()

# 全局请求节流：云厂商出口 IP 容易触发源站按频率断连（RST），
# 把请求间隔拉开到 ≥0.45s、并发降下来后成功率显著更高（本地无此问题，但无害）。
_pace_lock = threading.Lock()
_pace_last = [0.0]


def _pace():
    with _pace_lock:
        wait = 0.45 - (time.time() - _pace_last[0])
        if wait > 0:
            time.sleep(wait)
        _pace_last[0] = time.time()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
})
UT_HIS = "b2884a393a59ad64002292a3e90d46a5"
UT_KLINE = "fa5fd1943c7b386f172d6893dbfba10b"
YI = 1e8

# 重点监测的宽基 / 主题 ETF（市场代码：1=沪 0=深）
ETF_WATCH = ["1.510300", "0.159352", "0.159915", "1.512100",
             "1.588000", "1.510050", "0.159919", "1.512880"]
PERIODS = {"today": 1, "d3": 3, "d5": 5, "d20": 20}
# 各周期主力净额在 clist/ulist 中的字段（20 日无现成字段，用日频资金流 K 线累计）
NET_FIELD = {"today": "f62", "d3": "f267", "d5": "f164", "d20": "f174"}  # d20 仅用于选股
RATIO_FIELD = {"today": "f184", "d3": "f268", "d5": "f165", "d20": "f175"}


PUSH2_HOSTS = ("push2.eastmoney.com", "push2his.eastmoney.com",
               "82.push2.eastmoney.com", "1.push2.eastmoney.com",
               "48.push2.eastmoney.com", "7.push2.eastmoney.com",
               "45.push2.eastmoney.com", "63.push2.eastmoney.com",
               "push2delay.eastmoney.com")


def get_json(url, timeout=12, retries=6):
    """push2 接口逐次轮换域名重试（源站对单个域名/IP 的 502 限流是间歇性的，
    换域名往往立刻恢复）；其它域名原地退避重试。"""
    last = None
    is_push2 = "://push2.eastmoney.com" in url
    for i in range(retries):
        u = url
        if is_push2:
            u = url.replace("://push2.eastmoney.com",
                            f"://{PUSH2_HOSTS[i % len(PUSH2_HOSTS)]}")
        try:
            _pace()
            r = S.get(u, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise last


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def fflow_daily(secid, lmt=21):
    """日频资金流 K 线 -> [(date, main, super), ...] 单位：元"""
    url = ("https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get"
           f"?lmt=0&klt=101&secid={secid}&fields1=f1,f2,f3,f7"
           "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63"
           f"&ut={UT_HIS}")
    try:
        rows = get_json(url)["data"]["klines"]
    except Exception:
        return []
    out = []
    for row in rows[-lmt:]:
        p = row.split(",")
        out.append((p[0], float(p[1]), float(p[5])))
    return out


def price_daily(secid, lmt=46):
    """日 K -> [(date, close, amount_yuan), ...]"""
    url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get"
           f"?secid={secid}&ut={UT_KLINE}&fields1=f1,f2,f3,f4,f5,f6"
           f"&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=0&end=20500101&lmt={lmt}")
    try:
        rows = get_json(url)["data"]["klines"]
    except Exception:
        return []
    out = []
    for row in rows:
        p = row.split(",")
        out.append((p[0], float(p[2]), float(p[6])))
    return out


def trends_amounts(secid):
    """分时成交额（近 2 个交易日）-> {date: [(HH:MM, amount_yuan), ...]}"""
    url = ("https://push2his.eastmoney.com/api/qt/stock/trends2/get"
           f"?secid={secid}&ut={UT_KLINE}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"
           "&fields2=f51,f52,f53,f54,f55,f56,f57,f58&iscr=0&ndays=2")
    out = {}
    try:
        for row in get_json(url)["data"]["trends"]:
            p = row.split(",")
            out.setdefault(p[0][:10], []).append((p[0][11:16], float(p[6])))
    except Exception:
        pass
    return out


def fetch_turnover():
    """两市成交额历史（沪 + 深综指），今日用实时行情覆盖/追加。

    返回 (hist, prev_same_time)：prev_same_time 为上一交易日截至今日当前时刻的
    累计成交额（盘中涨跌幅按同时段比较，避免拿盘中累计和昨日全天硬比）。
    """
    sh = price_daily("1.000001")
    sz = price_daily("0.399106")
    sz_map = {d: amt for d, _, amt in sz}
    hist = [(d, amt + sz_map.get(d, 0.0)) for d, _, amt in sh if d in sz_map]
    prev_same_time = None
    try:
        j = get_json("https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2"
                     "&fields=f6,f12&secids=1.000001,0.399106")
        today_amt = sum(x["f6"] for x in j["data"]["diff"])
        today_str = time.strftime("%Y-%m-%d")
        if hist and hist[-1][0] == today_str:
            hist[-1] = (today_str, today_amt)
        else:
            hist.append((today_str, today_amt))
        # 同时段比较：今日最后一分钟对应的时刻，截取昨日分时累计
        sh_t, sz_t = trends_amounts("1.000001"), trends_amounts("0.399106")
        dates = sorted(sh_t.keys())
        # 沪深两市分时都齐全才做同时段比较；任一缺失就放弃（否则会算出翻倍的假涨幅）
        if (len(dates) >= 2 and dates[-1] == today_str
                and sz_t.get(dates[-1]) and sz_t.get(dates[-2])):
            last_time = sh_t[today_str][-1][0]
            prev_same_time = 0.0
            for src in (sh_t, sz_t):
                prev_same_time += sum(a for t, a in src.get(dates[-2], []) if t <= last_time)
    except Exception:
        pass
    return hist, prev_same_time  # [(date, amount_yuan)], float|None


def fetch_boards():
    """全部行业板块（m:90+t:2），按 (主力净额, 涨跌幅) 去重掉 银行/银行Ⅱ 这类层级重复。"""
    fields = "f12,f14,f3,f10,f62,f66,f164,f165,f174,f175,f184,f267,f268"
    diff = []
    for pn in (1, 2, 3):  # pz=200 分三页拉全量（总数约 496），小页更不容易触发源站限流
        url = ("https://push2.eastmoney.com/api/qt/clist/get"
               f"?pn={pn}&pz=200&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2&fields={fields}")
        page = get_json(url)["data"]["diff"]
        diff.extend(page)
        if len(page) < 200:
            break
        time.sleep(0.7)
    seen, boards = {}, []
    for b in diff:
        key = (b.get("f62"), b.get("f3"))
        if key in seen:
            if len(b["f14"]) < len(seen[key]["f14"]):
                boards[boards.index(seen[key])] = b
                seen[key] = b
            continue
        seen[key] = b
        boards.append(b)
    return boards


def fetch_etfs():
    url = ("https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2"
           "&fields=f2,f3,f6,f10,f12,f13,f14,f20,f62,f164,f165,f174,f175,f184,f267,f268,f441"
           "&secids=" + ",".join(ETF_WATCH))
    return get_json(url)["data"]["diff"]


def fetch_margin():
    url = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
           "?reportName=RPTA_RZRQ_LSHJ&columns=DIM_DATE,RZYE,RZJME,RZJME3D,RZJME5D"
           "&pageNumber=1&pageSize=25&sortTypes=-1&sortColumns=DIM_DATE")
    return get_json(url)["result"]["data"]  # 已按日期倒序


def fetch_lhb():
    """最近交易日龙虎榜席位聚合。返回 (date_str, buckets, n_seats) 或 None。"""
    base = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    j = get_json(base + "?reportName=RPT_DAILYBILLBOARD_DETAILSNEW&columns=TRADE_DATE"
                        "&pageNumber=1&pageSize=1&sortTypes=-1&sortColumns=TRADE_DATE")
    date = j["result"]["data"][0]["TRADE_DATE"][:10]
    filt = f"(TRADE_DATE='{date}')"
    rows, seen = [], set()
    for rpt in ("RPT_BILLBOARD_DAILYDETAILSBUY", "RPT_BILLBOARD_DAILYDETAILSSELL"):
        try:
            jj = get_json(base + f"?reportName={rpt}&columns=SECUCODE,OPERATEDEPT_NAME,BUY,SELL,TRADE_ID"
                                 f"&pageNumber=1&pageSize=500&filter={requests.utils.quote(filt, safe='()=+')}")
            for row in jj["result"]["data"]:
                k = (row["SECUCODE"], row["OPERATEDEPT_NAME"], row.get("TRADE_ID"))
                if k in seen:
                    continue
                seen.add(k)
                rows.append(row)
        except Exception:
            pass
    buckets = {
        "机构专用席位": [0.0, 0.0],
        "一线游资·营业部合计": [0.0, 0.0],
        "拉萨天团（散户）": [0.0, 0.0],
        "沪深股通专用": [0.0, 0.0],
    }
    for row in rows:
        name = row["OPERATEDEPT_NAME"] or ""
        buy, sell = float(row.get("BUY") or 0) / YI, float(row.get("SELL") or 0) / YI
        if name == "机构专用":
            b = buckets["机构专用席位"]
        elif "股通专用" in name:
            b = buckets["沪深股通专用"]
        elif "拉萨" in name:
            b = buckets["拉萨天团（散户）"]
        else:
            b = buckets["一线游资·营业部合计"]
        b[0] += buy
        b[1] += sell
    out = [{"name": k, "buy": round(v[0], 1), "sell": round(v[1], 1)}
           for k, v in buckets.items()]
    return date, out


FLOW_HOSTS = ("push2.eastmoney.com", "82.push2.eastmoney.com",
              "1.push2.eastmoney.com", "48.push2.eastmoney.com",
              "7.push2.eastmoney.com", "45.push2.eastmoney.com",
              "63.push2.eastmoney.com", "push2delay.eastmoney.com")


def get_json_hosts(url, hosts, timeout=12, retries=8):
    """在指定域名集合间轮换（分时资金流接口不能走 push2his：它会返回空数据）。"""
    last = None
    for i in range(retries):
        host = hosts[i % len(hosts)]
        u = url.replace("://push2.eastmoney.com", f"://{host}")
        try:
            _pace()
            r = S.get(u, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


def fetch_flow():
    """大盘资金流：沪+深指数分时累计净额曲线（亿元）+ 近10日主力净额。

    分时线列：时间,主力,小单,中单,大单,超大单（累计值，元）。
    收盘后 push2 仍保留当日分钟线；非交易日/盘前取不到时返回 None，
    页面改用 flow_daily 展示。
    """
    fields = ("fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,"
              "f57,f58,f59,f60,f61,f62,f63")
    merged = {}
    ok = False
    for secid in ("1.000001", "0.399106"):
        url = ("https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
               f"?lmt=0&klt=1&secid={secid}&{fields}&ut={UT_HIS}")
        try:
            rows = get_json_hosts(url, FLOW_HOSTS)["data"]["klines"]
        except Exception:
            rows = []
        if rows:
            ok = True
        for row in rows:
            p = row.split(",")
            t = p[0][11:16]
            acc = merged.setdefault(t, [0.0] * 5)
            for i in range(5):
                acc[i] += float(p[1 + i])
    flow = None
    if ok and merged:
        times = sorted(merged)
        flow = {"times": times,
                "main": [round(merged[t][0] / YI, 2) for t in times],
                "small": [round(merged[t][1] / YI, 2) for t in times],
                "mid": [round(merged[t][2] / YI, 2) for t in times],
                "big": [round(merged[t][3] / YI, 2) for t in times],
                "super": [round(merged[t][4] / YI, 2) for t in times]}
    daily = {}
    for secid in ("1.000001", "0.399106"):
        for d, main, sup in fflow_daily(secid, 10):
            a = daily.setdefault(d, [0.0, 0.0])
            a[0] += main
            a[1] += sup
    flow_daily = [{"date": d, "main": round(v[0] / YI, 1), "super": round(v[1] / YI, 1)}
                  for d, v in sorted(daily.items())]
    return flow, flow_daily


def fetch_stocks():
    """全市场个股主力净额 Top10 流入/流出（今日）。"""
    fields = "f12,f14,f2,f3,f6,f62,f184"
    fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"

    def top(po):
        url = ("https://push2.eastmoney.com/api/qt/clist/get"
               f"?pn=1&pz=10&po={po}&np=1&fltt=2&invt=2&fid=f62&fs={fs}&fields={fields}")
        diff = get_json(url)["data"]["diff"]
        return [{"name": s["f14"], "code": s["f12"], "price": s["f2"],
                 "chg": s["f3"], "net": round(s["f62"] / YI, 2),
                 "ratio": s["f184"], "amt": round(s["f6"] / YI, 1)} for s in diff]

    try:
        return {"in": top(1), "out": top(0)}
    except Exception:
        return None


def fetch_mood(trade_date, prev_date=None):
    """涨停/跌停/炸板池（push2ex）-> 市场情绪温度。trade_date: YYYY-MM-DD。

    温度公式（0-100，页面与日报同口径）：
      temp = clamp(30 + 涨停*0.5 - 跌停*1.5 - 炸板率*30 + 最高连板*3)
    """
    d = trade_date.replace("-", "")
    ut = "7eea3edcaed734bea9cbfc24409ed989"
    base = "https://push2ex.eastmoney.com"
    try:
        zt = get_json(f"{base}/getTopicZTPool?ut={ut}&dpt=wz.ztzt&Pageindex=0&pagesize=300&sort=fbt:asc&date={d}")
        dt = get_json(f"{base}/getTopicDTPool?ut={ut}&dpt=wz.ztzt&Pageindex=0&pagesize=100&sort=fund:asc&date={d}")
        zb = get_json(f"{base}/getTopicZBPool?ut={ut}&dpt=wz.ztzt&Pageindex=0&pagesize=100&sort=fbt:asc&date={d}")
    except Exception:
        return None
    zpool = (zt.get("data") or {}).get("pool") or []
    zt_n = (zt.get("data") or {}).get("tc", len(zpool))
    dt_n = (dt.get("data") or {}).get("tc", 0)
    zb_n = (zb.get("data") or {}).get("tc", 0)
    if zt_n == 0 and dt_n == 0 and zb_n == 0:
        return None
    max_lbc = max([x.get("lbc", 1) for x in zpool] or [0])
    lb2 = sum(1 for x in zpool if x.get("lbc", 1) >= 2)
    lb3 = sum(1 for x in zpool if x.get("lbc", 1) >= 3)
    fund_yi = sum((x.get("fund") or 0) for x in zpool) / YI
    zb_rate = zb_n / (zt_n + zb_n) * 100 if (zt_n + zb_n) else 0.0
    temp = clamp(30 + zt_n * 0.5 - dt_n * 1.5 - zb_rate * 0.30 + max_lbc * 3)
    level = ("火热" if temp >= 75 else "偏暖" if temp >= 60 else
             "中性" if temp >= 45 else "偏冷" if temp >= 30 else "冰点")
    mood = {"date": trade_date, "zt": zt_n, "dt": dt_n, "zb": zb_n,
            "zbRate": round(zb_rate, 1), "maxLbc": max_lbc, "lb2": lb2, "lb3": lb3,
            "fundYi": round(fund_yi, 1), "temp": round(temp), "level": level}
    # 赚钱效应 1：全市场涨跌家数（沪指+深综指成分汇总，f104 涨/f105 跌/f106 平）
    try:
        u = get_json("https://push2.eastmoney.com/api/qt/ulist.np/get"
                     "?fltt=2&invt=2&fields=f104,f105,f106&secids=1.000001,0.399106")
        diff = u["data"]["diff"]
        adv = sum(x.get("f104") or 0 for x in diff)
        dec = sum(x.get("f105") or 0 for x in diff)
        flat = sum(x.get("f106") or 0 for x in diff)
        if adv + dec > 0:
            mood.update({"adv": adv, "dec": dec, "flat": flat,
                         "upRatio": round(adv / (adv + dec + flat) * 100, 1)})
    except Exception:
        pass
    # 赚钱效应 2：昨日涨停股今日平均涨幅与红盘率（短线接力意愿）
    if prev_date:
        try:
            pd_ = prev_date.replace("-", "")
            y = get_json(f"{base}/getTopicZTPool?ut={ut}&dpt=wz.ztzt"
                         f"&Pageindex=0&pagesize=300&sort=fbt:asc&date={pd_}")
            ypool = (y.get("data") or {}).get("pool") or []
            secids = [f"{x['m']}.{x['c']}" for x in ypool]
            chgs = []
            for i in range(0, len(secids), 100):
                chunk = ",".join(secids[i:i + 100])
                q = get_json("https://push2.eastmoney.com/api/qt/ulist.np/get"
                             f"?fltt=2&invt=2&fields=f3&secids={chunk}")
                for s in (q["data"] or {}).get("diff") or []:
                    if isinstance(s.get("f3"), (int, float)):
                        chgs.append(s["f3"])
            if chgs:
                mood.update({"yztDate": prev_date, "yztAvg": round(sum(chgs) / len(chgs), 2),
                             "yztRed": round(sum(1 for c in chgs if c > 0) / len(chgs) * 100, 1),
                             "yztN": len(chgs)})
        except Exception:
            pass
    return mood


# ================= 新浪备选源 =================
# 东财对云 IP 限流整轮不可达时的备胎：新浪与东财是两套系统、限流互不影响。
# 口径差异（明示在产出 source/stamp 中）：个股 net 为新浪「全单净额」
# （netamount），非东财「主力净额（超大+大单）」；成交额为沪指+深综指
# 成交额（hq.sinajs.cn），与东财口径一致。新浪无板块资金流/涨停池/
# 龙虎榜/融资融券对应公开接口，这些面板沿用上一版东财快照。
SINA_H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
          "Referer": "https://finance.sina.com.cn/"}
SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"


def sina_json(url, timeout=12, retries=3):
    last = None
    for i in range(retries):
        try:
            _pace()
            r = requests.get(url, headers=SINA_H, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict) and j.get("__ERROR"):
                raise RuntimeError(f"sina input error: {url}")
            return j
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise last


def sina_stocks_top():
    """个股资金流 Top10 流入/流出（新浪全单净额口径）。"""
    def top(asc):
        rows = sina_json(f"{SINA_API}/MoneyFlow.ssl_bkzj_ssggzj"
                         f"?page=1&num=10&sort=netamount&asc={asc}")
        out = []
        for x in rows:
            sym = x.get("symbol") or ""
            code = sym[2:] if sym[:2] in ("sh", "sz", "bj") else sym
            out.append({
                "name": x.get("name") or "",
                "code": code,
                "price": float(x.get("trade") or 0),
                "chg": round(float(x.get("changeratio") or 0) * 100, 2),
                "net": round(float(x.get("netamount") or 0) / YI, 2),
                "ratio": round(float(x.get("ratioamount") or 0) * 100, 2),
                "amt": round(float(x.get("amount") or 0) / YI, 1),
            })
        return out
    return {"in": top(0), "out": top(1)}


def sina_turnover_yuan():
    """两市成交额（元）= 上证指数成交额 + 深证综指成交额（hq.sinajs.cn，单位万元）。"""
    _pace()
    r = requests.get("https://hq.sinajs.cn/list=s_sh000001,s_sz399106",
                     headers=SINA_H, timeout=12)
    r.raise_for_status()
    text = r.content.decode("gbk", "ignore")
    total_wan = 0.0
    for line in text.strip().splitlines():
        body = line.split('"')[1] if '"' in line else ""
        parts = body.split(",")
        if len(parts) >= 6:
            total_wan += float(parts[5])
    if total_wan <= 0:
        raise RuntimeError("sina turnover empty")
    return total_wan * 1e4


def build_sina_fallback(prev):
    """东财不可达时的备选快照：today 的个股榜/成交额换新浪实时值，
    其余作用域与面板沿用上一版东财快照（prev），并在 source/stamp 明示。"""
    if not prev or not prev.get("data") or "today" not in prev["data"]:
        raise RuntimeError("新浪备选需要上一版 data.json 作为沿用底稿，但不存在")
    stocks = sina_stocks_top()
    turnover_yuan = sina_turnover_yuan()
    now = _now_cst()
    today_str = now[:10]
    data = copy.deepcopy(prev["data"])
    t = data["today"]
    # 成交额涨跌：与上一交易日全天比较（新浪无同时段分时）
    spark_t = (t.get("spark") or {}).get("turnover") or []
    if prev.get("trade_date") == today_str and len(spark_t) >= 2:
        prev_day_wanyi = spark_t[-2]
    elif spark_t:
        prev_day_wanyi = spark_t[-1]
    else:
        prev_day_wanyi = 0
    t_v_wanyi = turnover_yuan / 1e12
    if prev_day_wanyi:
        t_chg = (t_v_wanyi / prev_day_wanyi - 1) * 100
        if prev.get("trade_date") == today_str:
            spark_t[-1] = round(t_v_wanyi, 2)
        else:
            spark_t.append(round(t_v_wanyi, 2))
            del spark_t[:-8]
        t.setdefault("spark", {})["turnover"] = spark_t
    else:
        t_chg = 0.0
    t["kpi"]["turnover"] = {"v": round(t_v_wanyi, 2), "unit": "万亿",
                            "chg": round(t_chg, 1), "chgUnit": "%",
                            "hint": "较前一交易日全天 · 新浪口径（备选轮次）"}
    t["stocks"] = stocks
    t["stamp"] = (f"单日口径 · 本轮为新浪备选（东财不可达）：个股资金流/成交额为"
                  f"新浪实时口径（个股为全单净额），其余面板沿用 {prev.get('updated_at', '')} "
                  f"东财快照 · 融资截至 {prev.get('margin_date', '')} · 龙虎榜 {prev.get('lhb_date', '')}")
    return {
        "updated_at": now,
        "trade_date": today_str if turnover_yuan > 0 else prev.get("trade_date", today_str),
        "source": "新浪财经公开接口（备选快照：东财不可达时启用）",
        "lhb_date": prev.get("lhb_date", ""),
        "margin_date": prev.get("margin_date", ""),
        "data": data,
    }


def build():
    turnover_hist, prev_same_time = fetch_turnover()
    time.sleep(1)
    boards = fetch_boards()
    time.sleep(1)
    etfs = fetch_etfs()
    time.sleep(1)
    margin_rows = fetch_margin()
    lhb = fetch_lhb()
    sh_fflow = fflow_daily("1.000001", 10)

    amounts = [a for _, a in turnover_hist]
    # ---- 板块：按各周期主力净额选 前5流入 + 后3流出，取并集拉日频资金流/价格 ----
    selections = {}
    union_codes = []
    for pk in PERIODS:
        f = NET_FIELD[pk]
        ranked = sorted(boards, key=lambda b: b.get(f) or 0, reverse=True)
        sel = ranked[:5] + ranked[-3:]
        selections[pk] = sel
        for b in sel:
            if b["f12"] not in union_codes:
                union_codes.append(b["f12"])
    board_ff, board_pk = {}, {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fflow_daily, f"90.{c}", 21): ("ff", c) for c in union_codes}
        futs.update({ex.submit(price_daily, f"90.{c}", 22): ("pk", c) for c in union_codes})
        for fut, (kind, code) in list(futs.items()):
            (board_ff if kind == "ff" else board_pk)[code] = fut.result()
    etf_ff = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fflow_daily, s, 21): s for s in ETF_WATCH}
        for fut, secid in list(futs.items()):
            etf_ff[secid] = fut.result()

    # ---- 龙虎榜博弈结构分（各周期共用最近交易日） ----
    if lhb:
        lhb_date, lhb_list = lhb
        net = {x["name"]: x["buy"] - x["sell"] for x in lhb_list}
        inst = net.get("机构专用席位", 0.0)
        hot = net.get("一线游资·营业部合计", 0.0)
        retail = net.get("拉萨天团（散户）", 0.0)
    else:
        lhb_date, lhb_list, inst, hot, retail = "", [], 0.0, 0.0, 0.0
    struct_score = clamp(50 + (inst + hot - retail) / 4.0)

    margin_latest = margin_rows[0]
    margin_date = margin_latest["DIM_DATE"][:10]
    mood = fetch_mood(turnover_hist[-1][0],
                      turnover_hist[-2][0] if len(turnover_hist) >= 2 else None)
    time.sleep(1)
    flow, flow_daily = fetch_flow()
    stocks = fetch_stocks()

    data = {}
    for pk, n in PERIODS.items():
        nf, rf = NET_FIELD[pk], RATIO_FIELD[pk]
        # 成交额：周期内日均 vs 前一周期日均；today 为当日 vs 昨日
        if pk == "today":
            t_v = amounts[-1]
            if prev_same_time:  # 盘中：与昨日同时段累计比较
                t_chg = (t_v / prev_same_time - 1) * 100
                t_hint = "较昨日同时段累计"
            else:  # 收盘后/分时不可用：与昨日全天比较
                t_chg = (t_v / amounts[-2] - 1) * 100
                t_hint = "较前一交易日全天"
        else:
            cur = amounts[-n:]
            prev = amounts[-2 * n:-n]
            t_v = sum(cur) / n
            t_chg = (t_v / (sum(prev) / len(prev)) - 1) * 100
            t_hint = f"日均，较前 {n} 日均值"
        # ETF
        etf_rows, etf_net_sum = [], 0.0
        for e in etfs:
            secid = f"{e['f13']}.{e['f12']}"
            if pk == "d20":
                ff = etf_ff.get(secid, [])
                net = sum(x[1] for x in ff[-20:]) / YI
                ratio = None
            else:
                net = (e.get(nf) or 0) / YI
                ratio = e.get(rf)
            etf_net_sum += net
            premium = ((e["f2"] / e["f441"] - 1) * 100) if e.get("f441") else 0.0
            etf_rows.append({
                "name": e["f14"], "code": e["f12"], "net": round(net, 1),
                "share": round(ratio, 2) if ratio is not None else 0.0,
                "premium": round(premium, 2), "aum": round((e.get("f20") or 0) / YI),
            })
        etf_turn = sum((e.get("f6") or 0) for e in etfs) / YI
        etf_ratio = (etf_net_sum / etf_turn * 100) if etf_turn else 0.0
        # 融资（T+1）：周期净买入与余额变动
        if pk == "today":
            m_net = margin_latest["RZJME"] / YI
        elif pk == "d3":
            m_net = (margin_latest.get("RZJME3D") or 0) / YI
        elif pk == "d5":
            m_net = (margin_latest.get("RZJME5D") or 0) / YI
        else:
            m_net = sum((r.get("RZJME") or 0) for r in margin_rows[:20]) / YI
        rz_now = margin_rows[0]["RZYE"]
        rz_then = margin_rows[min(n, len(margin_rows) - 1)]["RZYE"]
        m_bal_chg = (rz_now - rz_then) / YI
        # 板块
        sec_rows = []
        for b in selections[pk]:
            code = b["f12"]
            ff = board_ff.get(code, [])
            pk_rows = board_pk.get(code, [])
            if ff:
                big = sum(x[1] for x in ff[-n:]) / YI
                sup = sum(x[2] for x in ff[-n:]) / YI
            else:
                big, sup = (b.get(nf) or 0) / YI, (b.get("f66") or 0) / YI
            if pk == "today":
                chg = b.get("f3") or 0.0
            elif len(pk_rows) > n:
                chg = (pk_rows[-1][1] / pk_rows[-1 - n][1] - 1) * 100
            else:
                chg = b.get("f3") or 0.0
            sec_rows.append({"name": b["f14"], "code": b["f12"], "chg": round(chg, 2),
                             "big": round(big, 1), "margin": round(sup, 1),
                             "vol": round(b.get("f10") or 0, 2)})
        breadth = (sum(1 for b in boards if (b.get(nf) or 0) > 0) / len(boards) * 100) if boards else 50.0
        # 四维评分
        d_etf = clamp(50 + etf_ratio * 3)
        d_margin = clamp(50 + (m_net / n) / 6.0)
        d_rot = clamp(breadth)
        d_struct = struct_score
        score = round(0.30 * d_etf + 0.20 * d_margin + 0.25 * d_rot + 0.25 * d_struct)
        level = ("强势进攻" if score >= 75 else "偏多进攻" if score >= 65 else
                 "启动初期" if score >= 55 else "多空均衡" if score >= 45 else
                 "偏空防守" if score >= 35 else "弱势避险")
        tone = "up" if score >= 55 else "neu" if score >= 45 else "down"
        # 评分 spark：上证日频主力净额 / 当日成交额 映射
        score_spark = []
        for i, (d, main, _s) in enumerate(sh_fflow[-8:]):
            amt = dict(turnover_hist).get(d, 0) or 1
            score_spark.append(round(clamp(50 + main / amt * 600)))
        if pk != "today":
            score_spark = score_spark[:-1] + [score] if score_spark else [score]
        scope = {"today": "单日", "d3": "3日累计", "d5": "5日累计", "d20": "20日累计"}[pk]
        stamp = (f"{scope}口径 · 板块/ETF 为主力净额 · 融资截至 {margin_date} · "
                 f"龙虎榜 {lhb_date}")
        data[pk] = {
            "label": {"today": "今日实时", "d3": "近3日", "d5": "近5日", "d20": "近20日"}[pk],
            "scope": scope,
            "stamp": stamp,
            "kpi": {
                "turnover": {"v": round(t_v / 1e12, 2), "unit": "万亿",
                             "chg": round(t_chg, 1), "chgUnit": "%", "hint": t_hint},
                "etf": {"v": round(etf_net_sum, 1), "chg": round(etf_ratio, 1),
                        "chgUnit": "%", "hint": "主力净额占成交额比"},
                "margin": {"v": round(m_net, 1), "chg": round(m_bal_chg, 1),
                           "hint": f"融资余额变动 · 截至 {margin_date}"},
                "score": {"v": score, "level": level, "tone": tone, "hint": "四维加权"},
            },
            "spark": {
                "turnover": [round(a / 1e12, 2) for a in amounts[-8:]],
                "etf": [round(sum((etf_ff[s][-(8 - i)][1] if len(etf_ff.get(s, [])) >= 8 - i else 0)
                                   for s in ETF_WATCH) / YI, 1) for i in range(8)],
                "margin": [round((r.get("RZJME") or 0) / YI, 1) for r in reversed(margin_rows[:8])],
                "score": score_spark,
            },
            "dims": [
                {"name": "ETF 资金（慢钱）", "score": round(d_etf)},
                {"name": "杠杆情绪（快钱）", "score": round(d_margin)},
                {"name": "板块轮动强度", "score": round(d_rot)},
                {"name": "博弈结构质量", "score": round(d_struct)},
            ],
            "etf": etf_rows,
            "sectors": sec_rows,
            "lhb": lhb_list,
            "mood": mood,
            "flow": flow,
            "flowDaily": flow_daily,
            "stocks": stocks,
        }
    return {
        "updated_at": _now_cst(),
        "trade_date": turnover_hist[-1][0],
        "source": "东方财富公开接口（脚本快照，前端直连失败时兜底）",
        "lhb_date": lhb_date,
        "margin_date": margin_date,
        "data": data,
    }


if __name__ == "__main__":
    out_path = sys.argv[1] if len(sys.argv) > 1 else "data.json"
    prev = None
    for cand in (out_path, "data.json"):
        try:
            if os.path.exists(cand):
                with open(cand, encoding="utf-8") as f:
                    prev = json.load(f)
                break
        except Exception:
            prev = None
    if os.environ.get("SMF_FORCE_SINA") == "1":
        print("SMF_FORCE_SINA=1: skipping eastmoney, using sina fallback")
        payload = build_sina_fallback(prev)
    else:
        try:
            payload = build()
        except Exception as e:
            print(f"eastmoney build failed: {e!r}; falling back to sina")
            payload = build_sina_fallback(prev)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    print(f"wrote {out_path} updated_at={payload['updated_at']} "
          f"lhb={payload['lhb_date']} margin={payload['margin_date']}")
    for k, v in payload["data"].items():
        print(k, "turnover", v["kpi"]["turnover"], "etf", v["kpi"]["etf"]["v"],
              "margin", v["kpi"]["margin"]["v"], "score", v["kpi"]["score"]["v"],
              "sectors", len(v["sectors"]), "etf_rows", len(v["etf"]), "lhb", len(v["lhb"]))
