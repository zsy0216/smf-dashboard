#!/usr/bin/env python3
"""收盘日报生成器：读取 data.json 快照，生成 reports/YYYY-MM-DD.md 并更新 reports/index.md。

用法: python3 scripts/daily_report.py [data.json 路径] [reports 目录]
设计：只用快照里已有的数据，不再发网络请求（避免收盘后源站限流影响日报）；
当日已生成过则覆盖（幂等）。
"""
import json
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
snap_path = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else root / "data.json"
rep_dir = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else root / "reports"

snap = json.loads(snap_path.read_text(encoding="utf-8"))
date = snap["trade_date"]
today = snap["data"]["today"]
kpi = today["kpi"]


def sig_of(score):
    return ("强" if score >= 65 else "偏强" if score >= 55 else
            "均衡" if score >= 45 else "偏弱" if score >= 35 else "弱")


lines = []
lines.append(f"# A股主力资金收盘日报 · {date}")
lines.append("")
lines.append(f"> 快照生成时间：{snap['updated_at']}（北京时间） · 数据源：东方财富公开接口")
lines.append("")
lines.append("## 市场总览")
lines.append("")
to = kpi["turnover"]
_tline = f"- **两市成交额**：{to['v']} {to['unit']}（{to['hint']} {to['chg']:+.1f}{to['chgUnit']}）"
if to.get("diffYi") is not None:
    _tline += f"，{'放量' if to['diffYi'] >= 0 else '缩量'} {abs(to['diffYi']):,} 亿"
if to.get("predWan") is not None:
    _tline += f"；预计全天 {to['predWan']:.2f} 万亿"
lines.append(_tline)
lines.append(f"- **宽基 ETF 合计主力净额**：{kpi['etf']['v']:+.1f} 亿（{kpi['etf']['hint']} {kpi['etf']['chg']:+.1f}{kpi['etf']['chgUnit']}）")
lines.append(f"- **融资净买入**：{kpi['margin']['v']:+.1f} 亿（{kpi['margin']['hint']}）")
lines.append(f"- **主力合力评分**：{kpi['score']['v']} 分（{kpi['score']['level']}）")

mood = today.get("mood")
if mood:
    lines.append("")
    lines.append("## 情绪温度")
    lines.append("")
    lines.append(f"- **情绪温度**：{mood['temp']} 分（{mood['level']}）")
    lines.append(f"- 涨停 {mood['zt']} 家 · 跌停 {mood['dt']} 家 · 炸板 {mood['zb']} 家（炸板率 {mood['zbRate']:.1f}%）")
    lines.append(f"- 最高连板 {mood['maxLbc']} 板 · ≥2板 {mood['lb2']} 家 · ≥3板 {mood['lb3']} 家 · 涨停封板资金 {mood['fundYi']:.1f} 亿")
    if mood.get("adv") is not None:
        lines.append(f"- 涨跌家数（沪深）：{mood['adv']} 涨 / {mood['dec']} 跌，上涨占比 {mood['upRatio']:.1f}%")
    if mood.get("yztAvg") is not None:
        lines.append(f"- 昨日涨停股今日表现：平均 {mood['yztAvg']:+.2f}%，红盘率 {mood['yztRed']:.1f}%（{mood['yztN']} 只）")

fd = today.get("flowDaily") or []
if fd:
    last = fd[-1]
    lines.append("")
    lines.append("## 大盘主力资金（两市合计）")
    lines.append("")
    lines.append(f"- 今日主力净额：{last['main']:+.1f} 亿（超大单 {last['super']:+.1f} 亿）")
    prev = fd[-2] if len(fd) >= 2 else None
    if prev:
        lines.append(f"- 前一交易日主力净额：{prev['main']:+.1f} 亿")

stocks = today.get("stocks")
if stocks:
    lines.append("")
    lines.append("## 个股主力净额 Top5（今日）")
    lines.append("")
    lines.append("**净流入**：" + "、".join(f"{s['name']} {s['net']:+.1f}亿" for s in stocks["in"][:5]))
    lines.append("")
    lines.append("**净流出**：" + "、".join(f"{s['name']} {s['net']:+.1f}亿" for s in stocks["out"][:5]))

lines.append("")
lines.append("## 四维合力（今日口径）")
lines.append("")
lines.append("| 维度 | 评分 | 信号 |")
lines.append("|---|---|---|")
for d in today["dims"]:
    lines.append(f"| {d['name']} | {d['score']:.0f} | {sig_of(d['score'])} |")

lines.append("")
lines.append("## 宽基 ETF 资金（今日）")
lines.append("")
lines.append("| ETF | 代码 | 主力净额(亿) | 份额变化(亿份) | 溢价率 | 规模(亿) |")
lines.append("|---|---|---|---|---|---|")
for e in today["etf"]:
    lines.append(f"| {e['name']} | {e['code']} | {e['net']:+.1f} | {e['share']:+.2f} | {e['premium']:+.2f}% | {e['aum']:.0f} |")

lines.append("")
lines.append("## 板块大单净流入（今日）")
lines.append("")
lines.append("| 板块 | 涨跌幅 | 大单净额(亿) | 超大单净额(亿) | 量比 |")
lines.append("|---|---|---|---|---|")
for s in today["sectors"]:
    lines.append(f"| {s['name']} | {s['chg']:+.2f}% | {s['big']:+.1f} | {s['margin']:+.1f} | {s['vol']:.2f} |")

lines.append("")
lines.append(f"## 龙虎榜资金结构（{snap['lhb_date']}）")
lines.append("")
lines.append("| 席位类型 | 买入(亿) | 卖出(亿) | 净额(亿) |")
lines.append("|---|---|---|---|")
nets = {}
for b in today["lhb"]:
    net = b["buy"] - b["sell"]
    nets[b["name"]] = net
    lines.append(f"| {b['name']} | {b['buy']:.1f} | {b['sell']:.1f} | {net:+.1f} |")
inst = nets.get("机构专用席位", 0.0)
hot = nets.get("一线游资·营业部合计", 0.0)
retail = nets.get("拉萨天团（散户）", 0.0)
if inst > 0 and hot > 0:
    verdict = "机构与游资同向净买入，资金结构偏健康。"
elif inst < 0 and hot < 0:
    verdict = "机构与游资同向净卖出，短线风险偏好回落。"
elif inst > 0 > hot:
    verdict = "机构在买、游资在卖，长钱与短钱分歧。"
elif hot > 0 > inst:
    verdict = "游资活跃但机构在减仓，题材行情需防持续性不足。"
else:
    verdict = "各席位方向分化不明显。"
lines.append("")
lines.append(f"> 机构净{inst:+.1f}亿、游资净{hot:+.1f}亿、散户（拉萨）净{retail:+.1f}亿。{verdict}")
lines.append("")
lines.append("---")
lines.append("说明：ETF 净额＝主力净额口径；两融数据 T+1；龙虎榜为最近交易日。本日报由 GitHub Actions 自动生成，不构成投资建议。")
lines.append("")

rep_dir.mkdir(parents=True, exist_ok=True)
out = rep_dir / f"{date}.md"
out.write_text("\n".join(lines), encoding="utf-8")

idx = rep_dir / "index.md"
entry = f"- [{date} 收盘日报]({date}.md)"
if idx.exists():
    old = [l for l in idx.read_text(encoding="utf-8").splitlines()
           if l.strip() and not l.startswith("#") and date not in l]
else:
    old = []
idx.write_text("# 收盘日报存档\n\n" + "\n".join([entry] + old) + "\n", encoding="utf-8")
print(f"wrote {out} and updated {idx}")
