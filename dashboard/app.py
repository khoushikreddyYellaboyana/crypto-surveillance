"""Live dashboard for the surveillance pipeline (reads the SQLite DB written by the sink).

Tab 1 – Business overview : what is happening in the market, how risky each asset is right now,
                            and what the business should DO about each alert.
Tab 2 – Pipeline health   : throughput, latency, lag, data integrity (duplicates, late data,
                            backfill) and a recovery log showing restarts and how data was recovered.

Run:  streamlit run dashboard/app.py
"""
import json
import os
import sqlite3
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

DB = os.getenv("SQLITE_PATH", "data/surveillance.db")
TZ = os.getenv("DISPLAY_TZ", "Europe/Berlin")
st.set_page_config(page_title="Crypto Market Surveillance", page_icon="📈", layout="wide")

# What every alert means for the business, who owns it and what they should do.
PLAYBOOK = {
    "WHALE_TRADE": {
        "label": "🐋 Whale trade",
        "meaning": "One unusually large order hit the market.",
        "why": "Large players can move the price; possible manipulation or front-running; liquidity risk.",
        "owner": "Compliance / Surveillance",
        "action": "Review the order and the account behind it; check price impact and related orders.",
    },
    "PRICE_SPIKE": {
        "label": "📉 Price spike",
        "meaning": "Price crashed or pumped within seconds.",
        "why": "Customers may be liquidated or get bad fills; typical of pump-and-dump schemes.",
        "owner": "Risk management",
        "action": "Check margin positions and limits; consider a circuit breaker; find the cause.",
    },
    "VOLUME_BURST": {
        "label": "🤖 Volume burst",
        "meaning": "Far more trades than normal in a few seconds.",
        "why": "Sign of bot activity, wash trading (fake volume) or breaking news.",
        "owner": "Compliance / Surveillance",
        "action": "Check whether the same accounts trade with each other; check the news.",
    },
    "EXCHANGE_DIVERGENCE": {
        "label": "⚖️ Exchange price gap",
        "meaning": "The same coin trades at different prices on Coinbase and Binance.",
        "why": "Arbitrage opportunity for the trading desk – or a broken / manipulated price feed.",
        "owner": "Trading desk / Data ops",
        "action": "Evaluate the arbitrage (buy where cheap, sell where expensive); verify feed health.",
    },
}
SEV_WEIGHT = {"high": 3, "medium": 2, "low": 1}


def q(sql, params=()):
    if not os.path.exists(DB):
        return pd.DataFrame()
    try:
        with sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10) as con:
            return pd.read_sql_query(sql, con, params=params)
    except Exception:
        return pd.DataFrame()


def local(ms):
    return pd.to_datetime(ms, unit="ms", utc=True).dt.tz_convert(TZ).dt.tz_localize(None)


def usd(x):
    if x >= 1e9:
        return f"${x / 1e9:,.2f} B"
    if x >= 1e6:
        return f"${x / 1e6:,.2f} M"
    if x >= 1e3:
        return f"${x / 1e3:,.1f} K"
    return f"${x:,.2f}"


def price(x):
    if x is None or pd.isna(x):
        return "–"
    return f"${x:,.2f}" if x >= 100 else f"${x:,.4f}"


def risk_level(score):
    return "🔴 High" if score >= 6 else "🟡 Elevated" if score >= 2 else "🟢 Normal"


def metrics(component, since):
    df = q("SELECT instance, ts_ms, payload FROM metrics WHERE component=? AND ts_ms>=? ORDER BY ts_ms",
           (component, since))
    if df.empty:
        return pd.DataFrame()
    return pd.DataFrame([json.loads(p) for p in df["payload"]])


st.title("📈 Real-Time Crypto Market Surveillance")
st.caption("Live trades from Coinbase + Binance → Apache Kafka → stream processor → alerts. "
           "Updates every 3 seconds.")
live = st.sidebar.toggle("🔴 Live auto-refresh", value=True,
                         help="Switch OFF to freeze the page – e.g. to hover over or zoom into a chart. "
                              "Switch ON again to continue live updates.")
REFRESH = 3 if live else None
if not live:
    st.sidebar.info("⏸ Paused – the page is frozen. Switch the toggle on to resume live updates.")
minutes = st.sidebar.slider("Look-back window (minutes)", 5, 240, 30)
st.sidebar.markdown("**Risk score** = alerts in the window weighted by severity (high 3, medium 2, low 1). "
                    "🟢 Normal < 2 · 🟡 Elevated 2–5 · 🔴 High ≥ 6")
with st.sidebar.expander("What each alert means"):
    for r, p in PLAYBOOK.items():
        st.markdown(f"**{p['label']}** – {p['meaning']}  \n*Why it matters:* {p['why']}  \n"
                    f"*Owner:* {p['owner']}  \n*Action:* {p['action']}")

tab_biz, tab_ops = st.tabs(["📊 Business overview", "⚙️ Pipeline health & recovery"])


# =============================================================================== BUSINESS VIEW
@st.fragment(run_every=REFRESH)
def business():
    now = int(time.time() * 1000)
    since = now - minutes * 60_000
    bars = q("SELECT * FROM bars WHERE start_ms>=? ORDER BY start_ms", (since,))
    alerts = q("SELECT * FROM alerts WHERE event_ts_ms>=? ORDER BY event_ts_ms DESC", (since,))
    if bars.empty:
        st.info("Waiting for market data… (the first 5-second bars appear ~10 s after start-up)")
        return

    # ---------- per-asset market overview ----------------------------------------------------
    rows = []
    for sym, g in bars.groupby("symbol"):
        last = g.sort_values("start_ms").groupby("exchange").tail(1).set_index("exchange")
        first = g.sort_values("start_ms").groupby("exchange").head(1).set_index("exchange")
        px_cb = last["close"].get("coinbase")
        px_bn = last["close"].get("binance")
        ref = px_cb if px_cb is not None else px_bn
        ref_open = first["open"].get("coinbase", first["open"].get("binance"))
        vol_usd = g["notional"].sum()
        buy_share = g["buy_volume"].sum() / g["volume"].sum() * 100 if g["volume"].sum() else 50
        spread = (px_bn - px_cb) / ((px_bn + px_cb) / 2) * 1e4 if px_cb and px_bn else None
        a = alerts[alerts["symbol"] == sym] if not alerts.empty else alerts
        score = int(a["severity"].map(SEV_WEIGHT).sum()) if not a.empty else 0
        rows.append({
            "Asset": sym.split("-")[0], "Price (USD)": ref,
            "Change": (ref / ref_open - 1) * 100 if ref and ref_open else 0.0,
            "Coinbase": px_cb, "Binance": px_bn,
            "Gap (bps)": spread, "Volume (USD)": vol_usd, "Buy pressure %": buy_share,
            "Trades": int(g["trades"].sum()), "Alerts": len(a), "Risk score": score,
            "Risk": risk_level(score),
        })
    ov = pd.DataFrame(rows).sort_values(["Risk score", "Volume (USD)"], ascending=False)

    # ---------- headline KPIs -----------------------------------------------------------------
    whales = alerts[alerts["rule"] == "WHALE_TRADE"] if not alerts.empty else alerts
    high = int((alerts["severity"] == "high").sum()) if not alerts.empty else 0
    tot_buy = bars["buy_volume"].mul(bars["vwap"].fillna(bars["close"])).sum()
    tot = bars["notional"].sum()
    buy_pct = tot_buy / tot * 100 if tot else 50
    top = ov.iloc[0]
    gaps = ov.dropna(subset=["Gap (bps)"])
    best = gaps.loc[gaps["Gap (bps)"].abs().idxmax()] if not gaps.empty else None

    k = st.columns(6)
    k[0].metric("Volume monitored", usd(tot), help="USD value of all trades seen in the window")
    k[1].metric("Suspicious events", len(alerts), f"{high} high severity" if high else None,
                delta_color="inverse")
    k[2].metric("Whale value flagged", usd(whales["value"].sum()) if not whales.empty else "$0",
                help="Total USD value of the whale trades that need compliance review")
    k[3].metric("Highest-risk asset", f"{top['Asset']}" if top["Risk score"] > 0 else "none",
                top["Risk"] if top["Risk score"] > 0 else None, delta_color="off")
    k[4].metric("Arbitrage gap", f"{abs(best['Gap (bps)']):.1f} bps" if best is not None else "–",
                f"{best['Asset']} · ${abs(best['Gap (bps)']) * 10:,.0f}/$100k" if best is not None else None,
                delta_color="off",
                help="Largest Coinbase-vs-Binance price gap right now, and the gross profit (before fees) "
                     "of buying $100k on the cheap exchange and selling on the expensive one.")
    k[5].metric("Sentiment", "Buyers" if buy_pct >= 52 else "Sellers" if buy_pct <= 48 else "Balanced",
                f"{buy_pct:.0f}% buying", delta_color="off",
                help="Share of traded volume where the buyer was the aggressor")

    # ---------- market overview table + risk chart ---------------------------------------------
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Market overview & risk by asset")
        show = ov.copy()
        for c in ("Price (USD)", "Coinbase", "Binance"):
            show[c] = show[c].map(price)
        show["Volume (USD)"] = show["Volume (USD)"].map(usd)
        show = show[["Asset", "Risk", "Price (USD)", "Change", "Volume (USD)", "Buy pressure %", "Alerts",
                     "Risk score", "Coinbase", "Binance", "Gap (bps)", "Trades"]]
        st.dataframe(
            show, hide_index=True, use_container_width=True,
            column_config={
                "Change": st.column_config.NumberColumn("Change %", format="%+.2f%%"),
                "Gap (bps)": st.column_config.NumberColumn(format="%+.1f",
                                                           help="Binance vs Coinbase, basis points (0.01%)"),
                "Buy pressure %": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.0f%%"),
                "Risk score": st.column_config.NumberColumn(help="severity-weighted alerts in the window"),
            })
    with right:
        st.subheader("What is driving the risk")
        if alerts.empty:
            st.success("No suspicious activity in this window – all assets normal.")
        else:
            a = alerts.copy()
            a["Asset"] = a["symbol"].str.split("-").str[0]
            a["Alert"] = a["rule"].map(lambda r: PLAYBOOK[r]["label"])
            a["Points"] = a["severity"].map(SEV_WEIGHT)
            agg = a.groupby(["Asset", "Alert"], as_index=False)["Points"].sum()
            fig = px.bar(agg, x="Points", y="Asset", color="Alert", orientation="h", height=300)
            fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), legend=dict(orientation="h", y=-0.25),
                              yaxis=dict(categoryorder="total ascending"), xaxis_title="risk points")
            st.plotly_chart(fig, use_container_width=True, key="risk_chart")

    # ---------- action list ------------------------------------------------------------------------
    st.subheader("🚨 Alerts to act on")
    if alerts.empty:
        st.info("No alerts in this window.")
    else:
        sev_filter = st.multiselect("Severity", ["high", "medium", "low"], ["high", "medium", "low"],
                                    key="sev")
        a = alerts[alerts["severity"].isin(sev_filter)].head(60).copy()
        a["Time"] = local(a["event_ts_ms"]).dt.strftime("%H:%M:%S")
        a["Alert"] = a["rule"].map(lambda r: PLAYBOOK[r]["label"])
        a["Severity"] = a["severity"].map({"high": "🔴 high", "medium": "🟠 medium", "low": "🟡 low"})
        a["Asset"] = a["symbol"]
        a["What happened"] = a["message"]
        a["Owner"] = a["rule"].map(lambda r: PLAYBOOK[r]["owner"])
        a["Recommended action"] = a["rule"].map(lambda r: PLAYBOOK[r]["action"])
        a["Detected after"] = (a["detected_ts_ms"] - a["event_ts_ms"]).map(
            lambda v: f"{v / 1000:.1f} s" if v < 60_000 else f"{v / 60_000:.0f} min (recovered)")
        st.dataframe(a[["Time", "Severity", "Alert", "Asset", "What happened", "Recommended action", "Owner",
                        "Detected after"]], hide_index=True, use_container_width=True, height=330,
                     column_config={"What happened": st.column_config.TextColumn(width="large"),
                                    "Recommended action": st.column_config.TextColumn(width="large")})

    # ---------- price chart for one asset ----------------------------------------------------------
    syms = sorted(bars["symbol"].unique())
    symbol = st.selectbox("Price detail for", syms, index=syms.index("BTC-USD") if "BTC-USD" in syms else 0,
                          key="sym")
    g = bars[bars["symbol"] == symbol].copy()
    g["time"] = local(g["start_ms"])
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.03)
    for exch, e in g.groupby("exchange"):
        fig.add_trace(go.Scatter(x=e["time"], y=e["close"], name=f"{exch} price", mode="lines"), 1, 1)
        fig.add_trace(go.Bar(x=e["time"], y=e["notional"], name=f"{exch} USD volume", opacity=0.6), 2, 1)
    sa = alerts[alerts["symbol"] == symbol] if not alerts.empty else alerts
    for _, r in sa.iterrows():
        fig.add_vline(x=local(pd.Series([r["event_ts_ms"]])).iloc[0], line_dash="dot",
                      line_color="red" if r["severity"] == "high" else "orange", opacity=0.5)
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10), legend=dict(orientation="h"),
                      uirevision=symbol,      # keep zoom / pan between live refreshes
                      barmode="group")
    st.plotly_chart(fig, use_container_width=True, key="price_chart")
    st.caption("Tip: switch off *Live auto-refresh* in the sidebar to hover over the chart or zoom in. "
               "Dotted lines = alerts (red = high severity).")


# =============================================================================== OPERATIONS VIEW
@st.fragment(run_every=REFRESH)
def operations():
    now = int(time.time() * 1000)
    since = now - minutes * 60_000
    prod, proc, sink = metrics("producer", since), metrics("processor", since), metrics("sink", since)

    def recent(df, secs=7):
        return df[df["ts_ms"] >= df["ts_ms"].max() - secs * 1000] if not df.empty else df

    def live(df, secs=15):     # only heartbeats from the last few seconds count as "now"
        return df[df["ts_ms"] >= now - secs * 1000] if not df.empty else df

    pr, pc, sk = live(prod), live(proc), live(sink)
    flowing = not pc.empty and pc["msgs"].sum() > 0
    k = st.columns(6)
    k[0].metric("Ingest rate", f"{pr['msgs_per_s'].sum():,.0f} msg/s" if not pr.empty else "–")
    k[1].metric("Processing rate", f"{pc['msgs_per_s'].sum():,.0f} msg/s" if not pc.empty else "–")
    k[2].metric("End-to-end latency p95", f"{pc['latency_e2e_ms_p95'].max():,.0f} ms" if flowing else "–",
                help="exchange trade time → processed by our pipeline")
    k[3].metric("Kafka hop p95", f"{pc['latency_kafka_ms_p95'].max():,.0f} ms" if flowing else "–",
                help="producer received → processor done")
    k[4].metric("Consumer lag (processor / sink)",
                f"{int(pc['consumer_lag'].sum()) if not pc.empty else 0:,} / "
                f"{int(sk['consumer_lag'].sum()) if not sk.empty else 0:,}")
    k[5].metric("Processor instances", pc["instance"].nunique() if not pc.empty else "–")

    # ---------- data integrity -----------------------------------------------------------------
    st.subheader("Data integrity")
    dup_proc = int(proc["duplicates"].sum()) if not proc.empty else 0
    dup_prod = int(prod.groupby("instance")["skipped_duplicates_total"].max().sum()) \
        if not prod.empty and "skipped_duplicates_total" in prod else 0
    backfilled = int(prod.groupby("instance")["backfilled_total"].max().sum()) \
        if not prod.empty and "backfilled_total" in prod else 0
    late = int(proc["late_events"].sum()) if not proc.empty else 0
    corr = int(proc["bar_corrections"].sum()) if not proc.empty and "bar_corrections" in proc else 0
    gaps = int(recent(prod)["feed_gaps_total"].sum()) if not prod.empty else 0
    invalid = int(proc["invalid"].sum()) if not proc.empty else 0
    d = st.columns(6)
    d[0].metric("Duplicates blocked", f"{dup_proc + dup_prod:,}",
                help="Processor dedup on (exchange, symbol, trade_id) + producer duplicate filter")
    d[1].metric("Missed trades recovered", f"{backfilled:,}",
                help="Trades fetched from the exchange REST API after a producer restart / reconnect")
    d[2].metric("Late trades corrected", f"{late:,}", f"{corr:,} bars corrected" if corr else None,
                delta_color="off")
    d[3].metric("Unrecovered feed gaps", f"{gaps:,}", help="trade ids skipped by the exchange and not backfilled")
    d[4].metric("Invalid → dead-letter queue", f"{invalid:,}")
    d[5].metric("Processed in window", f"{int(proc['msgs'].sum()) if not proc.empty else 0:,}")

    # ---------- component status + recovery log ----------------------------------------------------
    c1, c2 = st.columns([2, 3])
    with c1:
        st.subheader("Component status")
        rows = []
        for name, df in (("producer", prod), ("processor", proc), ("sink", sink)):
            if df.empty:
                rows.append({"Component": name, "Instance": "–", "Status": "⚪ no data"})
                continue
            last = df.sort_values("ts_ms").groupby("instance").tail(1).sort_values("ts_ms", ascending=False)
            for i, (_, lastm) in enumerate(last.iterrows()):
                age = (now - lastm["ts_ms"]) / 1000
                # the newest instance is ALWAYS shown (so a stopped producer is visible as down);
                # older instances silent for > 5 min are leftovers (old version / scaled down) – hide
                if i > 0 and age > 300:
                    continue
                up = lastm.get("started_ms")
                rows.append({"Component": name, "Instance": str(lastm["instance"])[:12],
                             "Status": "🟢 running" if age < 15 else "🔴 down / stopped",
                             "Last heartbeat": f"{age:.0f} s ago",
                             "Up since": local(pd.Series([up])).iloc[0].strftime("%H:%M:%S")
                             if pd.notna(up) else "–"})
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    with c2:
        st.subheader("Recovery log")
        ev = []
        if not proc.empty and "recovery" in proc:
            for _, r in proc.iterrows():
                recs = r["recovery"] if isinstance(r["recovery"], list) else []   # old rows have none
                for rec in recs:
                    ev.append({"ts": r["ts_ms"], "Component": "processor",
                               "What happened": f"(Re)started – rebuilt state from {rec['restored_trades']:,} "
                                                f"already-processed trades in {rec['restore_ms']} ms, "
                                                f"resumed partitions {rec['partitions']} at committed offsets"})
        if not prod.empty and "started_ms" in prod:
            for (inst, st_ms), g in prod.groupby(["instance", "started_ms"]):
                res = g["resumed_from_checkpoint_ms"].max() if "resumed_from_checkpoint_ms" in g else 0
                res = res if pd.notna(res) else 0
                bf = g["backfilled_total"].max() if "backfilled_total" in g else 0
                bf = int(bf) if pd.notna(bf) else 0
                txt = (f"(Re)started – resumed from checkpoint saved {(st_ms - res) / 1000:,.0f} s earlier, "
                       f"backfilled {bf:,} missed trades" if res else "Started fresh (no checkpoint)")
                ev.append({"ts": st_ms, "Component": "producer", "What happened": txt})
        if not sink.empty:
            for (inst, st_ms), g in sink.groupby(["instance", "started_ms"]):
                ev.append({"ts": st_ms, "Component": "sink",
                           "What happened": f"(Re)started – resumed from committed offsets, caught up "
                                            f"{int(g['consumer_lag'].max()):,} buffered records"})
        if ev:
            e = pd.DataFrame(ev).sort_values("ts", ascending=False)
            e["Time"] = local(e["ts"]).dt.strftime("%H:%M:%S")
            st.dataframe(e[["Time", "Component", "What happened"]], hide_index=True,
                         use_container_width=True, height=250)
        else:
            st.info("No restarts in this window.")

    # ---------- charts -------------------------------------------------------------------------------
    if not proc.empty:
        df = proc.copy()
        df["time"] = local(df["ts_ms"])
        agg = df.groupby(pd.Grouper(key="time", freq="5s")).agg(
            msgs_per_s=("msgs_per_s", "sum"), p50=("latency_e2e_ms_p50", "max"),
            p95=("latency_e2e_ms_p95", "max"), p99=("latency_e2e_ms_p99", "max"),
            lag=("consumer_lag", "sum"), duplicates=("duplicates", "sum")).dropna()
        a, b, c = st.columns(3)
        a.caption("Throughput (msg/s)")
        a.line_chart(agg[["msgs_per_s"]], height=220)
        b.caption("End-to-end latency (ms)")
        b.line_chart(agg[["p50", "p95", "p99"]], height=220)
        c.caption("Consumer lag & duplicates blocked")
        c.line_chart(agg[["lag", "duplicates"]], height=220)


with tab_biz:
    business()
with tab_ops:
    operations()
