"""
╔══════════════════════════════════════════════════════╗
║       NIFTY50 ZONE ASSISTANT BOT — V3               ║
║       Smart Assistant | Manual Trade Confirm         ║
║       Output: BUY CE / BUY PE / WAIT                ║
║                                                        ║
║  V3 FIXES (on top of V2):                            ║
║  FIX 4  : tg_send() silent-failure + HTML escaping   ║
║  FIX 5  : closing_job() order (EOD summary before    ║
║           OPEN→EOD_OPEN rename, else "Open:0" bug)   ║
║  FIX 6  : OI snapshot decoupled from zone-touch gate ║
║           (saved every 5min tick → history not N/A)  ║
║  FIX 7  : Nearest expiry fetched from Upstox API,    ║
║           cached 6hr (holiday-shift safe), with      ║
║           hardcoded-Tuesday fallback                 ║
║  FIX 8  : Model split — Sonnet for first analysis,   ║
║           Haiku for zone decision. max_tokens split  ║
║           (2500 / 1000) — was truncating zones JSON  ║
║  FIX 9  : Dynamic zone-touch buffer (15% of zone     ║
║           width, min 10) replaces flat ZONE_NEAR_PTS ║
║  FIX 10 : Wick threshold 0.25→0.30 for support/      ║
║           resistance rejection (kept body>=-3 —      ║
║           strict body>=0 would reject real hammers)  ║
║  FIX 11 : Liquidity-fight module now conditional —   ║
║           Python detects dominance candle + 50%      ║
║           reclaim, only injects module when found    ║
║  FIX 12 : get_context_zones() — adds factual S/R     ║
║           range + position% to prompt, no AI-biasing ║
║           "lean" language                            ║
║  FIX 18 : Logging timestamps now IST (was server     ║
║           local/UTC) — ISTFormatter                  ║
║  FIX 19 : Restart-persistence — main() no longer     ║
║           blindly re-runs morning_job() on restart.  ║
║           If today's zones+context already in Redis, ║
║           recover them instead (Telegram alert +     ║
║           recovery_log entry, shown in EOD summary)  ║
║  FIX 20 : Structure-shift flag — Python tracks zone  ║
║           flips in real time (flip_log), flags 2+    ║
║           same-direction flips within 60min to the   ║
║           AI without waiting for next hourly bias    ║
║  FIX 21 : ZONE_DECISION_SYSTEM now tells the AI the  ║
║           [MORNING CONTEXT] bias can be ~58min stale ║
║           — weigh current candles over old bias      ║
║  FIX 22 : PCR-reversal confluence module — injected  ║
║           only for reversal-type events, applies     ║
║           PCR>1.5@support / PCR<0.7@resistance rule  ║
║  FIX 23 : AI reason/risk_note/confirmations now      ║
║           logged directly (Koyeb logs), not just     ║
║           buried in Redis ai_raw_log                 ║
╚══════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════
# SECTION 1: IMPORTS + CONFIG
# ═══════════════════════════════════════════════════════

import os
import json
import math
import time
import html
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
import pytz
from dotenv import load_dotenv
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

# ── Timezone (moved above logging setup — FIX 18 needs this first) ──
IST = pytz.timezone("Asia/Kolkata")


# ── Logging (FIX 18: IST timestamps, not server-local/UTC) ─────────
class ISTFormatter(logging.Formatter):
    """
    Koyeb (and most hosts) run containers on UTC. Default logging.Formatter
    uses time.localtime() → UTC on the server → every log line was showing
    UTC time, ~5.5hr behind actual IST market time, making log correlation
    with Telegram alerts / candle times confusing.
    This formatter converts record.created (a UTC epoch timestamp) to IST
    explicitly, using the same `IST` pytz object used everywhere else in
    the bot — no separate zoneinfo import, no second source of truth.
    """
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=pytz.utc).astimezone(IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


_log_handler = logging.StreamHandler()
_log_handler.setFormatter(
    ISTFormatter("%(asctime)s IST | %(levelname)s | %(message)s")
)
logging.basicConfig(level=logging.INFO, handlers=[_log_handler])
log = logging.getLogger("ZoneBot")

# ── ENV Variables ─────────────────────────────────────
UPSTOX_TOKEN  = os.getenv("UPSTOX_ANALYTICS_TOKEN", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TG_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")

# ── Upstox ───────────────────────────────────────────
NIFTY_KEY     = "NSE_INDEX|Nifty 50"
NIFTY_KEY_ENC = quote("NSE_INDEX|Nifty 50")
UPSTOX_V2     = "https://api.upstox.com/v2"
UPSTOX_V3     = "https://api.upstox.com/v3"
UPSTOX_HDRS   = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json"
}

# ── Bot Settings ──────────────────────────────────────
# NOTE (FIX 9): flat ZONE_NEAR_PTS removed — replaced by zone_buffer()
# dynamic buffer (15% of zone width, min 10pts). See SECTION 4.
WAIT_COOLDOWN_MIN   = int(os.getenv("WAIT_COOLDOWN_MIN",   "25"))
SIGNAL_COOLDOWN_MIN = int(os.getenv("SIGNAL_COOLDOWN_MIN", "30"))
MAX_ZONES           = int(os.getenv("MAX_ZONES",            "7"))
MAX_AI_RAW_LOG      = int(os.getenv("MAX_AI_RAW_LOG",     "100"))
MIN_CONFIRMATIONS   = int(os.getenv("MIN_CONFIRMATIONS",    "2"))

# ── Candle Trigger Engine Constants ───────────────────
ZONE_TOUCH_BUFFER    = int(os.getenv("ZONE_TOUCH_BUFFER",   "12"))
SIDEWAYS_CANDLE_CNT  = int(os.getenv("SIDEWAYS_CANDLE_CNT",  "4"))
SIDEWAYS_MAX_RANGE   = int(os.getenv("SIDEWAYS_MAX_RANGE",  "30"))
BREAKOUT_BUFFER      = int(os.getenv("BREAKOUT_BUFFER",     "10"))
LAST_AI_CALL_MINUTE  = int(os.getenv("LAST_AI_CALL_MINUTE",  "5"))
# FIX 10: wick threshold raised 0.25 → 0.30 (used in detect_candle_event)
REJECTION_WICK_RATIO = float(os.getenv("REJECTION_WICK_RATIO", "0.30"))
# Minimum range (pts) for a candle to count as a "dominance candle"
DOMINANCE_MIN_RANGE  = int(os.getenv("DOMINANCE_MIN_RANGE", "15"))
DOMINANCE_LOOKBACK   = int(os.getenv("DOMINANCE_LOOKBACK",  "15"))

# ── AI Models (FIX 8: split by task) ──────────────────
ANALYSIS_MODEL            = os.getenv("ANALYSIS_MODEL", "claude-sonnet-4-6")
DECISION_MODEL            = os.getenv("DECISION_MODEL", "claude-haiku-4-5-20251001")
FIRST_ANALYSIS_MAX_TOKENS = int(os.getenv("FIRST_ANALYSIS_MAX_TOKENS", "2500"))
ZONE_DECISION_MAX_TOKENS  = int(os.getenv("ZONE_DECISION_MAX_TOKENS",  "1000"))


# ── Small helper (FIX 4) ──────────────────────────────
def esc(text):
    """HTML-escape any AI-generated free text before sending to Telegram
    with parse_mode=HTML. Prevents silent send failures when AI text
    contains '<', '>' or '&' (e.g. 'price < zone')."""
    if text is None:
        return text
    return html.escape(str(text), quote=False)


# ═══════════════════════════════════════════════════════
# SECTION 2: UPSTOX DATA FETCH
# ═══════════════════════════════════════════════════════

def upstox_get(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UPSTOX_HDRS, timeout=15)
            if r.status_code == 200:
                d = r.json()
                if d.get("status") == "success":
                    return d.get("data", {})
            elif r.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception as e:
            log.error(f"API error: {e}")
        time.sleep(1)
    return None


def get_ltp():
    url  = f"{UPSTOX_V3}/market-quote/ltp?instrument_key={NIFTY_KEY_ENC}"
    data = upstox_get(url)
    if data:
        first = next(iter(data.values()), None)
        if first and "last_price" in first:
            return float(first["last_price"])
    log.error("LTP fetch failed")
    return None


def fetch_historical(unit, interval, candles_needed):
    """Fetch historical OHLC — includes yesterday + older data"""
    now     = datetime.now(IST)
    to_date = now.strftime("%Y-%m-%d")

    if unit == "days":
        from_date = (now - timedelta(days=candles_needed + 10)).strftime("%Y-%m-%d")
    elif unit == "hours":
        from_date = (now - timedelta(days=15)).strftime("%Y-%m-%d")
    else:  # minutes
        from_date = (now - timedelta(days=3)).strftime("%Y-%m-%d")

    url  = (f"{UPSTOX_V3}/historical-candle/"
            f"{NIFTY_KEY_ENC}/{unit}/{interval}/{to_date}/{from_date}")
    data = upstox_get(url)
    if not data or "candles" not in data:
        return pd.DataFrame()

    df = pd.DataFrame(
        data["candles"],
        columns=["ts", "o", "h", "l", "c", "v", "oi"]
    )
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o","h","l","c"]] = df[["o","h","l","c"]].round(0).astype(int)
    return df.tail(candles_needed)


def fetch_intraday(unit, interval):
    """Fetch today's intraday OHLC"""
    url  = (f"{UPSTOX_V3}/historical-candle/intraday/"
            f"{NIFTY_KEY_ENC}/{unit}/{interval}")
    data = upstox_get(url)
    if not data or "candles" not in data:
        return pd.DataFrame()

    df = pd.DataFrame(
        data["candles"],
        columns=["ts", "o", "h", "l", "c", "v", "oi"]
    )
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o","h","l","c"]] = df[["o","h","l","c"]].round(0).astype(int)
    return df


def fetch_all_data():
    """
    Fetch all TF data.
    Works for any start time — 9:20 or 12:00.
    15M/5M historical gives yesterday + today candles.
    """
    log.info("Fetching all TF data...")

    daily  = fetch_historical("days",    1, 15)
    hourly = fetch_historical("hours",   1, 30)
    m15_hist = fetch_historical("minutes", 15, 60)

    m5 = fetch_intraday("minutes", 5)
    if m5.empty or len(m5) < 10:
        log.warning("Intraday 5M empty → using historical")
        m5 = fetch_historical("minutes", 5, 80)

    return {
        "daily":  daily,
        "hourly": hourly,
        "m15":    m15_hist,
        "m5":     m5
    }


def fetch_zone_decision_data():
    """Light fetch for zone decision — 15M + 5M only"""
    m15 = fetch_intraday("minutes", 15)
    m5  = fetch_intraday("minutes", 5)
    if m15.empty:
        m15 = fetch_historical("minutes", 15, 30)
    if m5.empty:
        m5  = fetch_historical("minutes", 5,  40)
    return {"m15": m15, "m5": m5}


# ═══════════════════════════════════════════════════════
# SECTION 2.5: OI + PCR TRACKING
# ═══════════════════════════════════════════════════════

def get_weekly_expiry_fallback():
    """
    FIX 7: This is now only a FALLBACK if the live expiry API fails.
    Hardcoded 'next Tuesday' logic breaks on holiday shifts
    (NSE moves expiry to Wed/other day around holidays, budget day etc).
    """
    now = datetime.now(IST)
    days_to_tue = (1 - now.weekday()) % 7
    if days_to_tue == 0 and now.hour >= 15:
        days_to_tue = 7
    return (now + timedelta(days=days_to_tue)).strftime("%Y-%m-%d")


def get_nearest_expiry():
    """
    FIX 7: Fetch real expiry dates from Upstox option/contract endpoint.
    Cached 6hr (Redis/RAM) so we don't hit this every 5min tick.
    Falls back to hardcoded-Tuesday calc if API fails — this is the
    real reason OI was silently dead since day 1 (wrong expiry → empty
    chain → fetch_oi_chain() returns None → OI module never added to
    the AI prompt, not even once).
    """
    cached = _get("nearest_expiry_cache")
    if cached and cached.get("fetched_at"):
        try:
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            if fetched_at.tzinfo is None:
                fetched_at = IST.localize(fetched_at)
            if datetime.now(IST) - fetched_at < timedelta(hours=6):
                return cached["expiry"]
        except Exception:
            pass

    try:
        url  = f"{UPSTOX_V2}/option/contract?instrument_key={NIFTY_KEY_ENC}"
        data = upstox_get(url)

        expiries = set()
        if isinstance(data, list):
            for item in data:
                exp = item.get("expiry")
                if exp:
                    expiries.add(exp)
        elif isinstance(data, dict):
            # some Upstox responses nest under a list-valued key
            for v in data.values():
                if isinstance(v, list):
                    for item in v:
                        exp = item.get("expiry") if isinstance(item, dict) else None
                        if exp:
                            expiries.add(exp)

        if not expiries:
            log.warning("Expiry API returned no expiries — using fallback calc")
            return get_weekly_expiry_fallback()

        nearest = sorted(expiries)[0]
        _set("nearest_expiry_cache", {
            "expiry":     nearest,
            "fetched_at": datetime.now(IST).isoformat()
        }, ttl=6 * 3600 + 300)
        log.info(f"✅ Nearest expiry fetched: {nearest}")
        return nearest

    except Exception as e:
        log.error(f"get_nearest_expiry error: {e} — using fallback calc")
        return get_weekly_expiry_fallback()


def fetch_oi_chain(ltp):
    """
    Fetch ATM ± 3 strikes OI + volume from Upstox.
    Nifty strike gap = 50 pts → 7 strikes total.
    Returns compact dict or None.
    """
    try:
        atm    = round(ltp / 50) * 50
        expiry = get_nearest_expiry()           # FIX 7
        url    = (f"{UPSTOX_V2}/option/chain"
                  f"?instrument_key={NIFTY_KEY_ENC}"
                  f"&expiry_date={expiry}")

        resp = upstox_get(url)
        if not resp:
            log.warning("OI chain fetch failed")
            return None

        target = {atm + (i * 50) for i in range(-3, 4)}
        strikes = {}

        for opt in resp:
            sp = int(opt.get("strike_price", 0))
            if sp not in target:
                continue
            ce = opt.get("call_options", {}).get("market_data", {})
            pe = opt.get("put_options",  {}).get("market_data", {})
            strikes[sp] = {
                "ce_oi":  int(ce.get("oi",     0)),
                "pe_oi":  int(pe.get("oi",     0)),
                "ce_vol": int(ce.get("volume", 0)),
                "pe_vol": int(pe.get("volume", 0)),
            }

        if not strikes:
            log.warning(f"OI: no strikes found for ATM {atm}")
            return None

        tot_ce_oi  = sum(v["ce_oi"]  for v in strikes.values())
        tot_pe_oi  = sum(v["pe_oi"]  for v in strikes.values())
        tot_ce_vol = sum(v["ce_vol"] for v in strikes.values())
        tot_pe_vol = sum(v["pe_vol"] for v in strikes.values())
        pcr        = round(tot_pe_oi / tot_ce_oi, 3) if tot_ce_oi > 0 else 1.0
        ce_wall    = max(strikes, key=lambda s: strikes[s]["ce_oi"])
        pe_wall    = max(strikes, key=lambda s: strikes[s]["pe_oi"])

        return {
            "atm":         atm,
            "expiry":      expiry,
            "strikes":     strikes,
            "tot_ce_oi":   tot_ce_oi,
            "tot_pe_oi":   tot_pe_oi,
            "tot_ce_vol":  tot_ce_vol,
            "tot_pe_vol":  tot_pe_vol,
            "pcr":         pcr,
            "ce_wall":     ce_wall,
            "pe_wall":     pe_wall,
        }
    except Exception as e:
        log.error(f"fetch_oi_chain error: {e}")
        return None


def save_oi_snapshot(data):
    """Save OI snapshot keyed by current 5-min slot"""
    now  = datetime.now(IST)
    slot = now.strftime("%H:%M")
    _set(f"oi:{slot}", data, ttl=7200)
    log.info(f"OI snapshot saved: {slot} | PCR:{data.get('pcr')}")


def get_oi_snapshot(minutes_ago):
    """Retrieve OI snapshot from N minutes ago (rounded to 5min)"""
    past = datetime.now(IST) - timedelta(minutes=minutes_ago)
    rounded = (past.minute // 5) * 5
    slot = past.replace(minute=rounded, second=0).strftime("%H:%M")
    return _get(f"oi:{slot}")


def _fmt_chg(past_data, key):
    if past_data is None:
        return "N/A"
    val = past_data.get(key)
    return f"{val:+.1f}%" if val is not None else "N/A"


def _vol_label(current_total, past_5min_total, baseline_total, elapsed_min):
    """
    FIX 15: Compare INCREMENTAL volume over the last 5min against the
    day's average 5-min run-rate so far, instead of cumulative-day-volume
    vs the single near-zero 9:20AM baseline snapshot. Upstox's option
    chain `volume` field is the day's running total (confirmed via
    Upstox docs) — a cumulative-vs-morning-baseline ratio grows just
    from time passing, regardless of any real spike, so by midday it
    always reads "HIGH" even with nothing unusual happening.
    """
    if past_5min_total is None or elapsed_min is None or elapsed_min <= 0:
        return "?"
    incremental_5m = current_total - past_5min_total
    avg_5m_rate    = (current_total - baseline_total) / (elapsed_min / 5)
    if avg_5m_rate <= 0:
        return "?"
    r = incremental_5m / avg_5m_rate
    if r >= 2.0: return f"HIGH({r:.1f}x)"
    if r >= 1.0: return f"MED({r:.1f}x)"
    return f"LOW({r:.1f}x)"


def format_oi_context(current):
    """
    FIX 6: Pure formatter — takes an ALREADY-FETCHED `current` OI dict
    and turns it into the compact AI-prompt string. Does NOT fetch or
    save anything itself anymore (that's now done once per tick in
    zone_monitor_job, decoupled from zone-touch gating — see SECTION 8).
    """
    if not current:
        return None

    baseline          = _get("oi_baseline") or {}
    base_ce_vol       = baseline.get("tot_ce_vol", 0)
    base_pe_vol       = baseline.get("tot_pe_vol", 0)
    baseline_saved_at = baseline.get("saved_at")

    # FIX 15: elapsed minutes since baseline — needed for the incremental
    # volume comparison below. Floored at 5 to avoid a huge/undefined
    # rate in the first few minutes after baseline is saved.
    elapsed_min = None
    if baseline_saved_at:
        try:
            bt = datetime.fromisoformat(baseline_saved_at)
            if bt.tzinfo is None:
                bt = IST.localize(bt)
            elapsed_min = max(5, (datetime.now(IST) - bt).total_seconds() / 60)
        except Exception:
            elapsed_min = None

    past5        = get_oi_snapshot(5)
    past5_ce_vol = past5.get("tot_ce_vol") if past5 else None
    past5_pe_vol = past5.get("tot_pe_vol") if past5 else None

    chg = {}
    for mins in [5, 15, 30]:
        past = get_oi_snapshot(mins)
        if past and past.get("tot_ce_oi", 0) > 0:
            chg[mins] = {
                "ce_chg": round(
                    (current["tot_ce_oi"] - past["tot_ce_oi"])
                    / past["tot_ce_oi"] * 100, 1),
                "pe_chg": round(
                    (current["tot_pe_oi"] - past["tot_pe_oi"])
                    / past["tot_pe_oi"] * 100, 1),
                "pcr": past.get("pcr"),
            }
        else:
            chg[mins] = None

    pcr_str = (
        f"{chg[30]['pcr'] if chg[30] else 'N/A'}→"
        f"{chg[15]['pcr'] if chg[15] else 'N/A'}→"
        f"{chg[5]['pcr']  if chg[5]  else 'N/A'}→"
        f"{current['pcr']}"
    )

    ce_chg_str = (
        f"{_fmt_chg(chg[30],'ce_chg')}/"
        f"{_fmt_chg(chg[15],'ce_chg')}/"
        f"{_fmt_chg(chg[5], 'ce_chg')} (30/15/5min)"
    )
    pe_chg_str = (
        f"{_fmt_chg(chg[30],'pe_chg')}/"
        f"{_fmt_chg(chg[15],'pe_chg')}/"
        f"{_fmt_chg(chg[5], 'pe_chg')} (30/15/5min)"
    )

    cw_oi = current["strikes"].get(current["ce_wall"], {}).get("ce_oi", 0)
    pw_oi = current["strikes"].get(current["pe_wall"], {}).get("pe_oi", 0)

    return (
        f"[OI | ATM:{current['atm']} | {current.get('expiry', get_nearest_expiry())}]\n"
        f"PCR:{pcr_str}\n"
        f"CE_OI_CHG:{ce_chg_str}\n"
        f"PE_OI_CHG:{pe_chg_str}\n"
        f"CE_WALL:{current['ce_wall']}({cw_oi//100000:.1f}L) "
        f"PE_WALL:{current['pe_wall']}({pw_oi//100000:.1f}L)\n"
        f"CE_VOL:{_vol_label(current['tot_ce_vol'], past5_ce_vol, base_ce_vol, elapsed_min)} "
        f"PE_VOL:{_vol_label(current['tot_pe_vol'], past5_pe_vol, base_pe_vol, elapsed_min)} "
        f"(vs 5min-ago, relative to today's avg pace)"
    )


# ═══════════════════════════════════════════════════════
# SECTION 3: DATA COMPRESS
# ═══════════════════════════════════════════════════════

def get_base(df):
    if df.empty:
        return 24000
    return math.floor(int(df["l"].min()) / 500) * 500


def compress_ohlc(df, base, max_candles=None):
    """O H L C per line — delta encoded"""
    if df.empty:
        return "N/A"
    d = df.tail(max_candles) if max_candles else df
    lines = []
    for _, row in d.iterrows():
        lines.append(
            f"{int(row['o'])-base} {int(row['h'])-base} "
            f"{int(row['l'])-base} {int(row['c'])-base}"
        )
    return "\n".join(lines)


def compress_hl(df, base, max_candles=None):
    """H L per line — for daily structure"""
    if df.empty:
        return "N/A"
    d = df.tail(max_candles) if max_candles else df
    lines = []
    for _, row in d.iterrows():
        lines.append(f"{int(row['h'])-base} {int(row['l'])-base}")
    return "\n".join(lines)


def drop_incomplete_candle(df, interval_minutes=5, buffer_sec=10):
    """Remove last candle if still forming"""
    if df.empty or len(df) < 2:
        return df
    try:
        last_ts = pd.Timestamp(df.iloc[-1]["ts"])
        if last_ts.tzinfo is None:
            last_ts = IST.localize(last_ts.to_pydatetime())
        else:
            last_ts = last_ts.tz_convert(IST).to_pydatetime()

        complete_at = last_ts + timedelta(
            minutes=interval_minutes, seconds=buffer_sec
        )
        if datetime.now(IST) < complete_at:
            log.info(f"Dropped incomplete {interval_minutes}M candle: {last_ts.strftime('%H:%M')}")
            return df.iloc[:-1].copy()
    except Exception as e:
        log.error(f"drop_incomplete_candle: {e}")
    return df


def build_first_analysis_string(tf_data):
    """Build compressed data string for first analysis + hourly reanalysis"""
    daily  = tf_data.get("daily",  pd.DataFrame())
    hourly = tf_data.get("hourly", pd.DataFrame())
    m15    = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)
    m5     = drop_incomplete_candle(tf_data.get("m5",  pd.DataFrame()), 5)

    if daily.empty:
        return None

    d_base   = get_base(daily)
    h_base   = get_base(hourly)
    m15_base = get_base(m15)
    m5_base  = get_base(m5)

    pdc_row = None
    try:
        today   = datetime.now(IST).date()
        last_ts = pd.to_datetime(daily.iloc[-1]["ts"]).date()
        pdc_row = daily.iloc[-2] if last_ts == today else daily.iloc[-1]
    except Exception:
        pass

    pdh = int(pdc_row["h"]) if pdc_row is not None else 0
    pdl = int(pdc_row["l"]) if pdc_row is not None else 0
    pdc = int(pdc_row["c"]) if pdc_row is not None else 0
    ltp_approx = int(m5.iloc[-1]["c"]) if not m5.empty else 0

    now_str = datetime.now(IST).strftime("%d-%m-%Y %H:%M")

    return f"""=== NIFTY50 ZONE ANALYSIS | {now_str} ===

[CONTEXT - Python Calculated]
PDH:{pdh} | PDL:{pdl} | PDC:{pdc}
LTP_approx:{ltp_approx}

[DAILY - 15 candles | BASE:{d_base}]
(H L per candle | oldest→newest)
{compress_hl(daily, d_base, 15)}

[1H - 30 candles | BASE:{h_base}]
(O H L C per candle | oldest→newest)
{compress_ohlc(hourly, h_base, 30)}

[15M - last 40 candles | BASE:{m15_base}]
(O H L C | oldest→newest)
{compress_ohlc(m15, m15_base, 40)}

[5M - last 60 candles | BASE:{m5_base}]
(O H L C | oldest→newest)
{compress_ohlc(m5, m5_base, 60)}"""


def _price_context_block(context_zones):
    """
    FIX 12: Factual S/R range + position% block.
    Deliberately NO 'lean'/'bias' language here — that was Python
    pre-judging the trade and could contaminate the AI's independent
    confirmation-based reasoning. Just the raw facts.
    """
    if not context_zones:
        return ""
    sup = context_zones.get("support")
    res = context_zones.get("resistance")
    if not sup and not res:
        return ""

    sup_str = f"{sup['id']}({sup['low']}-{sup['high']})" if sup else "None"
    res_str = f"{res['id']}({res['low']}-{res['high']})" if res else "None"

    extra = ""
    width = context_zones.get("range_width")
    pos   = context_zones.get("position_pct")
    if width is not None and pos is not None:
        extra = f"\nRange width:{width}pts | Position:{pos}% (0%=at support, 100%=at resistance)"

    return f"\n\n[PRICE CONTEXT]\nNearest Support:{sup_str} | Nearest Resistance:{res_str}{extra}"


def _dominance_block(dominance):
    """FIX 11: factual dominance-candle numbers, no interpretation here."""
    if not dominance:
        return ""
    return (
        f"\n\n[DOMINANCE CANDLE]\n"
        f"Range:{dominance['low']}-{dominance['high']} ({dominance['rng']}pts) | "
        f"Side:{dominance['side']} | 50%:{dominance['mid']} | "
        f"Reclaimed:{'YES' if dominance['reclaimed'] else 'NO'}"
    )


def _structure_shift_block(shift):
    """
    FIX 20: Formats the Python-detected structure-shift fact (see
    get_structure_shift()) into a prompt block. Explicitly flags that
    [MORNING CONTEXT] Bias/Structure may be stale relative to this —
    the AI is told to weigh this real-time signal alongside candles,
    not treat the old bias as the final word.
    """
    if not shift:
        return ""
    return (
        f"\n\n[STRUCTURE SHIFT DETECTED — Python-tracked, real-time]\n"
        f"{shift['count']} zone flip(s) in the '{shift['direction'].upper()}' "
        f"direction since {shift['since']} (within the last hour).\n"
        f"NOTE: The [MORNING CONTEXT] Bias/Structure above is from the last "
        f"hourly reanalysis and may not reflect this yet — weigh this "
        f"real-time structure signal together with the current candles, "
        f"not just the (possibly stale) bias label."
    )


def build_zone_decision_string(tf_data, ltp, touched_zone, all_zones, morning_ctx,
                                oi_context=None, context_zones=None, dominance=None):
    """Build compact string for zone touch decision"""
    m15 = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)
    m5  = drop_incomplete_candle(tf_data.get("m5",  pd.DataFrame()), 5)

    base    = get_base(m5) if not m5.empty else get_base(m15)
    m15_str = compress_ohlc(m15, base, 20)
    m5_str  = compress_ohlc(m5,  base, 30)

    other = [
        f"  {z['id']}:{z['type']} {z['low']}-{z['high']} [{z['strength']}]"
        for z in all_zones if z.get("id") != touched_zone.get("id")
    ]
    other_str = "\n".join(other[:6]) if other else "None"

    now_str = datetime.now(IST).strftime("%H:%M")
    bias    = morning_ctx.get("bias", "?")
    struct  = morning_ctx.get("structure", "?")

    oi_block         = f"\n\n{oi_context}" if oi_context else ""
    price_ctx_block  = _price_context_block(context_zones)
    dominance_block  = _dominance_block(dominance)

    return f"""=== ZONE DECISION | {now_str} ===

[MORNING CONTEXT]
Bias:{bias} | Structure:{struct}
Day:{morning_ctx.get('day_type','?')}
Summary:{morning_ctx.get('summary','')}

[TOUCHED ZONE]
ID:{touched_zone.get('id')} | Type:{touched_zone.get('type')}
Range:{touched_zone.get('low')}-{touched_zone.get('high')}
Strength:{touched_zone.get('strength')}
Preferred:{touched_zone.get('preferred_action','?')}
Why:{touched_zone.get('why','')}

[OTHER ACTIVE ZONES]
{other_str}

[CURRENT LTP]
{ltp}{oi_block}{price_ctx_block}{dominance_block}

[15M - last 20 candles | BASE:{base}]
(O H L C | oldest→newest)
{m15_str}

[5M - last 30 candles | BASE:{base}]
(O H L C | oldest→newest)
{m5_str}"""


# ═══════════════════════════════════════════════════════
# SECTION 4: ZONE MANAGER (Redis)
# ═══════════════════════════════════════════════════════

_memory = {}

def _redis():
    try:
        import redis
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None

_r = _redis()
log.info("✅ Redis connected" if _r else "⚠️ RAM mode")


def _set(key, value, ttl=86400):
    v = json.dumps(value)
    if _r:
        _r.setex(key, ttl, v)
    else:
        _memory[key] = v


def _get(key):
    try:
        raw = _r.get(key) if _r else _memory.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _delete(key):
    if _r:
        _r.delete(key)
    else:
        _memory.pop(key, None)


def save_zones(zones):
    """
    FIX 19: also stamps today's IST date alongside the zones, so a
    restart can tell whether these zones belong to today or are stale
    from before a crash boundary. Without this stamp there was no way
    to distinguish "today's zones, just saved" from "yesterday's zones
    that never got flushed" on restart.
    """
    _set("zones", zones[:MAX_ZONES])
    _set("zones_date", datetime.now(IST).strftime("%Y-%m-%d"))
    log.info(f"✅ {len(zones)} zones saved")


def get_zones():
    return _get("zones") or []


def save_morning_context(ctx):
    """FIX 19: date-stamped alongside — see save_zones() note."""
    _set("morning_context", ctx)
    _set("morning_context_date", datetime.now(IST).strftime("%Y-%m-%d"))


def get_morning_context():
    return _get("morning_context")


def is_today_data_valid():
    """
    FIX 19: Restart-recovery check. Returns True only if BOTH zones and
    morning_context exist in Redis AND both were stamped with today's
    IST date. Used by main() on startup to decide: recover existing
    data (restart mid-day) vs run a fresh morning_job() (first-ever
    start, or data missing/stale from a previous day that somehow
    survived without being flushed).
    """
    today       = datetime.now(IST).strftime("%Y-%m-%d")
    zones       = get_zones()
    ctx         = get_morning_context()
    zones_date  = _get("zones_date")
    ctx_date    = _get("morning_context_date")
    return bool(zones and ctx and zones_date == today and ctx_date == today)


def save_recovery_log(entry):
    """
    FIX 19: Tracks restart-recovery events for the day so they're
    auditable later (not just a Telegram message that scrolls away) —
    surfaced in the EOD summary's "Restarts Today" section.
    """
    logs = _get("recovery_log") or []
    logs.append(entry)
    _set("recovery_log", logs)


def get_recovery_log():
    return _get("recovery_log") or []


def save_flip_event(zone_id, direction):
    """
    FIX 20: Records the moment a zone actually flips (RESISTANCE→
    FLIP_SUPPORT = 'up' break, SUPPORT→FLIP_RESISTANCE = 'down' break).
    This is Python-side and happens the instant maybe_flip_zone() fires
    in zone_monitor_job() — it does NOT wait for the next hourly
    reanalysis. Purpose: detect a same-direction structure shift across
    multiple zones within the hour, well before hourly_job() eventually
    updates morning_context['bias'] to match reality.
    """
    events = _get("flip_log") or []
    events.append({
        "time":      datetime.now(IST).strftime("%H:%M"),
        "zone_id":   zone_id,
        "direction": direction
    })
    _set("flip_log", events[-20:])  # last 20 is plenty for a single day


def get_structure_shift(window_minutes=60, min_flips=2):
    """
    FIX 20: Reads recent flip events (see save_flip_event). If
    `min_flips` or more flips in the SAME direction happened within the
    last `window_minutes`, returns a factual dict — otherwise None.
    Deliberately no "bias"/"lean" language here, same principle as
    get_context_zones()/_dominance_block() — Python reports facts, the
    AI decides what to do with them.
    """
    events = _get("flip_log") or []
    if not events:
        return None

    now = datetime.now(IST)
    recent = []
    for e in events:
        try:
            t = datetime.strptime(e["time"], "%H:%M").replace(
                year=now.year, month=now.month, day=now.day
            )
            t = IST.localize(t)
            if 0 <= (now - t).total_seconds() / 60 <= window_minutes:
                recent.append(e)
        except Exception:
            continue

    if len(recent) < min_flips:
        return None

    down_count = sum(1 for e in recent if e["direction"] == "down")
    up_count   = sum(1 for e in recent if e["direction"] == "up")

    if down_count >= min_flips and down_count > up_count:
        matches = [e for e in recent if e["direction"] == "down"]
        return {"direction": "down", "count": len(matches), "since": matches[0]["time"]}
    if up_count >= min_flips and up_count > down_count:
        matches = [e for e in recent if e["direction"] == "up"]
        return {"direction": "up", "count": len(matches), "since": matches[0]["time"]}

    return None


def flush_day():
    for k in ["zones", "zones_date", "morning_context", "morning_context_date",
              "signal_history", "ai_raw_log", "recovery_log", "flip_log"]:
        _delete(k)
    log.info("🧹 Day data flushed")


def get_signal_history():
    return _get("signal_history") or []


def save_signal_log(entry):
    h = get_signal_history()
    h.append(entry)
    _set("signal_history", h)


def save_ai_raw_log(entry):
    logs = _get("ai_raw_log") or []
    logs.append(entry)
    _set("ai_raw_log", logs[-MAX_AI_RAW_LOG:])


def zone_buffer(zone):
    """
    FIX 9: Dynamic touch buffer — replaces flat ZONE_NEAR_PTS=25.
    15% of zone width, floor 10pts. For this bot's typical 20-30pt
    zones this mostly floors to 10 (tighter than the old flat 25) —
    wider zones (60pt+) get a genuinely larger, proportional buffer.
    """
    width = zone.get("high", 0) - zone.get("low", 0)
    return max(10, width * 0.15)


def get_touched_zone(ltp, zones):
    """
    Return touched zone. Priority:
    1. Non-NO_TRADE/SIDEWAYS zones first
    2. NO_TRADE/SIDEWAYS only if nothing else touched
    """
    no_trade_types = ["NO_TRADE", "SIDEWAYS"]

    for z in zones:
        if z.get("type") in no_trade_types:
            continue
        low, high = z.get("low", 0), z.get("high", 0)
        buf = zone_buffer(z)
        if (ltp >= low - buf) and (ltp <= high + buf):
            return z

    for z in zones:
        if z.get("type") not in no_trade_types:
            continue
        low, high = z.get("low", 0), z.get("high", 0)
        buf = zone_buffer(z)
        if (ltp >= low - buf) and (ltp <= high + buf):
            return z

    return None


def get_context_zones(ltp, zones):
    """
    FIX 12: Nearest support/resistance around current LTP, plus range
    width and position% within that range. Pure facts — no bias label.
    """
    no_trade_types = ["NO_TRADE", "SIDEWAYS"]
    candidates = [z for z in zones if z.get("type") not in no_trade_types]

    above = [z for z in candidates if z.get("low", 0) > ltp]
    below = [z for z in candidates if z.get("high", 0) < ltp]

    nearest_resistance = min(above, key=lambda z: z["low"] - ltp) if above else None
    nearest_support    = max(below, key=lambda z: ltp - z["high"]) if below else None

    range_width  = None
    position_pct = None
    if nearest_resistance and nearest_support:
        range_low  = nearest_support["high"]
        range_high = nearest_resistance["low"]
        range_width = range_high - range_low
        if range_width > 0:
            position_pct = round((ltp - range_low) / range_width * 100, 1)

    return {
        "support":      nearest_support,
        "resistance":   nearest_resistance,
        "range_width":  range_width,
        "position_pct": position_pct
    }


def detect_dominance_candle(df, ltp, lookback=DOMINANCE_LOOKBACK):
    """
    FIX 11: Python-side dominance-candle detection (replaces asking the
    AI to eyeball OHLC arrays for this every call). Finds the
    biggest-range candle in the last `lookback` 5M candles, determines
    which side (BUYER/SELLER) dominated it, and whether current price
    has reclaimed (SELLER candle) or lost (BUYER candle) its 50% level.
    Returns None if no candle clears DOMINANCE_MIN_RANGE — most ticks
    won't have one, and the AI prompt module is only injected when this
    returns something (token savings + the module wasn't relevant most
    of the time anyway).
    """
    if df.empty or len(df) < 3:
        return None

    d = df.tail(lookback).copy()
    if d.empty:
        return None

    d["rng"] = d["h"] - d["l"]
    idx = d["rng"].idxmax()
    row = d.loc[idx]
    rng = int(row["rng"])
    if rng < DOMINANCE_MIN_RANGE:
        return None

    o, c, h, l = int(row["o"]), int(row["c"]), int(row["h"]), int(row["l"])
    mid = round((h + l) / 2)
    side = "SELLER" if c < o else "BUYER"

    # SELLER dominance candle: reclaimed if price closed back above 50%
    # BUYER dominance candle: "reclaimed" here means buyer is losing —
    # price closed back below 50%
    reclaimed = (ltp > mid) if side == "SELLER" else (ltp < mid)

    return {
        "low": l, "high": h, "mid": mid,
        "side": side, "rng": rng, "reclaimed": reclaimed
    }


def _next_tick_boundary(dt, ticks_ahead=1):
    """
    FIX 14: Snap to the actual zone_monitor 5-min job grid (:00,:05,:10...
    +10sec) instead of raw wall-clock + N minutes. Wall-clock cooldowns
    drifted a few seconds off the grid and silently ate the very next
    real tick (confirmed in logs: "7s left" / "12s left" skipping a
    genuine confirmation candle). ticks_ahead counts in 5-min job ticks.
    """
    base = dt.replace(second=0, microsecond=0)
    rem  = base.minute % 5
    nxt  = base + timedelta(minutes=(5 - rem) if rem else 5)
    nxt  = nxt + timedelta(minutes=5 * (ticks_ahead - 1), seconds=10)
    return nxt


def zone_cooldown_ok(zone_id):
    """Only block if cooldown is still active (tick-grid aligned — see mark_zone_cooldown)."""
    key = f"cooldown_{zone_id}"
    cd  = _get(key)
    if cd:
        try:
            until = datetime.fromisoformat(cd["until"])
            if until.tzinfo is None:
                until = IST.localize(until)
            now = datetime.now(IST)
            if now < until:
                rem = int((until - now).total_seconds())
                log.info(f"Zone {zone_id} cooldown: {rem}s left")
                return False
        except Exception as e:
            log.warning(f"Cooldown parse error {zone_id}: {e}")
    return True


def mark_zone_cooldown(zone_id, signal_type):
    """
    BUY_CE/BUY_PE: ~30min cooldown (6 ticks), tick-grid aligned so drift
    can't eat a real candle (FIX 14).
    WAIT: NO cooldown anymore (FIX 14b) — the very next 5-min tick can
    re-check the same zone immediately. Previously a flat 5min WAIT
    cooldown was blocking the confirmation candle that often arrives
    right after the first WAIT, which was the main cause of signals
    landing 15-20min after the real move's origin instead of ~5-10min.
    """
    if signal_type in ["BUY_CE", "BUY_PE"]:
        now   = datetime.now(IST)
        until = _next_tick_boundary(now, ticks_ahead=6)
        _set(f"cooldown_{zone_id}", {"until": until.isoformat()},
             ttl=int((until - now).total_seconds()) + 120)
        log.info(f"Zone {zone_id} cooldown: until {until.strftime('%H:%M:%S')} ({signal_type})")
    else:
        _delete(f"cooldown_{zone_id}")
        log.info(f"Zone {zone_id}: no cooldown (WAIT) — next tick can re-check")


def merge_zones(existing, new_zones, ltp):
    """
    Merge new zones into existing.
    FLIP_ zones preserved, 50% overlap → replace, too far → drop.
    """
    def overlap(a, b):
        ol = max(0, min(a["high"], b["high"]) - max(a["low"], b["low"]))
        span_a = max(a["high"] - a["low"], 1)
        return ol / span_a

    flipped  = [z for z in existing if z.get("type","").startswith("FLIP_")]
    non_flip = [z for z in existing if not z.get("type","").startswith("FLIP_")]

    result = list(non_flip)
    for nz in new_zones:
        if abs((nz["low"] + nz["high"]) / 2 - ltp) > 500:
            continue

        skip = False
        for fz in flipped:
            if overlap(fz, nz) >= 0.5:
                skip = True
                break
        if skip:
            continue

        replaced = False
        for i, ez in enumerate(result):
            if overlap(ez, nz) >= 0.5:
                result[i] = nz
                replaced = True
                break
        if not replaced:
            result.append(nz)

    result.extend(flipped)
    result.sort(key=lambda z: abs((z["low"] + z["high"]) / 2 - ltp))
    return result[:MAX_ZONES]


def track_open_signals(ltp):
    """Check if ref_sl or ref_target hit for open signals"""
    history = get_signal_history()
    updated = False
    for sig in history:
        if sig.get("result") != "OPEN":
            continue

        ref_sl  = sig.get("ref_sl", 0)
        ref_tgt = sig.get("ref_target", 0)
        stype   = sig.get("signal", "")

        if stype == "BUY_CE":
            if ref_sl and ltp <= ref_sl:
                sig["result"] = "REF_SL_HIT"
                updated = True
            elif ref_tgt and ltp >= ref_tgt:
                sig["result"] = "REF_TARGET_HIT"
                updated = True
        elif stype == "BUY_PE":
            if ref_sl and ltp >= ref_sl:
                sig["result"] = "REF_SL_HIT"
                updated = True
            elif ref_tgt and ltp <= ref_tgt:
                sig["result"] = "REF_TARGET_HIT"
                updated = True

    if updated:
        _set("signal_history", history)


# ═══════════════════════════════════════════════════════
# SECTION 4.5: CANDLE TRIGGER ENGINE
# ═══════════════════════════════════════════════════════

def _to_ist_datetime(ts):
    """Convert any timestamp to IST-aware datetime"""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return IST.localize(ts.to_pydatetime())
    return ts.tz_convert(IST).to_pydatetime()


def get_last_completed_m5_candle(tf_data, buffer_seconds=10):
    """
    Return last COMPLETED 5min candle.
    If latest candle still forming → use previous one.
    """
    m5 = tf_data.get("m5", pd.DataFrame())
    if m5.empty:
        return None

    now         = datetime.now(IST)
    last        = m5.iloc[-1]
    last_ts     = _to_ist_datetime(last["ts"])
    complete_at = last_ts + timedelta(minutes=5, seconds=buffer_seconds)

    if now >= complete_at:
        return last

    if len(m5) >= 2:
        prev    = m5.iloc[-2]
        prev_ts = _to_ist_datetime(prev["ts"])
        log.info(
            f"5M candle {last_ts.strftime('%H:%M')} still forming "
            f"→ using {prev_ts.strftime('%H:%M')}"
        )
        return prev

    return None


def detect_candle_event(tf_data, zone, ltp=0):
    """
    Detect what type of candle event is happening at this zone.
    Returns: (event_name, event_detail)

    Events:
    BREAKOUT_CANDLE        → Close above resistance + buffer
    BREAKDOWN_CANDLE       → Close below support + buffer
    BULLISH_SWEEP_RECLAIM  → Wick below support, close back above
    BEARISH_SWEEP_REJECT   → Wick above resistance, close back below
    SUPPORT_REJECTION      → Bullish candle at support zone
    RESISTANCE_REJECTION   → Bearish candle at resistance zone
    COMPRESSION_BREAKOUT   → Tight 4-candle range then breakout
    RETEST_HOLD            → Flip zone being retested + holding
    NO_EVENT               → No clear setup — skip AI
    """
    try:
        m5 = drop_incomplete_candle(
            tf_data.get("m5", pd.DataFrame()), 5
        )
        if m5.empty or len(m5) < 5:
            return "NO_EVENT", "insufficient 5M data"

        candle = get_last_completed_m5_candle({"m5": m5})
        if candle is None:
            return "NO_EVENT", "no completed candle"

        o = int(candle["o"])
        h = int(candle["h"])
        l = int(candle["l"])
        c = int(candle["c"])

        zone_low  = zone.get("low",  0)
        zone_high = zone.get("high", 0)
        zone_type = zone.get("type", "")

        if zone_type == "FLIP":
            zone_type = "FLIP_SUPPORT" if ltp >= zone_low else "FLIP_RESISTANCE"

        body        = c - o
        candle_rng  = h - l
        upper_wick  = h - max(o, c)
        lower_wick  = min(o, c) - l

        # ── 1. BREAKOUT ───────────────────────────────────
        if c > zone_high + BREAKOUT_BUFFER:
            if zone_type in ["RESISTANCE", "FLIP_RESISTANCE", "LIQUIDITY"]:
                return (
                    "BREAKOUT_CANDLE",
                    f"close {c} > zone_high {zone_high}+{BREAKOUT_BUFFER}"
                )

        # ── 2. BREAKDOWN ──────────────────────────────────
        if c < zone_low - BREAKOUT_BUFFER:
            if zone_type in ["SUPPORT", "FLIP_SUPPORT", "LIQUIDITY"]:
                return (
                    "BREAKDOWN_CANDLE",
                    f"close {c} < zone_low {zone_low}-{BREAKOUT_BUFFER}"
                )

        # ── 3. BULLISH SWEEP RECLAIM ──────────────────────
        # FIX 13: zone_type gated — was firing on SUPPORT zones too,
        # mislabeling a normal support-bounce as a resistance-sweep event.
        if l < zone_low - 5 and c > zone_low:
            if zone_type in ["SUPPORT", "FLIP_SUPPORT", "LIQUIDITY"]:
                return (
                    "BULLISH_SWEEP_RECLAIM",
                    f"wick swept to {l} below {zone_low}, reclaimed to {c}"
                )

        # ── 4. BEARISH SWEEP REJECT ───────────────────────
        # FIX 13: zone_type gated — mirror of above.
        if h > zone_high + 5 and c < zone_high:
            if zone_type in ["RESISTANCE", "FLIP_RESISTANCE", "LIQUIDITY"]:
                return (
                    "BEARISH_SWEEP_REJECT",
                    f"wick swept to {h} above {zone_high}, rejected to {c}"
                )

        # ── 5. COMPRESSION BREAKOUT / BREAKDOWN ───────────
        if len(m5) >= SIDEWAYS_CANDLE_CNT + 1:
            prev = m5.iloc[-(SIDEWAYS_CANDLE_CNT + 1):-1]
            h4   = int(prev["h"].max())
            l4   = int(prev["l"].min())
            rng4 = h4 - l4
            if rng4 <= SIDEWAYS_MAX_RANGE:
                if c > h4 + BREAKOUT_BUFFER:
                    return (
                        "COMPRESSION_BREAKOUT",
                        f"4-candle range {rng4}pts → breakout close {c} above {h4}"
                    )
                if c < l4 - BREAKOUT_BUFFER:
                    return (
                        "COMPRESSION_BREAKDOWN",
                        f"4-candle range {rng4}pts → breakdown close {c} below {l4}"
                    )

        # ── 6. SUPPORT REJECTION ──────────────────────────
        # FIX 10: wick ratio raised 0.25 → 0.30 (REJECTION_WICK_RATIO).
        # body>=-3 kept as-is on purpose — a hammer (small red body,
        # long lower wick) is a real bullish reversal signal; forcing
        # body>=0 would reject genuine hammers, not just noise.
        if zone_type in ["SUPPORT", "FLIP_SUPPORT", "LIQUIDITY"]:
            bullish_body = body > 0
            doji_body    = abs(body) <= 3
            good_wick    = lower_wick > candle_rng * REJECTION_WICK_RATIO if candle_rng > 0 else False
            close_holds  = c > zone_low + 5
            not_bearish  = body >= -3

            if close_holds and not_bearish and (bullish_body or (doji_body and good_wick)):
                return (
                    "SUPPORT_REJECTION",
                    f"bullish at support: body={body} lower_wick={lower_wick} close={c}"
                )

        # ── 7. RESISTANCE REJECTION ───────────────────────
        if zone_type in ["RESISTANCE", "FLIP_RESISTANCE"]:
            bearish_body = body < 0
            doji_body    = abs(body) <= 3
            good_wick    = upper_wick > candle_rng * REJECTION_WICK_RATIO if candle_rng > 0 else False
            close_below  = c < zone_high - 5
            not_bullish  = body <= 3

            if close_below and not_bullish and (bearish_body or (doji_body and good_wick)):
                return (
                    "RESISTANCE_REJECTION",
                    f"bearish at resistance: body={body} upper_wick={upper_wick} close={c}"
                )

        # ── 8. RETEST HOLD (flip zone) ────────────────────
        if zone_type == "FLIP_SUPPORT" and c > zone_low and body > 0:
            return (
                "RETEST_HOLD",
                f"flip support retest: bullish close {c} above {zone_low}"
            )
        if zone_type == "FLIP_RESISTANCE" and c < zone_high and body < 0:
            return (
                "RETEST_HOLD",
                f"flip resistance retest: bearish close {c} below {zone_high}"
            )

        return "NO_EVENT", f"zone touched but no clear candle event (body={body})"

    except Exception as e:
        log.error(f"detect_candle_event error: {e}")
        return "NO_EVENT", f"error: {e}"


def maybe_flip_zone(zone, event):
    """
    After breakout/breakdown, flip zone type dynamically.
    RESISTANCE → FLIP_SUPPORT after BREAKOUT_CANDLE
    SUPPORT    → FLIP_RESISTANCE after BREAKDOWN_CANDLE
    """
    z = dict(zone)
    if event == "BREAKOUT_CANDLE" and z.get("type") == "RESISTANCE":
        z["type"]             = "FLIP_SUPPORT"
        z["preferred_action"] = "BUY_CE"
        z["why"]              = f"[FLIPPED→SUPPORT] {z.get('why','')}"
        log.info(f"Zone {z.get('id')} flipped: RESISTANCE → FLIP_SUPPORT")

    elif event == "BREAKDOWN_CANDLE" and z.get("type") == "SUPPORT":
        z["type"]             = "FLIP_RESISTANCE"
        z["preferred_action"] = "BUY_PE"
        z["why"]              = f"[FLIPPED→RESISTANCE] {z.get('why','')}"
        log.info(f"Zone {z.get('id')} flipped: SUPPORT → FLIP_RESISTANCE")

    return z


# ═══════════════════════════════════════════════════════
# SECTION 5: AI CALLS
# ═══════════════════════════════════════════════════════

PROMPT_MODULES = {
    "SUPPORT_REJECTION": (
        "SITUATION: SUPPORT_REJECTION — Price at support, bullish candle visible. "
        "Check: genuine bounce or weak close before further drop? "
        "BUY_CE if strong bullish rejection confirmed. WAIT if weak."
    ),
    "RESISTANCE_REJECTION": (
        "SITUATION: RESISTANCE_REJECTION — Price at resistance, bearish candle visible. "
        "Check: genuine rejection or fakeout before breakout? "
        "BUY_PE if strong bearish rejection. WAIT if weak."
    ),
    "BREAKOUT_CANDLE": (
        "SITUATION: BREAKOUT_CANDLE — Price closed ABOVE resistance with momentum. "
        "This is a potential breakout continuation. "
        "BUY_CE if breakout is strong and body is big. "
        "WAIT if candle is small/suspicious or if immediate resistance is overhead. "
        "Do NOT return BUY_PE on a breakout candle unless clear trap signal."
    ),
    "BREAKDOWN_CANDLE": (
        "SITUATION: BREAKDOWN_CANDLE — Price closed BELOW support with momentum. "
        "Potential breakdown continuation. "
        "BUY_PE if breakdown is strong. WAIT if weak/possible trap. "
        "Do NOT return BUY_CE on a breakdown candle."
    ),
    "BULLISH_SWEEP_RECLAIM": (
        "SITUATION: BULLISH_SWEEP_RECLAIM / CHoCH — Price swept BELOW support then "
        "closed back above zone. Classic liquidity sweep + reclaim. "
        "BUY_CE if last 5M close is strong above zone AND higher low forming. "
        "WAIT if close is weak or no structural break yet."
    ),
    "BEARISH_SWEEP_REJECT": (
        "SITUATION: BEARISH_SWEEP_REJECT / CHoCH — Price swept ABOVE resistance then "
        "closed back below zone. Classic trap rejection. "
        "BUY_PE if close is strong below zone AND lower high forming. "
        "WAIT if close is weak."
    ),
    "COMPRESSION_BREAKOUT": (
        "SITUATION: COMPRESSION_BREAKOUT — Last 4 candles tight range (<30pts), "
        "now breaking out. "
        "BUY_CE if breaking upside and aligned with bias. "
        "BUY_PE if breaking downside and aligned with bias. "
        "WAIT if direction unclear or volume/body weak."
    ),
    "COMPRESSION_BREAKDOWN": (
        "SITUATION: COMPRESSION_BREAKDOWN — Last 4 candles tight range (<30pts), "
        "now breaking down. "
        "BUY_PE if breakdown is clean and aligned with bias. "
        "WAIT if uncertain."
    ),
    "RETEST_HOLD": (
        "SITUATION: RETEST_HOLD — Previously broken level (flip zone) being retested. "
        "Price returned to broken zone and is holding. "
        "BUY_CE if flip support holding (bullish close above zone). "
        "BUY_PE if flip resistance holding (bearish close below zone). "
        "This is a high-quality setup if confirmed."
    ),
}


OI_INTERPRETATION_MODULE = """
OI CONTEXT MODULE (use when [OI CONTEXT] block is present):

DATA MEANING:
- PCR trend: 30min→15min→5min→now (rising PCR = more PE writing = bearish bias)
- CE_OI_CHG: CE open interest change % (positive = fresh short writing = bearish)
- PE_OI_CHG: PE open interest change % (positive = fresh put writing = bearish hedge)
- CE_WALL: Strike with highest CE OI (resistance — sellers protecting this level)
- PE_WALL: Strike with highest PE OI (support — sellers protecting this level)
- CE_VOL/PE_VOL: Volume vs morning baseline (HIGH = active directional buying)

SIGNAL RULES:
- CE_OI rising + PE_OI falling + PCR falling = Bullish bias → lean BUY_CE
- PE_OI rising + CE_OI falling + PCR rising = Bearish bias → lean BUY_PE
- CE_VOL HIGH = Call buyers active = directional bullish bet
- PE_VOL HIGH = Put buyers active = directional bearish bet OR hedge
- CE_WALL breaking (OI suddenly drops) = Resistance gone → price can move up
- PE_WALL breaking (OI suddenly drops) = Support gone → price can move down
- OI and price action BOTH confirm same direction = HIGH confidence
- OI and price action CONFLICT = reduce confidence → lean toward WAIT
- If OI shows N/A (no history yet) = ignore OI, use only price action
- FIX 17: PCR is given as 30min→15min→now (oldest→newest). Weight the
  MOST RECENT leg (15min→now) most heavily. If the most recent leg reverses
  the earlier trend (e.g. was rising but the last step fell), treat this
  as an early-reversal warning, not a continuation of the old trend —
  describe it accurately as "reversing" rather than calling it "rising"
  or "falling" based on the overall span.
"""

# FIX 11: This module is now INJECTED CONDITIONALLY (only when Python's
# detect_dominance_candle() actually finds a qualifying candle), and it
# no longer asks the AI to "identify the biggest-range candle" itself —
# Python already did that and put exact numbers in [DOMINANCE CANDLE].
# The AI's job here is interpretation only.
LIQUIDITY_FIGHT_MODULE = """
LIQUIDITY FIGHT MODULE (a dominance candle was detected — see the
[DOMINANCE CANDLE] block in the data: Range/Side/50%/Reclaimed):

- The dominance candle is the biggest-range candle in recent price
  action — strong BUYER or SELLER control at the time it formed.
- Side:SELLER + Reclaimed:YES → price closed back above that candle's
  50% level → seller weakening → lean bullish, especially if the
  touched zone is support/liquidity.
- Side:BUYER + Reclaimed:YES → price closed back below that candle's
  50% level → buyer weakening → lean bearish.
- Reclaimed:NO → the dominant side is still in control → no extra edge
  from this candle; fall back to standard zone rules.
- "Reaction" = small bounce/fall at a level. "Action" = clean break with
  follow-through toward the next liquidity target (use [OTHER ACTIVE
  ZONES] / [PRICE CONTEXT] to judge the next target). Judge from wick
  size, close strength, and follow-through which is more likely.
- A weak/indecisive close right at the level is often an invitation for
  the opposite side to take over — treat this as WAIT/early warning,
  not a confirmed signal, unless the next candle confirms.
"""

# FIX 22: Only injected for reversal-type events (rejection/reclaim/
# retest at a zone) AND only when OI context is available. This is the
# user's own long-standing OI philosophy (PCR>1.5=support wall,
# PCR<0.7=resistance wall) applied specifically to confirm-or-doubt a
# reversal candle — separate from the general OI_INTERPRETATION_MODULE,
# which is about OI/PCR trend direction generally, not zone-confluence.
PCR_REVERSAL_EVENTS = {
    "SUPPORT_REJECTION", "RESISTANCE_REJECTION",
    "BULLISH_SWEEP_RECLAIM", "BEARISH_SWEEP_REJECT", "RETEST_HOLD"
}

PCR_CONFLUENCE_MODULE = """
PCR-REVERSAL CONFLUENCE MODULE (this is a reversal-type setup — check
this alongside OI_INTERPRETATION_MODULE above):

- PCR > 1.5 at a SUPPORT zone = strong put-writing wall = genuine
  support confluence.
- PCR < 0.7 at a RESISTANCE zone = strong call-writing wall = genuine
  resistance confluence.
- At SUPPORT: current PCR > 1.5 AND the most recent PCR leg is falling
  = bullish reversal confluence with the candle event → this can raise
  confidence toward HIGH if candles also confirm.
- At SUPPORT: current PCR < 1.0, OR PCR rising while price bounces =
  the OI does NOT support this bounce (no real put wall here) → treat
  as a weaker/possible-trap setup, lean WAIT or lower confidence even
  if the candle looks clean.
- At RESISTANCE: current PCR < 0.7 AND the most recent PCR leg is
  rising = bearish reversal confluence → can raise confidence toward
  HIGH if candles also confirm.
- At RESISTANCE: current PCR > 1.0, OR PCR falling while price
  rejects = OI does NOT support this rejection → lean WAIT or lower
  confidence.
- If OI context is unavailable (N/A) this module does not apply —
  judge from candles alone.
"""

FIRST_ANALYSIS_SYSTEM = """You are an expert NIFTY50 price-action zone analyst.
The user manually confirms every trade. This is NOT auto-trading.

DATA FORMAT: Each line = one candle (O H L C or H L).
Values are delta-encoded (add BASE to get real price).
Candles: oldest → newest.

TASK: Analyze given multi-timeframe data and create practical intraday trading zones.

Create:
- Support zones
- Resistance zones
- Flip zones (old S now R, or vice versa)
- Liquidity zones (equal highs/lows, sweep targets)
- No-trade / sideways zone if applicable

RULES:
- LTP nearby zones are HIGHEST priority (actionable today)
- Far HTF levels → context only, not main zones
- Use 5M/15M for actionable zone boundaries
- Daily/1H for bias and context only
- Zones = price RANGES (low to high), not single lines
- Max 5–7 useful zones total. Quality over quantity — only the most relevant zones.
- If unclear structure → create WAIT/NO_TRADE zone
- Do NOT force bullish or bearish bias

SWEEP/RECLAIM RULE:
If recent candles show price swept below a support then reclaimed it →
mark that area as FLIP or LIQUIDITY zone with preferred_action BUY_CE.
Mirror for bearish: swept above resistance then rejected → BUY_PE zone.

LIQUIDITY FIGHT MODULE (apply this thinking when identifying zones):
- Liquidity is relative to current price. When price is near an area, even
  minor local levels matter. When price has moved far away from an area,
  only the major base (origin of a clean, strong move) matters — find the
  ORIGIN point of the cleanest/strongest move as the major liquidity base.
- A "clean move" = consecutive same-direction candles with small wicks and
  minimal overlap/pullback. The starting point of such a move is a strong
  liquidity base — mark it as a zone with tag "MAJOR_LIQUIDITY" and
  note in "why" that it's a clean-move origin.
- Identify the single biggest-range candle (dominance candle) visible in
  the data. Calculate its mid-point (50% of its high-low range). This
  mid-point is the level where the losing side (buyer or seller) typically
  gives up. If you find such a candle relevant to current price action,
  create a zone around that mid-point with tag "DOMINANCE_50" and explain
  which side (BUYER/SELLER) dominance it represents and at what price the
  opposite side would "fail" (i.e. lose) if breached.
- For every zone you create, add a "tags" array. Use tags from:
  ["MAJOR_LIQUIDITY", "LOCAL_LIQUIDITY", "DOMINANCE_50", "CLEAN_ORIGIN",
  "FLIP", "SWEEP_TARGET"] — as many as apply, or empty array if none apply.

RESPOND ONLY in valid JSON. No text outside JSON."""


ZONE_DECISION_SYSTEM = """You are an expert NIFTY50 intraday directional signal analyst.
The user manually confirms every trade. This is NOT auto-trading.

You receive:
- Morning bias/structure context
- Touched zone details
- All active zones
- Current LTP
- Price context (nearest support/resistance, range position) when available
- A detected dominance candle (if any) when available
- Last completed 15M and 5M candles (delta-encoded)

TASK: Decide ONE output for price touching/entering a saved zone:
  BUY_CE / BUY_PE / WAIT

RULES:
- Focus on the TOUCHED ZONE first
- Check: respecting / rejecting / breaking / trapping around zone
- Use last completed 5M and 15M candles for confirmation
- Do NOT chase big candles already moved
- Weak candle confirmation → WAIT
- Inside sideways/no-trade zone → WAIT
- Setup forming but not confirmed → WAIT + mention "setup forming"
- Need minimum 2 REAL confirmations for BUY_CE or BUY_PE
- confidence LOW → WAIT
- Direction unclear → WAIT. Never force signal.

BIAS FRESHNESS (read this carefully):
- The [MORNING CONTEXT] Bias/Structure fields are from the LAST HOURLY
  reanalysis (runs at :02 past each hour) — they can be up to ~58
  minutes stale, NOT real-time.
- If the current 5M/15M candles and the touched zone's detected event
  clearly CONTRADICT this bias (e.g. bias says LONG but candles show a
  clean breakdown with 2+ real confirmations), trust the current
  candles over the stale bias label. Do not let an outdated bias alone
  push you to WAIT when the immediate price action is clear.
- If a [STRUCTURE SHIFT DETECTED] block is present below, treat it as
  a stronger real-time signal than the morning bias — it means Python
  has already detected multiple zones breaking in the same new
  direction since the bias was last computed.

CHoCH RULE:
Bullish: price swept below support → reclaimed above → minor lower-high broken
→ BUY_CE if last completed 5M close confirms reclaim.
Bearish: price swept above resistance → rejected back below → minor higher-low broken
→ BUY_PE if last completed 5M close confirms rejection.

REFERENCE LEVELS (for the "reference" field — analytics only, not advice):
- ref_sl should be placed just beyond the relevant structural invalidation
  point: for BUY_CE, just below the dominance candle's failure level / the
  touched zone's low. For BUY_PE, just above the dominance candle's failure
  level / the touched zone's high.
- ref_target should be the next realistic liquidity zone in the trade's
  direction (use the "OTHER ACTIVE ZONES" list to pick the nearest relevant
  one), not an arbitrary point distance.
- Briefly justify ref_sl/ref_target in terms of structure in your "reason"
  or "risk_note" if relevant.
- CRITICAL: ref_sl is the ONE invalidation number for this trade. If you
  mention an invalidation/cancel level anywhere in "reason", "risk_note",
  or "message", it MUST be the exact same number as ref_sl — never a
  second, different invalidation level. Before finalizing, check that any
  signal you send is not ALREADY invalidated: if current LTP has already
  crossed back past ref_sl (or past the touched zone's own boundary) by
  the time you're writing this response, return WAIT instead of forcing
  a signal that contradicts your own risk_note.
- CRITICAL: All prices written in "reason", "risk_note", and "message"
  must be real index prices (the same scale as the [CURRENT LTP] value
  given to you), never the raw delta-encoded numbers from the candle
  data blocks above. Convert before writing.

RESPOND ONLY in valid JSON. No text outside JSON."""


def call_claude(system_prompt, user_prompt, model=DECISION_MODEL, max_tokens=ZONE_DECISION_MAX_TOKENS):
    """
    FIX 8: renamed from call_haiku — now parameterized by model + max_tokens
    so both Sonnet (first analysis) and Haiku (zone decision) share one
    code path. Defaults stay Haiku/1000 for any caller that doesn't override.
    """
    url  = "https://api.anthropic.com/v1/messages"
    hdrs = {
        "x-api-key":         ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json"
    }
    body = {
        "model":      model,
        "max_tokens": max_tokens,
        "system":     system_prompt,
        "messages":   [{"role": "user", "content": user_prompt}]
    }
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=45)
        if r.status_code == 200:
            return r.json()["content"][0]["text"]
        log.error(f"Claude API error {r.status_code} ({model}): {r.text[:200]}")
    except Exception as e:
        log.error(f"Claude call failed ({model}): {e}")
    return None


def parse_json(raw):
    if not raw:
        return None
    try:
        clean = raw.strip().replace("```json","").replace("```","").strip()
        s = clean.find("{")
        e = clean.rfind("}") + 1
        return json.loads(clean[s:e]) if s != -1 else None
    except Exception as ex:
        log.error(f"JSON parse error: {ex}")
        return None


def run_first_analysis(tf_data, mode="FIRST"):
    """Run first analysis or hourly reanalysis — uses ANALYSIS_MODEL (Sonnet)"""
    log.info(f"🌅 Running {mode} analysis...")

    data_str = build_first_analysis_string(tf_data)
    if not data_str:
        log.error("Data build failed")
        return None

    if mode == "FIRST":
        liquidity_focus = (
            "\n[FOCUS FOR THIS RUN: MAJOR LIQUIDITY]\n"
            "This is the first analysis of the day. Use the Daily/1H data "
            "to identify MAJOR liquidity bases — the origin points of the "
            "cleanest, strongest historical moves. These are zones that "
            "matter even when price is far away from them. Tag them "
            "MAJOR_LIQUIDITY."
        )
    else:
        liquidity_focus = (
            "\n[FOCUS FOR THIS RUN: LOCAL LIQUIDITY]\n"
            "This is an hourly reanalysis. In addition to validating "
            "major zones, use the recent 15M/5M candles to identify LOCAL "
            "liquidity — recent reaction areas, equal highs/lows, and "
            "dominance candles near current price. Tag them LOCAL_LIQUIDITY "
            "or DOMINANCE_50 as relevant."
        )

    user_prompt = data_str + liquidity_focus + """

RETURN ONLY this JSON:
{
  "bias": "LONG/SHORT/NEUTRAL",
  "structure": "HH_HL/LH_LL/SIDEWAYS/UNCLEAR",
  "day_type": "TRENDING/RANGING/VOLATILE/UNCLEAR",
  "summary": "short market explanation (1-2 lines)",
  "zones": [
    {
      "id": "S1",
      "type": "SUPPORT/RESISTANCE/FLIP/LIQUIDITY/NO_TRADE",
      "low": 0,
      "high": 0,
      "strength": "STRONG/MED/WEAK",
      "tags": [],
      "preferred_action": "BUY_CE/BUY_PE/WAIT",
      "why": "short reason"
    }
  ],
  "no_trade_zone": {
    "low": 0,
    "high": 0,
    "why": "short reason"
  }
}"""

    raw    = call_claude(
        FIRST_ANALYSIS_SYSTEM, user_prompt,
        model=ANALYSIS_MODEL, max_tokens=FIRST_ANALYSIS_MAX_TOKENS
    )
    result = parse_json(raw)

    save_ai_raw_log({
        "time":   datetime.now(IST).strftime("%H:%M"),
        "mode":   mode,
        "model":  ANALYSIS_MODEL,
        "raw":    raw[:500] if raw else None,
        "parsed": bool(result)
    })

    if result:
        log.info(f"✅ {mode} done: {result.get('bias')} | {result.get('day_type')}")
    return result


def run_zone_decision(tf_data, ltp, touched_zone, all_zones, morning_ctx,
                       event="", event_reason="", oi_context=None):
    """Run zone touch AI decision — uses DECISION_MODEL (Haiku) + event-based prompt routing"""
    log.info(f"🎯 Zone decision: {touched_zone.get('id')} @ LTP:{ltp} | Event:{event}")

    # FIX 11/12: compute Python-side facts once, inject conditionally
    m5_for_dom    = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    dominance     = detect_dominance_candle(m5_for_dom, ltp)
    context_zones = get_context_zones(ltp, all_zones)
    # FIX 20: real-time structure-shift fact, independent of hourly bias
    structure_shift = get_structure_shift()

    data_str = build_zone_decision_string(
        tf_data, ltp, touched_zone, all_zones, morning_ctx,
        oi_context=oi_context, context_zones=context_zones, dominance=dominance
    )

    event_block = ""
    if event and event != "NO_EVENT":
        event_block = f"\n\n[DETECTED CANDLE EVENT]\nEvent: {event}\nDetail: {event_reason}"

    shift_block = _structure_shift_block(structure_shift)

    situation_module = PROMPT_MODULES.get(event, "")
    system_prompt    = ZONE_DECISION_SYSTEM
    if situation_module:
        system_prompt += f"\n\n{situation_module}"
    if dominance:
        system_prompt += f"\n\n{LIQUIDITY_FIGHT_MODULE}"
    if oi_context:
        system_prompt += OI_INTERPRETATION_MODULE
        # FIX 22: PCR-reversal confluence — only for reversal-type events
        if event in PCR_REVERSAL_EVENTS:
            system_prompt += f"\n\n{PCR_CONFLUENCE_MODULE}"

    user_prompt = data_str + event_block + shift_block + """

RETURN ONLY this JSON:
{
  "signal": "BUY_CE/BUY_PE/WAIT",
  "confidence": "HIGH/MED/LOW",
  "zone_id": "id of touched zone",
  "zone_type": "SUPPORT/RESISTANCE/FLIP/LIQUIDITY/NO_TRADE",
  "zone_reaction": "BOUNCE/REJECTION/BREAKOUT/BREAKDOWN/TRAP/NO_REACTION",
  "confirmations": [],
  "confirmation_count": 0,
  "reason": "short reason (2 lines max)",
  "risk_note": "what user should manually check",
  "message": "short Hinglish summary for Telegram",
  "reference": {
    "ref_sl": 0,
    "ref_target": 0,
    "valid_for_minutes": 45,
    "note": "analytics only, not trade advice"
  }
}"""

    raw    = call_claude(
        system_prompt, user_prompt,
        model=DECISION_MODEL, max_tokens=ZONE_DECISION_MAX_TOKENS
    )
    result = parse_json(raw)

    save_ai_raw_log({
        "time":    datetime.now(IST).strftime("%H:%M"),
        "mode":    "ZONE_DECISION",
        "model":   DECISION_MODEL,
        "event":   event,
        "zone_id": touched_zone.get("id"),
        "ltp":     ltp,
        "dominance_found": bool(dominance),
        "structure_shift": structure_shift,
        "raw":     raw[:500] if raw else None,
        "parsed":  bool(result)
    })

    # FIX 23: reason/risk_note/confirmations straight into Koyeb logs —
    # previously only visible by digging into Redis ai_raw_log. This is
    # what answers "why did the AI WAIT here" without extra steps.
    if result:
        log.info(
            f"🧠 {result.get('signal','?')} | conf:{result.get('confidence','?')} "
            f"| reason: {result.get('reason','')}"
        )
        if result.get("risk_note"):
            log.info(f"🧠 risk_note: {result.get('risk_note')}")
        if result.get("confirmations"):
            log.info(f"🧠 confirmations: {result.get('confirmations')}")

    return result


# ═══════════════════════════════════════════════════════
# SECTION 6: SIGNAL ANALYTICS + EOD
# ═══════════════════════════════════════════════════════

def validate_decision(result, ltp, touched_zone=None, event=""):
    """
    Basic validation — check confirmations.
    FIX 16 (corrected): hard Python-side guards against sending a signal
    that's already self-invalidated by the time it fires.

    Two separate checks, deliberately scoped differently:

    1. ref_sl backstop (universal, any event) — if the AI's own ref_sl
       has already been crossed by current LTP, the trade is already
       invalidated by the AI's own stated level. Works for every event
       type since ref_sl is signal-specific, not zone-direction-specific.

    2. zone-boundary backstop (ONLY for break-continuation events:
       BREAKOUT_CANDLE / BREAKDOWN_CANDLE / COMPRESSION_BREAKOUT /
       COMPRESSION_BREAKDOWN) — for these, the trade thesis specifically
       requires LTP to stay on the broken side of the zone. This must
       NOT be applied to REJECTION/SWEEP/RETEST events, where LTP sitting
       *inside* the zone is the normal, expected setup (that's literally
       why the zone got touched and the AI call fired) — applying a
       zone-boundary check there would incorrectly reject most valid
       bounce-trade signals. This was a real bug in the first version of
       this fix, caught on recheck before it ever shipped.
    """
    if not result:
        return False

    sig   = result.get("signal", "WAIT")
    conf  = result.get("confidence", "LOW")
    confs = result.get("confirmations", [])
    count = result.get("confirmation_count", len(confs))

    if sig == "WAIT":
        return True

    ref    = result.get("reference", {})
    ref_sl = ref.get("ref_sl", 0)

    # 1. Universal ref_sl backstop
    if ref_sl:
        if sig == "BUY_CE" and ltp <= ref_sl:
            log.info(f"Rejected: BUY_CE but LTP {ltp} already at/below its own ref_sl {ref_sl}")
            return False
        if sig == "BUY_PE" and ltp >= ref_sl:
            log.info(f"Rejected: BUY_PE but LTP {ltp} already at/above its own ref_sl {ref_sl}")
            return False

    # 2. Zone-boundary backstop — break-continuation events ONLY
    BREAK_CONTINUATION_EVENTS = {
        "BREAKOUT_CANDLE", "COMPRESSION_BREAKOUT",
        "BREAKDOWN_CANDLE", "COMPRESSION_BREAKDOWN"
    }
    if touched_zone and event in BREAK_CONTINUATION_EVENTS:
        zone_low  = touched_zone.get("low", 0)
        zone_high = touched_zone.get("high", 0)
        if sig == "BUY_PE" and ltp > zone_low:
            log.info(f"Rejected: BUY_PE but LTP {ltp} already back above broken zone low {zone_low}")
            return False
        if sig == "BUY_CE" and ltp < zone_high:
            log.info(f"Rejected: BUY_CE but LTP {ltp} already back below broken zone high {zone_high}")
            return False

    if conf == "LOW":
        log.info("Rejected: LOW confidence")
        return False

    fake = {"none","na","n/a","null","","factor1","factor2","reason1","reason2"}
    real = [c for c in confs if str(c).strip().lower() not in fake and len(str(c)) > 5]

    if len(real) < MIN_CONFIRMATIONS:
        log.info(f"Rejected: only {len(real)}/{MIN_CONFIRMATIONS} real confirmations")
        return False

    return True


def run_eod_summary():
    """
    3:30 PM — send day summary to Telegram.
    FIX 5: This MUST run (and read signal_history) BEFORE closing_job()
    renames "OPEN" → "EOD_OPEN", otherwise still_open always reads 0
    even when trades were genuinely open at close.
    FIX 19: Now also shows a "Restarts Today" section from recovery_log,
    so a mid-day restart-and-recover event is visible right in the same
    summary, next to the signals that were logged before/after it — this
    is what makes a restart-test (e.g. restart at 3pm, check 3:30 summary)
    actually verifiable instead of just "trusting" nothing broke.
    """
    history = get_signal_history()
    today   = datetime.now(IST).strftime("%d %b %Y")

    buy_ce   = [s for s in history if s.get("signal") == "BUY_CE"]
    buy_pe   = [s for s in history if s.get("signal") == "BUY_PE"]
    waits    = [s for s in history if s.get("signal") == "WAIT"]
    rejected = [s for s in history if s.get("signal") == "REJECTED"]

    ref_hit    = sum(1 for s in history if s.get("result") == "REF_TARGET_HIT")
    ref_sl     = sum(1 for s in history if s.get("result") == "REF_SL_HIT")
    expired    = sum(1 for s in history if s.get("result") == "EXPIRED")
    still_open = sum(1 for s in history if s.get("result") == "OPEN")

    detail = ""
    for s in history:
        if s.get("signal") not in ["BUY_CE", "BUY_PE"]:
            continue
        r  = s.get("result","?")
        em = ("✅" if r=="REF_TARGET_HIT" else
              "❌" if r=="REF_SL_HIT" else
              "⏰" if r=="EXPIRED" else
              "⏳" if r=="OPEN" else "⚪")
        detail += f"\n{em} {s['time']} {esc(s['signal'])} @ {esc(s.get('zone_id','?'))} → {esc(r)}"

    # FIX 19: restart-recovery events today
    recoveries = get_recovery_log()
    recovery_block = ""
    if recoveries:
        recovery_block = "\n\n<b>🔁 Restarts Today:</b>"
        for rec in recoveries:
            recovery_block += (
                f"\n♻️ {rec.get('time','?')} — recovered "
                f"{rec.get('zones_recovered',0)} zones, "
                f"{rec.get('signals_recovered',0)} signals were logged so far"
            )

    msg = f"""📈 <b>DAY SUMMARY | {today}</b>

Signals scanned : {len(history)}
🟢 BUY CE: {len(buy_ce)}
🔴 BUY PE: {len(buy_pe)}
⚪ WAIT (silent)    : {len(waits)}
🚫 Rejected (silent): {len(rejected)}

<b>Ref Analytics (trades only):</b>
✅ Ref Target: {ref_hit}
❌ Ref SL    : {ref_sl}
⏰ Expired   : {expired}
⏳ Open      : {still_open}
{detail}{recovery_block}"""

    tg_send(msg)
    log.info("✅ EOD summary sent")


# ═══════════════════════════════════════════════════════
# SECTION 7: TELEGRAM
# ═══════════════════════════════════════════════════════

def tg_send(text):
    """
    FIX 4: capture + check response status code (was previously fire-
    and-forget — non-200 responses, e.g. HTML parse errors from
    unescaped AI text, went completely undetected). Callers should esc()
    any AI-generated free text before it reaches here.
    """
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code != 200:
            log.error(f"Telegram send failed {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.error(f"Telegram error: {e}")


def send_zone_brief(ctx, zones):
    """Morning/hourly zone brief"""
    today  = datetime.now(IST).strftime("%d %b %Y %H:%M")
    bias   = esc(ctx.get("bias","?"))
    struct = esc(ctx.get("structure","?"))
    dtype  = esc(ctx.get("day_type","?"))
    summ   = esc(ctx.get("summary",""))

    zone_lines = ""
    for z in zones[:8]:
        em = ("🟢" if z.get("preferred_action")=="BUY_CE" else
              "🔴" if z.get("preferred_action")=="BUY_PE" else "⚪")
        zone_lines += (
            f"\n{em} {esc(z['id'])}: {z['low']}–{z['high']} "
            f"[{esc(z['strength'])}] → {esc(z.get('preferred_action','?'))}"
            f"\n   {esc(z.get('why',''))}"
        )

    nt  = ctx.get("no_trade_zone",{})
    nt_str = (f"\n❌ No Trade: {nt.get('low')}–{nt.get('high')}"
              if nt and nt.get("low") else "")

    msg = f"""📊 <b>NIFTY ZONES | {today}</b>

Bias      : {bias}
Structure : {struct}
Day Type  : {dtype}

{summ}

<b>Active Zones:</b>{zone_lines}{nt_str}

Manual chart confirm karach trade ghe!"""

    tg_send(msg)
    log.info("✅ Zone brief sent")


def send_recovery_alert(zones, ctx, history):
    """
    FIX 19: Sent when the bot restarts mid-day and recovers today's
    zones/context from Redis instead of running a fresh morning_job().
    Keeps the user in the loop that a restart happened AND that it did
    NOT silently wipe/replace the day's zones.
    """
    now_str = datetime.now(IST).strftime("%H:%M:%S")
    bias    = esc(ctx.get("bias","?")) if ctx else "?"
    struct  = esc(ctx.get("structure","?")) if ctx else "?"

    msg = f"""♻️ <b>Bot Restarted — Recovered from Redis</b>

Time      : {now_str} IST
Zones     : {len(zones)} recovered
Signals   : {len(history)} already logged today
Bias      : {bias} | Structure: {struct}

Fresh analysis SKIPPED — using today's existing zones/context.
(Check 3:30 PM EOD summary — it will list this restart, and all
signals logged before this restart should still be counted there.)"""

    tg_send(msg)
    log.info("✅ Recovery alert sent")


def send_signal(result, ltp, touched_zone):
    """Send zone decision to Telegram"""
    sig    = result.get("signal","WAIT")
    conf   = esc(result.get("confidence","?"))
    react  = esc(result.get("zone_reaction","?"))
    reason = esc(result.get("reason",""))
    risk   = esc(result.get("risk_note",""))
    msg_h  = esc(result.get("message",""))
    confs  = result.get("confirmations",[])

    ref     = result.get("reference",{})
    ref_sl  = ref.get("ref_sl",0)
    ref_tgt = ref.get("ref_target",0)

    emoji = ("🟢" if sig=="BUY_CE" else
             "🔴" if sig=="BUY_PE" else "⚪")

    conf_lines = "\n".join([f"  ✔ {esc(c)}" for c in confs[:5] if c])
    conf_str   = f"\n{conf_lines}" if conf_lines else ""

    ref_str = ""
    if sig != "WAIT" and (ref_sl or ref_tgt):
        ref_str = f"\n\n📊 <i>Ref Analytics (not advice):</i>\nRef SL:{ref_sl} | Ref T:{ref_tgt}"

    now_str = datetime.now(IST).strftime("%H:%M")

    msg = f"""{emoji} <b>{esc(sig)} | {now_str}</b>

LTP       : {ltp}
Zone      : {esc(touched_zone.get('id'))} ({touched_zone.get('low')}–{touched_zone.get('high')})
Reaction  : {react}
Confidence: {conf}

Confirmations:{conf_str}

Reason    : {reason}
Risk Note : {risk}

{msg_h}{ref_str}

<i>Manual chart check karunch trade ghe!</i>"""

    tg_send(msg)
    log.info(f"✅ Signal sent: {sig}")


# ═══════════════════════════════════════════════════════
# SECTION 8: ORCHESTRATOR
# ═══════════════════════════════════════════════════════

def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    from datetime import time as dtime
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


def morning_job():
    """9:20 IST — First Analysis"""
    log.info("=" * 50)
    log.info("🌅 FIRST ANALYSIS JOB")
    log.info("=" * 50)
    try:
        tf_data = fetch_all_data()
        result  = run_first_analysis(tf_data, mode="FIRST")

        if not result:
            tg_send("⚠️ First analysis failed — check logs")
            return

        zones = result.get("zones", [])
        save_morning_context(result)
        save_zones(zones)

        ltp_now = get_ltp()
        if ltp_now:
            baseline = fetch_oi_chain(ltp_now)
            if baseline:
                baseline["saved_at"] = datetime.now(IST).isoformat()  # FIX 15
                _set("oi_baseline", baseline, ttl=86400)
                save_oi_snapshot(baseline)
                log.info(f"✅ OI baseline saved | PCR:{baseline.get('pcr')} ATM:{baseline.get('atm')} Expiry:{baseline.get('expiry')}")
            else:
                log.warning("OI baseline fetch failed — OI context will show N/A today")

        send_zone_brief(result, zones)

    except Exception as e:
        log.error(f"Morning job error: {e}")
        tg_send(f"⚠️ Morning job error: {esc(str(e)[:100])}")


def hourly_job():
    """Every hour at :02 — Reanalysis + zone merge"""
    if not is_market_open():
        return
    log.info("🔄 HOURLY REANALYSIS")
    try:
        ltp = get_ltp()
        if not ltp:
            return

        tf_data = fetch_all_data()
        result  = run_first_analysis(tf_data, mode="HOURLY")
        if not result:
            return

        existing  = get_zones()
        new_zones = result.get("zones", [])
        merged    = merge_zones(existing, new_zones, ltp)

        ctx = get_morning_context() or {}
        ctx.update({
            "bias":      result.get("bias", ctx.get("bias")),
            "structure": result.get("structure", ctx.get("structure")),
            "day_type":  result.get("day_type", ctx.get("day_type")),
            "summary":   result.get("summary", ctx.get("summary"))
        })

        save_morning_context(ctx)
        save_zones(merged)

        now_str = datetime.now(IST).strftime("%H:%M")
        tg_send(
            f"🔄 <b>Zone Update | {now_str}</b>\n"
            f"Bias:{esc(result.get('bias'))} | Structure:{esc(result.get('structure'))} | {len(merged)} zones active"
        )
        log.info(f"✅ Hourly zone update sent | {len(merged)} zones")

    except Exception as e:
        log.error(f"Hourly job error: {e}")


def zone_monitor_job():
    """
    Every 5:10 IST — Zone monitor with candle trigger engine.
    FIX 6 applied: OI chain fetched + snapshot saved ONCE per tick right
    after LTP, unconditionally (no longer gated behind "zone touched AND
    not NO_TRADE"). The fetched `current_oi` is then reused (not
    re-fetched) later if/when we actually need the formatted OI context
    for an AI call.
    """
    if not is_market_open():
        return

    now    = datetime.now(IST)
    hour   = now.hour
    minute = now.minute

    if hour == 9 and minute < 25:
        return
    if hour == 15 and minute > LAST_AI_CALL_MINUTE:
        log.info(f"After 15:{LAST_AI_CALL_MINUTE:02d} — no more AI calls")
        return

    try:
        ltp = get_ltp()
        if not ltp:
            return
        track_open_signals(ltp)

        # FIX 6: unconditional OI snapshot — every tick, regardless of
        # whether a zone is touched. This is what makes the 5/15/30min
        # OI history lookback actually have data to find.
        current_oi = None
        try:
            current_oi = fetch_oi_chain(ltp)
            if current_oi:
                save_oi_snapshot(current_oi)
        except Exception as oi_err:
            log.warning(f"OI fetch failed: {oi_err} — continuing without OI")

        morning_ctx = get_morning_context()
        if not morning_ctx:
            log.warning("No morning context — running first analysis")
            morning_job()
            morning_ctx = get_morning_context()
            if not morning_ctx:
                return

        log.info(f"📍 LTP: {ltp}")

        zones = get_zones()
        if not zones:
            log.info("No zones saved")
            return

        touched = get_touched_zone(ltp, zones)
        if not touched:
            log.info("No zone touched")
            return

        zone_id   = touched.get("id", "?")
        zone_type = touched.get("type", "")
        log.info(f"🎯 Zone: {zone_id} ({touched.get('low')}-{touched.get('high')}) [{zone_type}]")

        if zone_type in ["NO_TRADE", "SIDEWAYS"]:
            log.info(f"⏭️ Skipping {zone_type} zone — no AI call")
            return

        tf_data = fetch_zone_decision_data()

        event, event_reason = detect_candle_event(tf_data, touched, ltp)
        log.info(f"📊 Candle event: {event} | {event_reason}")

        if event == "NO_EVENT":
            log.info("⏭️ No candle event — skipping AI call")
            return

        updated_zone = maybe_flip_zone(touched, event)
        if updated_zone["type"] != touched["type"]:
            updated_zones = [
                updated_zone if z.get("id") == zone_id else z
                for z in zones
            ]
            save_zones(updated_zones)
            zones = updated_zones
            # FIX 20: track flip direction for real-time structure-shift
            # detection — RESISTANCE→FLIP_SUPPORT is an upward break,
            # SUPPORT→FLIP_RESISTANCE is a downward break.
            flip_direction = "up" if updated_zone["type"] == "FLIP_SUPPORT" else "down"
            save_flip_event(zone_id, flip_direction)
            log.info(
                f"🔄 Zone flipped: {zone_id} "
                f"{touched['type']} → {updated_zone['type']} @ LTP:{ltp}"
            )

        if not zone_cooldown_ok(zone_id):
            return

        oi_context = format_oi_context(current_oi) if current_oi else None
        if oi_context:
            log.info("✅ OI context ready")
        else:
            log.info("OI context unavailable — proceeding without OI")

        result = run_zone_decision(
            tf_data, ltp, updated_zone, zones,
            morning_ctx, event, event_reason, oi_context
        )

        if not result:
            return

        if not validate_decision(result, ltp, updated_zone, event):
            log.info(f"⏭️ Signal rejected (low conf / weak confirmations): {zone_id} | {event}")
            save_signal_log({
                "time":       now.strftime("%H:%M"),
                "zone_id":    zone_id,
                "event":      event,
                "signal":     "REJECTED",
                "confidence": result.get("confidence"),
                "ref_sl":     0,
                "ref_target": 0,
                "result":     "REJECTED"
            })
            mark_zone_cooldown(zone_id, "WAIT")
            return

        sig = result.get("signal", "WAIT")
        mark_zone_cooldown(zone_id, sig)

        ref = result.get("reference", {})
        save_signal_log({
            "time":       now.strftime("%H:%M"),
            "zone_id":    zone_id,
            "event":      event,
            "signal":     sig,
            "confidence": result.get("confidence"),
            "ref_sl":     ref.get("ref_sl", 0),
            "ref_target": ref.get("ref_target", 0),
            "result":     "OPEN" if sig != "WAIT" else "WAIT_SENT"
        })

        if sig in ["BUY_CE", "BUY_PE"]:
            send_signal(result, ltp, updated_zone)
        else:
            log.info("⏭️ WAIT — logged silently, no Telegram alert")

    except Exception as e:
        log.error(f"Zone monitor error: {e}")


def closing_job():
    """
    3:30 PM — EOD summary, then mark open signals expired, then flush.
    FIX 5: run_eod_summary() now runs BEFORE the OPEN→EOD_OPEN rename.
    Previously the rename happened first, so run_eod_summary()'s
    still_open count (which looks for result=="OPEN") always read 0,
    even on days with genuinely open trades at market close.
    """
    log.info("🔔 CLOSING JOB")

    run_eod_summary()

    history = get_signal_history()
    changed = False
    for s in history:
        if s.get("result") == "OPEN":
            s["result"] = "EOD_OPEN"
            changed = True
    if changed:
        _set("signal_history", history)

    flush_day()


# ═══════════════════════════════════════════════════════
# SECTION 9: FLASK + SCHEDULER + MAIN
# ═══════════════════════════════════════════════════════

def main():
    log.info("╔══════════════════════════════════════════╗")
    log.info("║  NIFTY50 ZONE ASSISTANT BOT Starting...  ║")
    log.info("╚══════════════════════════════════════════╝")

    if not UPSTOX_TOKEN:
        log.error("❌ UPSTOX_ANALYTICS_TOKEN missing!")
        return
    if not ANTHROPIC_KEY:
        log.error("❌ ANTHROPIC_API_KEY missing!")
        return
    if not TG_BOT_TOKEN:
        log.error("❌ TELEGRAM_BOT_TOKEN missing!")
        return

    now = datetime.now(IST)
    tg_send(
        f"🚀 <b>Zone Assistant Bot Started!</b>\n"
        f"Time: {now.strftime('%H:%M IST')}\n"
        f"Analysis model: {esc(ANALYSIS_MODEL)} | Decision model: {esc(DECISION_MODEL)}\n"
    )

    scheduler = BackgroundScheduler(timezone=IST)

    scheduler.add_job(
        morning_job, "cron",
        hour=9, minute=20, second=0,
        id="morning"
    )
    scheduler.add_job(
        hourly_job, "cron",
        minute=2, second=0,
        hour="10,11,12,13,14",
        id="hourly"
    )
    scheduler.add_job(
        zone_monitor_job, "cron",
        minute="0,5,10,15,20,25,30,35,40,45,50,55",
        second=10,
        hour="9,10,11,12,13,14,15",
        id="zone_monitor"
    )
    scheduler.add_job(
        closing_job, "cron",
        hour=15, minute=30, second=0,
        id="closing"
    )

    scheduler.start()
    log.info("✅ Scheduler started")
    log.info("   First analysis    : 9:20 IST")
    log.info("   Hourly reanalysis : :02 of each hour")
    log.info("   Zone monitor      : Every 5min :10sec")
    log.info("   EOD summary       : 15:30 IST")

    # FIX 19: Restart-persistence check.
    # Previously this block unconditionally called morning_job() any
    # time the process started during market hours — including a
    # mid-day restart at, say, 11:00 or 15:00. That blindly overwrote
    # today's zones/context (built from hours of accumulated hourly_job
    # merges) with a fresh from-scratch analysis, and reset the OI
    # volume baseline too. Now: if today's zones+context are already in
    # Redis (stamped with today's IST date), recover and reuse them —
    # only run a fresh morning_job() if nothing valid for today exists
    # yet (true first start, or a day where data never got saved).
    if is_market_open():
        if is_today_data_valid():
            zones   = get_zones()
            ctx     = get_morning_context()
            history = get_signal_history()
            log.info(
                f"♻️ RESTART RECOVERY: today's zones/context found in Redis "
                f"({len(zones)} zones, {len(history)} signals logged so far) "
                f"— skipping fresh morning_job()"
            )
            save_recovery_log({
                "time":              now.strftime("%H:%M:%S"),
                "zones_recovered":   len(zones),
                "signals_recovered": len(history)
            })
            send_recovery_alert(zones, ctx, history)
        else:
            log.info("Market open, no valid today's data in Redis → running first analysis now...")
            morning_job()

    flask_app = Flask(__name__)

    @flask_app.route("/")
    def index():
        return "NIFTY50 Zone Assistant Bot ✅", 200

    @flask_app.route("/health")
    def health():
        ctx     = get_morning_context()
        zones   = get_zones()
        history = get_signal_history()
        return {
            "status":          "ok",
            "time_ist":        datetime.now(IST).strftime("%d-%m-%Y %H:%M:%S"),
            "morning_context": "loaded" if ctx else "missing",
            "bias":            ctx.get("bias","?") if ctx else "?",
            "zones_active":    len(zones),
            "signals_today":   len(history),
            "restarts_today":  len(get_recovery_log()),
            "analysis_model":  ANALYSIS_MODEL,
            "decision_model":  DECISION_MODEL,
            "scheduler":       "running" if scheduler.running else "stopped"
        }, 200

    port = int(os.getenv("PORT", 8000))
    log.info(f"🌐 Flask on port {port}")
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
