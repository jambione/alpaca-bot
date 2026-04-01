"""
trending.py — Trending watchlist fetchers (StockTwits + Finviz)

Extracted from alpaca_dashboard.py to keep the main module lean.

Initialise once at startup by calling:
    import trending
    trending.init(STATE, dlog)

All public functions are then usable without arguments beyond their
documented parameters.
"""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ── Module-level state injected by alpaca_dashboard.py ──────────────────────
_state  = None  # BotState instance
_log    = None  # DashboardHandler instance


def init(state, logger):
    """Call once from alpaca_dashboard after BotState is created."""
    global _state, _log
    _state = state
    _log   = logger


# ── Helper (local copy of alpaca_dashboard._get_feed_arg) ───────────────────
def _get_feed_arg(cfg: dict = None) -> dict:
    """Return the correct Alpaca DataFeed kwarg based on config."""
    try:
        from alpaca.data.enums import DataFeed as _DF
        cfg = cfg or (_state.config if _state else {})
        feed_name = cfg.get("data_feed", "IEX").upper()
        feed = _DF.SIP if feed_name == "SIP" else _DF.IEX
        return {"feed": feed}
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════════
#  STOCKTWITS TRENDING  (sentiment page scraper)
#  The old /api/2/trending/symbols.json endpoint locked down in 2024.
#  We now scrape stocktwits.com/sentiment instead — no auth needed.
# ═══════════════════════════════════════════════════════════

_ST_SENTIMENT_URL = "https://stocktwits.com/sentiment"
_ST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Legacy Yahoo constants (kept for reference — function currently unused)
_YAHOO_TRENDING_URLS = [
    "https://query1.finance.yahoo.com/v1/finance/trending/US?count=20",
    "https://query2.finance.yahoo.com/v1/finance/trending/US?count=20",
]
_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


def _walk_json_for_tickers(obj, depth: int = 0, max_depth: int = 7) -> list:
    """Recursively extract stock ticker values from a nested JSON object."""
    if depth > max_depth:
        return []
    out = []
    if isinstance(obj, dict):
        for key in ("symbol", "ticker", "sym"):
            val = obj.get(key, "")
            if isinstance(val, str):
                sym = val.strip().upper()
                if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                    out.append(sym)
        for v in obj.values():
            out.extend(_walk_json_for_tickers(v, depth + 1, max_depth))
    elif isinstance(obj, list):
        for item in obj[:200]:
            out.extend(_walk_json_for_tickers(item, depth + 1, max_depth))
    seen: set = set()
    return [t for t in out if not (t in seen or seen.add(t))]


def fetch_stocktwits_trending() -> list:
    """
    Pull trending tickers from the StockTwits sentiment page.

    Strategy 1 — __NEXT_DATA__ JSON blob (Next.js): fastest and most reliable.
    Strategy 2 — BeautifulSoup link scan: fallback if Next.js payload changes.
    Both strategies filter to plain 2-5 char alpha US equity symbols only.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as ie:
        _log.warning(f"[TREND] StockTwits scraper requires 'requests' + 'beautifulsoup4': {ie}")
        return []

    try:
        resp = requests.get(_ST_SENTIMENT_URL, headers=_ST_HEADERS, timeout=12)
        if resp.status_code != 200:
            _log.debug(f"[TREND] StockTwits unavailable (HTTP {resp.status_code}) — using Finviz only")
            return []

        soup    = BeautifulSoup(resp.text, "html.parser")
        tickers: list = []

        # ── Strategy 1: __NEXT_DATA__ JSON (Next.js embed) ────────────────
        nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if nd_tag and nd_tag.string:
            try:
                nd    = json.loads(nd_tag.string)
                props = nd.get("props", {}).get("pageProps", {})
                for key in ("trendingSymbols", "trending", "symbols",
                            "rankings", "data", "sentimentData"):
                    items = props.get(key, [])
                    if isinstance(items, list) and items:
                        for item in items:
                            sym = str(
                                item.get("symbol") or item.get("ticker") or ""
                            ).strip().upper()
                            if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                                tickers.append(sym)
                        if tickers:
                            break
                if not tickers:
                    tickers = _walk_json_for_tickers(nd)
            except Exception as je:
                _log.debug(f"[TREND] ST __NEXT_DATA__ parse error: {je}")

        # ── Strategy 2: BeautifulSoup — /symbol/TICKER hrefs ──────────────
        if not tickers:
            seen: set = set()
            for a in soup.find_all("a", href=True):
                href = str(a["href"])
                if "/symbol/" in href:
                    sym = href.split("/symbol/")[-1].split("?")[0].split("/")[0].upper()
                    if sym and sym.isalpha() and 2 <= len(sym) <= 5 and sym not in seen:
                        tickers.append(sym)
                        seen.add(sym)

        if not tickers:
            _log.warning("[TREND] StockTwits: no tickers found — page structure may have changed")
            return []

        _log.debug(f"[TREND] StockTwits raw ({len(tickers)} equities): {', '.join(tickers[:20])}")
        return tickers

    except Exception as e:
        _log.error(f"[TREND] StockTwits sentiment fetch failed: {e}")
        return []


# ── kept for reference — replaced by fetch_stocktwits_trending() above ──
def _fetch_yahoo_trending_UNUSED() -> list:
    """
    Pull trending US tickers from Yahoo Finance (no API key required).

    Endpoint: /v1/finance/trending/US — returns the tickers users are actively
    viewing on Yahoo Finance right now, a good proxy for social momentum.
    Falls back to the secondary query server if the first is unavailable.
    """
    for url in _YAHOO_TRENDING_URLS:
        try:
            req = urllib.request.Request(url, headers=_YAHOO_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    continue
                data = json.loads(resp.read().decode())
            result = data.get("finance", {}).get("result", [])
            if not result:
                _log.warning("[TREND] Yahoo trending: empty result payload")
                continue
            quotes  = result[0].get("quotes", [])
            tickers = []
            for q in quotes:
                sym = str(q.get("symbol", "")).strip().upper()
                # Plain US equity symbols only — skip crypto (BTC-USD), ETFs, warrants
                if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                    tickers.append(sym)
            _log.debug(f"[TREND] Yahoo raw ({len(tickers)} equities): {', '.join(tickers[:20])}")
            return tickers
        except Exception as e:
            _log.debug(f"[TREND] Yahoo trending error ({url}): {e}")
            continue
    _log.error("[TREND] Yahoo trending fetch failed — both endpoints unreachable")
    return []


def fetch_finviz_trending() -> list:
    """
    Pull momentum tickers from the Finviz screener using finvizfinance.

    Filters are driven by config keys:
        finviz_exchange    — "NASDAQ", "NYSE", "AMEX", or "" for all
        finviz_performance — Finviz Performance label, e.g. "Week Up"
        finviz_rel_volume  — Relative Volume label, e.g. "Over 2"
        finviz_avg_volume  — Average Volume label, e.g. "Over 500K"
        finviz_float       — Float label, e.g. "Under 20M"
        finviz_signal      — reserved / not supported by finvizfinance (keep "")
        trending_max_price — upper price cap (shared with StockTwits + Finviz filter)
        finviz_include_news — if True, log top Finviz market news headlines

    Requires:  pip install finvizfinance
    """
    try:
        from finvizfinance.screener.overview import Overview
    except ImportError:
        _log.warning("[TREND] finvizfinance not installed — skipping Finviz source  "
                     "(pip install finvizfinance)")
        return []

    cfg = _state.config

    try:
        from finvizfinance.screener.overview import Overview
        fov = Overview()

        max_price   = cfg.get("trending_max_price", 5.0)
        exchange    = cfg.get("finviz_exchange",    "NASDAQ")
        performance = cfg.get("finviz_performance", "Week Up")
        rel_volume  = cfg.get("finviz_rel_volume",  "Over 2")
        avg_volume  = cfg.get("finviz_avg_volume",  "Over 500K")
        float_size  = cfg.get("finviz_float",       "Under 20M")
        signal      = cfg.get("finviz_signal",      "")   # not supported by finvizfinance

        # ── Build filter dict ─────────────────────────────────────────────
        price_str = f"Under ${int(max_price)}" if max_price == int(max_price) else f"Under ${max_price}"
        filters: dict = {
            "Price":           price_str,
            "Relative Volume": rel_volume,
            "Performance":     performance,
        }
        if exchange:    filters["Exchange"]       = exchange
        if float_size:  filters["Float"]          = float_size
        if avg_volume:  filters["Average Volume"] = avg_volume
        if signal:      filters["Signal"]         = signal

        _log.debug(f"[TREND] Finviz screener filters: {filters}")
        fov.set_filter(filters_dict=filters)
        df = fov.screener_view()

        if df is None or df.empty:
            _log.warning("[TREND] Finviz screener returned no results")
            return []

        # Normalise column name — finvizfinance uses 'Ticker' in recent versions
        ticker_col = next((c for c in df.columns if c.lower() == "ticker"), None)
        if ticker_col is None:
            _log.warning(f"[TREND] Finviz screener: no Ticker column found (cols={list(df.columns)})")
            return []

        tickers = [
            str(t).strip().upper()
            for t in df[ticker_col].dropna().tolist()
            if str(t).strip().isalpha() and 2 <= len(str(t).strip()) <= 5
        ]

        _log.debug(f"[TREND] Finviz raw ({len(tickers)} equities): {', '.join(tickers[:25])}")

        # ── Optional: log top market headlines for context ─────────────────
        if cfg.get("finviz_include_news", True):
            try:
                from finvizfinance.news import News
                news_obj = News()
                all_news = news_obj.get_news()
                headlines = []
                if isinstance(all_news, dict):
                    for key in ("news", "blogs"):
                        ndf2 = all_news.get(key)
                        if ndf2 is not None and not ndf2.empty:
                            title_col = next((c for c in ndf2.columns if "title" in c.lower()), None)
                            if title_col:
                                headlines += ndf2[title_col].head(3).tolist()
                if headlines:
                    _log.debug("[TREND] Finviz headlines: " +
                               "  |  ".join(str(h)[:90] for h in headlines[:5]))
            except Exception as ne:
                _log.debug(f"[TREND] Finviz news fetch skipped: {ne}")

        return tickers

    except Exception as e:
        _log.error(f"[TREND] Finviz fetch failed: {e}")
        return []


def filter_under_five(tickers: list, data_client, max_price: float = None) -> list:
    """Return only tickers whose latest ask/bid price is at or under max_price."""
    if not tickers or data_client is None:
        return tickers
    if max_price is None:
        max_price = _state.config.get("trending_max_price", 5.0)
    cfg = _state.config
    try:
        from alpaca.data.requests import StockLatestQuoteRequest
        req    = StockLatestQuoteRequest(symbol_or_symbols=tickers, **_get_feed_arg(cfg))
        quotes = data_client.get_stock_latest_quote(req)
        result    = []
        filtered  = []
        price_map = {}
        for t in tickers:
            q = quotes.get(t)
            if q:
                price = float(q.ask_price or q.bid_price or 0)
                if 0 < price < max_price:   # strictly under — e.g. 5.0 means <$5.00
                    result.append(t)
                    price_map[t] = round(price, 4)
                else:
                    filtered.append(f"{t}(${price:.2f})")
            else:
                filtered.append(f"{t}(no quote)")
        if filtered:
            _log.debug(f"[TREND] Filtered out (>${max_price}): {', '.join(filtered)}")
        # Persist prices in state for UI tooltip display
        with _state.lock:
            _state.trending_prices = price_map
        return result
    except Exception as e:
        # Unauthorized means data client not ready yet — silent fallback, not an error
        err_str = str(e)
        if "401" in err_str or "unauthorized" in err_str.lower():
            _log.debug(f"[TREND] Price filter skipped — data client not ready")
        else:
            _log.warning(f"[TREND] Price filter failed: {e}")
        return tickers   # fallback: return unfiltered


def update_trending_watchlist() -> list:
    """
    Fetch trending stocks from StockTwits + Finviz (if enabled),
    merge/deduplicate, filter by trending_max_price, then update _state.trending_tickers.
    Called on startup and every 5 minutes by trending_updater().

    Source priority: StockTwits first (social momentum), then Finviz extras appended.
    """
    cfg = _state.config

    # ── Fetch from each source ─────────────────────────────────────────
    st_raw = fetch_stocktwits_trending()
    fv_raw = fetch_finviz_trending() if cfg.get("finviz_enabled", True) else []

    # ── Merge — StockTwits first, then Finviz additions, deduplicated ──
    st_set = set(st_raw)
    fv_set = set(fv_raw)
    raw_sources: dict = {}
    seen: set = set()
    raw:  list = []
    for t in st_raw + fv_raw:
        if t not in seen:
            seen.add(t)
            raw.append(t)
        # Determine source label (may upgrade to "both" if in both lists)
        if t in st_set and t in fv_set:
            raw_sources[t] = "both"
        elif t in st_set:
            raw_sources[t] = "stocktwits"
        else:
            raw_sources[t] = "finviz"

    if not raw:
        _log.warning("[TREND] No tickers returned from any source — watchlist unchanged")
        with _state.lock:
            return list(_state.trending_tickers)

    # ── Price filter via Alpaca quotes ─────────────────────────────────
    from alpaca_api import connect_alpaca  # lazy import to avoid circular at module load
    dc = _state.data_client
    if dc is None:
        try:
            if cfg.get("api_key") and "YOUR_" not in cfg["api_key"]:
                _, dc = connect_alpaca(cfg)
        except Exception:
            pass

    max_p    = cfg.get("trending_max_price", 5.0)
    filtered = filter_under_five(raw, dc, max_price=max_p) if dc else raw

    if filtered:
        ts = datetime.now(ET).strftime("%H:%M:%S ET")
        filtered_sources = {t: raw_sources.get(t, "unknown") for t in filtered}
        with _state.lock:
            _state.trending_tickers = filtered
            _state.trending_sources = filtered_sources
            _state.trending_updated = ts
        n_st   = sum(1 for s in filtered_sources.values() if s == "stocktwits")
        n_fv   = sum(1 for s in filtered_sources.values() if s == "finviz")
        n_both = sum(1 for s in filtered_sources.values() if s == "both")
        src_str = "  ".join(filter(None, [
            f"{n_st}ST"   if n_st   else "",
            f"{n_fv}FV"   if n_fv   else "",
            f"{n_both}✦"  if n_both else "",
        ]))
        _log.info(f"[TREND] {len(filtered)} tickers ({src_str}) — {', '.join(filtered)}")
    return filtered


def trending_updater():
    """
    Background thread — refresh trending watchlist on a market-hours schedule.

    Schedule:
      • First fetch at 9:15 AM ET (or immediately if already past 9:15 during market hours)
      • Then every 5 minutes aligned to the clock: 9:20, 9:25, 9:30 … 15:55
      • No fetches outside 9:15 AM–4:00 PM ET, or on weekends
    """
    _log.info("[TREND] Trending updater started — waiting for market hours (9:15 AM ET)")

    while True:
        now  = datetime.now(ET)
        wday = now.weekday()   # 0=Mon … 6=Sun

        # ── Weekend — sleep until Monday ────────────────────────────
        if wday >= 5:
            days_ahead = 7 - wday   # 5→2, 6→1
            next_monday = (now + timedelta(days=days_ahead)).replace(
                hour=9, minute=15, second=0, microsecond=0)
            wait = (next_monday - now).total_seconds()
            _log.info(f"[TREND] Weekend — next fetch {next_monday.strftime('%a %m/%d %H:%M ET')}")
            time.sleep(max(wait, 60))
            continue

        # ── Weekday boundaries ───────────────────────────────────────
        session_start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        session_end   = now.replace(hour=16, minute=0,  second=0, microsecond=0)

        if now < session_start:
            wait = (session_start - now).total_seconds()
            _log.info(f"[TREND] Pre-market — first fetch at 9:15 AM ET ({wait/60:.0f} min away)")
            time.sleep(min(wait, 60))
            continue

        if now >= session_end:
            # After hours — sleep until next trading day 9:15 AM
            if wday == 4:   # Friday → Monday
                days_ahead = 3
            else:
                days_ahead = 1
            next_open = (now + timedelta(days=days_ahead)).replace(
                hour=9, minute=15, second=0, microsecond=0)
            wait = (next_open - now).total_seconds()
            _log.info(f"[TREND] Market closed — next fetch {next_open.strftime('%a %m/%d %H:%M ET')}")
            time.sleep(min(wait, 300))
            continue

        # ── In session — fetch now ───────────────────────────────────
        try:
            update_trending_watchlist()
        except Exception as e:
            _log.error(f"[TREND] Updater error: {e}")

        # Sleep until the next 5-minute clock boundary (9:20, 9:25, 9:30…)
        now         = datetime.now(ET)
        interval    = 5  # minutes between trending fetches
        mins_past   = now.minute % interval
        secs_to_next = (interval - mins_past) * 60 - now.second
        if secs_to_next <= 5:          # avoid a near-zero sleep causing a double-fire
            secs_to_next += 300
        next_fire = now + timedelta(seconds=secs_to_next)
        _log.info(f"[TREND] Next trending fetch at {next_fire.strftime('%H:%M:%S ET')}")
        time.sleep(secs_to_next)
