"""
screen_ticker_scanner.py

Lightweight local screen OCR ticker scanner for live video or stock scanner feeds.
No cloud APIs, no data leaving your machine.

Features:
- Save all detections to CSV with full timestamps
- Filter by time window (e.g., only show tickers from last 5 minutes)
- Optional LLM-based OCR error correction (Phi-4-mini)

Install once:
    pip install mss opencv-python easyocr numpy pyyaml
    pip install phi4-mini  (for OCR correction)

Usage:
    python screen_ticker_scanner.py

Or import from another script:
    from screen_ticker_scanner import run_screen_scanner, scan_frame
"""

from __future__ import annotations

import argparse
import csv
import re
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import cv2
import easyocr
import numpy as np
import mss

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

DEFAULT_REGION = {
    "top": 200,
    "left": 300,
    "width": 1200,
    "height": 600,
}
DEFAULT_INTERVAL_SECONDS = 1.5
DEFAULT_HISTORY = 50
DEFAULT_TIME_WINDOW_MINUTES = 5
# Require at least 3 characters to reduce false positives from UI text
TICKER_PATTERN = re.compile(r"\b[A-Z]{3,5}\b")
EXCLUDE_TICKERS = {
    # Common words that OCR picks up from UI
    "AND", "FOR", "THE", "WITH", "THIS", "THAT", "FROM",
    "LIVE", "FEED", "STOCK", "MARKET", "BUY", "SELL", "HOLD",
    "STOP", "ALERT", "WATCH", "PRICE", "LIST", "SCAN", "SHOW",
    "HIDE", "OPEN", "SHOW", "HIDE", "DATA", "TIME", "VOLUME",
    "SHARE", "SHARES", "SHORT", "LONG", "GAIN", "LOSS",
    "HIGH", "LOW", "LAST", "BEST", "MORE", "LESS", "OVER",
    "UNDER", "INTO", "INTC", "LOSS", "EVEN", "EVENING",
    # Common symbols that aren't single-letter tickers but look like words
    "MOVE", "TREND", "TRENDY", "ALERT", "ALERTS", "SCANS",
    "SCANNER", "SCREEN", "PREV", "NEXT", "NEWS", "NEW",
    "YOUR", "BROWSE", "BROWSER", "ABOUT", "TOTAL", "TOTALS",
    "FIRST", "SECOND", "THIRD", "FOURTH", "MIN", "MAX",
    "MEAN", "MODE", "MEDIAN", "RANGE", "SCALE",
    # Additional screen text that appears in trading platforms
    "TRADING", "PLATFORM", "STREAM", "STREAMING",
    "MOMENTUM", "SCANNERS", "SCANNING", "SIGNALS",
    "SYMBOL", "SYMBOLS", "CURRENT", "PREVIOUS", "CHANGE",
    "PERCENT", "PERCENTAGE", "AVERAGE", "TYPICAL", "TODAY",
    "YESTERDAY", "WEEK", "WEEKLY", "MONTH", "MONTHLY",
    "YEAR", "YEARLY", "DAILY", "DAYS", "HOUR", "HOURS",
    "MINUTE", "MINUTES", "SECOND", "SECONDS", "REFRESH",
    "REFRESHING", "UPDATING", "UPDATE", "UPDATED",
    "CLICK", "CLICKS", "DRAG", "DROP", "SCROLL", "SCROLLING",
    "LEFT", "RIGHT", "CENTER", "CENTERING", "ZOOM", "ZOOMING",
    "PAGING", "PAGE", "PAGES", "LOAD", "LOADING", "LOADED",
    # Financial terms that might appear in UI
    "DIVIDEND", "DIVIDENDS", "EPS", "EARNING", "EARNINGS",
    "REVENUE", "PROFIT", "PROFITS", "VALUATION", "VALUES",
    "QUOTE", "QUOTES", "QUOTING", "BID", "BIDS", "ASK", "ASKS",
    "OFFER", "OFFERS", "TRADE", "TRADES", "TRADING",
    # Misc false positives
    "ALPHA", "BETA", "DELTA", "GAMMA", "THETA", "VEGA",
    "INDEX", "INDICES", "ETF", "ETFS", "FUND", "FUNDS",
    "CALL", "CALLS", "PUT", "PUTS", "OPTION", "OPTIONS",
    "FUTURE", "FUTURES", "FORWARD", "FORWARDS", "SWAP", "SWAPS",
}

SECTION_PATTERNS = {
    "Momentum": ["MOMENTUM"],
    "Volume Scanner": ["VOLUME SCANNER", "VOLUMN SCANNER", "VOLUME SCAN"],
    "Price Spike Scanner": ["PRICE SPIKE SCANNER", "PRICE SPIKE"],
    "Gap List": ["GAP LIST"],
    "Trending Above VWAP": ["TRENDING ABOVE VWAP"],
    "Bob's Open Watch List": ["BOB'S OPEN WATCH LIST", "BOBS OPEN WATCH LIST", "BOB OPEN WATCH LIST"],
    "Find it First Momentum": ["FIND IT FIRST MOMENTUM", "FIND IT FIRST"],
    "Volume HOD": ["VOLUME HOD", "HOD"],
}

TIMESTAMP_PATTERN = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b")

# CSV output file
DETECTIONS_CSV = Path("ticker_detections.csv")


class LLMCorrector:
    """Simple LLM-based OCR error correction for tickers using Phi-4-mini."""
    
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.llm = None
        if enabled:
            try:
                from phi4_mini import Phi4Mini
                self.llm = Phi4Mini()
                print("✓ Phi-4-mini LLM loaded for OCR correction")
            except ImportError:
                print("⚠ Phi-4-mini not available, OCR correction disabled")
                self.enabled = False
    
    def correct(self, ticker: str) -> str:
        """Try to correct a potentially misread ticker using LLM."""
        if not self.enabled or not self.llm:
            return ticker
        
        # Skip if already a valid-looking ticker
        if self._looks_valid(ticker):
            return ticker
        
        prompt = f"""The following text was read from a stock trading screen using OCR.
It may contain OCR errors. The text is: "{ticker}"

If this looks like a valid US stock ticker symbol (1-5 uppercase letters), return it corrected.
If it's just a regular word from the UI (like "AND", "THE", "VOLUME"), return "INVALID".
If uncertain, return the most likely stock ticker.

Respond with ONLY the corrected ticker or INVALID, nothing else."""

        try:
            response = self.llm.generate(prompt, max_tokens=10).strip().upper()
            if response == "INVALID":
                return ""
            # Validate response is a reasonable ticker
            if 1 <= len(response) <= 5 and response.isalpha():
                return response
        except Exception:
            pass
        return ticker
    
    def _looks_valid(self, ticker: str) -> bool:
        """Check if ticker looks valid without LLM."""
        if len(ticker) < 1 or len(ticker) > 5:
            return False
        if not ticker.isalpha():
            return False
        if ticker in EXCLUDE_TICKERS:
            return False
        return True


def load_config(path: Path | str | None = None) -> dict[str, Any]:
    if path is None or yaml is None:
        return {}

    config_path = Path(path)
    if not config_path.exists():
        return {}

    try:
        with config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def create_ocr_reader(language: str = "en", gpu: bool = False) -> easyocr.Reader:
    return easyocr.Reader([language], gpu=gpu)


def preprocess_frame(frame: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return thresh


def normalize_tickers(raw_tickers: list[str], llm: LLMCorrector | None = None) -> list[str]:
    tickers: list[str] = []
    for ticker in raw_tickers:
        ticker = ticker.strip().upper()
        if len(ticker) < 1 or len(ticker) > 5:
            continue
        if ticker in EXCLUDE_TICKERS:
            continue
        if ticker.isalpha():
            # Apply LLM correction if available
            if llm and llm.enabled:
                corrected = llm.correct(ticker)
                if corrected and corrected not in EXCLUDE_TICKERS:
                    tickers.append(corrected)
            else:
                tickers.append(ticker)
    return tickers


def extract_section(text: str) -> str | None:
    normalized = re.sub(r"[^A-Z0-9 ]", "", text.upper())
    for section, patterns in SECTION_PATTERNS.items():
        for pattern in patterns:
            if pattern in normalized:
                return section
    return None


def extract_timestamp(text: str) -> str | None:
    match = TIMESTAMP_PATTERN.search(text)
    return match.group(0) if match else None


def save_detection_to_csv(ticker: str, section: str | None, raw_text: str, timestamp: str):
    """Save a ticker detection to CSV with full timestamp."""
    now = datetime.now()
    row = [
        now.strftime("%Y-%m-%d"),
        now.strftime("%H:%M:%S.%f")[:-3],  # timestamp with ms
        ticker,
        section or "",
        raw_text[:100],  # truncate raw text
    ]
    with open(DETECTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write header if file is new
        if DETECTIONS_CSV.stat().st_size == 0:
            writer.writerow(["date", "time", "ticker", "section", "raw_text"])
        writer.writerow(row)


def load_recent_detections(time_window_minutes: int = 5) -> dict[str, datetime]:
    """Load tickers detected within the time window."""
    if not DETECTIONS_CSV.exists():
        return {}
    
    cutoff = datetime.now() - timedelta(minutes=time_window_minutes)
    recent: dict[str, datetime] = {}
    
    try:
        with open(DETECTIONS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    dt = datetime.strptime(f"{row['date']} {row['time']}", "%Y-%m-%d %H:%M:%S.%f")
                    if dt >= cutoff:
                        ticker = row['ticker']
                        if ticker not in recent or dt > recent[ticker]:
                            recent[ticker] = dt
                except (ValueError, KeyError):
                    continue
    except Exception:
        pass
    
    return recent


def scan_frame(
    frame: np.ndarray,
    reader: easyocr.Reader,
    detail: bool = False,
    llm: LLMCorrector | None = None,
) -> list[str] | list[dict[str, str | None]]:
    processed = preprocess_frame(frame)
    results = reader.readtext(processed, detail=1, paragraph=False)

    entries: list[dict[str, str | None]] = []
    current_section: str | None = None
    current_timestamp: str | None = None

    for item in results:
        if not item or len(item) < 2:
            continue
        text = item[1].strip()
        if not text:
            continue
        upper_text = text.upper()

        section = extract_section(upper_text)
        if section:
            current_section = section

        timestamp = extract_timestamp(upper_text)
        if timestamp:
            current_timestamp = timestamp

        raw = TICKER_PATTERN.findall(upper_text)
        tickers = normalize_tickers(raw, llm)
        for ticker in tickers:
            entries.append(
                {
                    "ticker": ticker,
                    "section": current_section,
                    "timestamp": current_timestamp,
                    "text": text,
                }
            )

    if detail:
        return entries
    return [entry["ticker"] for entry in entries]


def capture_region(region: dict[str, int]) -> np.ndarray:
    with mss.mss() as sct:
        screenshot = sct.grab(region)
        return cv2.cvtColor(np.array(screenshot), cv2.COLOR_BGRA2BGR)


def select_screen_region() -> dict[str, int] | None:
    with mss.mss() as sct:
        monitor = sct.monitors[0]
        screenshot = sct.grab(monitor)
    frame = cv2.cvtColor(np.array(screenshot), cv2.COLOR_BGRA2BGR)
    clone = frame.copy()

    selection: dict[str, int | tuple[int, int] | None] = {
        "start": None,
        "end": None,
    }
    selection_region: dict[str, int] | None = None
    drawing = False

    def mouse_callback(event, x, y, flags, param):
        nonlocal drawing, selection_region
        if event == cv2.EVENT_LBUTTONDOWN:
            selection["start"] = (x, y)
            selection["end"] = (x, y)
            drawing = True
        elif event == cv2.EVENT_MOUSEMOVE and drawing:
            selection["end"] = (x, y)
        elif event == cv2.EVENT_LBUTTONUP:
            drawing = False
            selection["end"] = (x, y)
            if selection["start"] and selection["end"]:
                x1, y1 = selection["start"]
                x2, y2 = selection["end"]
                left = min(x1, x2)
                top = min(y1, y2)
                width = abs(x2 - x1)
                height = abs(y2 - y1)
                if width > 0 and height > 0:
                    selection_region = {
                        "top": int(top),
                        "left": int(left),
                        "width": int(width),
                        "height": int(height),
                    }

    window_name = "Select OCR Region (S=save, Q=cancel)"
    try:
        cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
        cv2.resizeWindow(window_name, 1024, 768)
        cv2.setMouseCallback(window_name, mouse_callback)
    except cv2.error:
        print(
            "OpenCV GUI support is unavailable. Falling back to manual region input."
        )
        try:
            top = int(input("Enter top: ").strip())
            left = int(input("Enter left: ").strip())
            width = int(input("Enter width: ").strip())
            height = int(input("Enter height: ").strip())
            return {"top": top, "left": left, "width": width, "height": height}
        except Exception as exc:
            raise RuntimeError(
                "Failed to read manual region values: " + str(exc)
            ) from exc

    while True:
        display = clone.copy()
        if selection["start"] and selection["end"]:
            cv2.rectangle(
                display,
                tuple(selection["start"]),
                tuple(selection["end"]),
                (0, 255, 0),
                2,
            )
        cv2.putText(
            display,
            "Drag to select region, press S to save, Q to cancel",
            (10, 30),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (255, 255, 255),
            2,
            cv2.LINE_AA,
        )
        if selection_region:
            text = f"Selected: {selection_region['left']}x{selection_region['top']} {selection_region['width']}x{selection_region['height']}"
            cv2.putText(
                display,
                text,
                (10, 60),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.7,
                (0, 255, 0),
                2,
                cv2.LINE_AA,
            )

        cv2.imshow(window_name, display)
        key = cv2.waitKey(10) & 0xFF
        if key == ord("q"):
            selection_region = None
            break
        if key == ord("s"):
            if selection_region is not None:
                break

    cv2.destroyAllWindows()
    return selection_region


def run_screen_scanner(
    region: dict[str, int] | None = None,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    max_history: int = DEFAULT_HISTORY,
    language: str = "en",
    gpu: bool = False,
    show_preview: bool = True,
    use_llm: bool = True,
    time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES,
) -> None:
    region = region or DEFAULT_REGION
    reader = create_ocr_reader(language=language, gpu=gpu)
    llm = LLMCorrector(enabled=use_llm)
    seen_tickers = deque(maxlen=max_history)
    
    # Load recent detections for time-window filtering
    recent_tickers = load_recent_detections(time_window_minutes)
    
    print(f"Loading EasyOCR... (this happens only once)")
    print(f"Watching region: {region}")
    print(f"Time window: {time_window_minutes} minutes")
    print(f"LLM correction: {'enabled' if use_llm else 'disabled'}")

    with mss.mss() as sct:
        while True:
            start_time = time.time()
            screenshot = sct.grab(region)
            frame = cv2.cvtColor(np.array(screenshot), cv2.COLOR_BGRA2BGR)
            tickers = scan_frame(frame, reader, llm=llm)

            new_tickers = [t for t in tickers if t not in seen_tickers]
            if new_tickers:
                timestamp = time.strftime("%H:%M:%S")
                print(f"🔔 NEW TICKERS @ {timestamp}: {', '.join(new_tickers)}")
                seen_tickers.extend(new_tickers)
                
                # Save to CSV
                for ticker in new_tickers:
                    save_detection_to_csv(ticker, None, "", timestamp)
                
                # Show filtered view (only recent)
                recent = load_recent_detections(time_window_minutes)
                print(f"   Recent (last {time_window_minutes}m): {list(recent.keys())}")

            if show_preview:
                cv2.imshow("Ticker Scanner Preview (press Q to quit)", frame)
                if cv2.waitKey(1) & 0xFF == ord("q"):
                    break

            elapsed = time.time() - start_time
            time.sleep(max(0, interval_seconds - elapsed))

    if show_preview:
        cv2.destroyAllWindows()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local screen OCR ticker scanner for live video feeds",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_REGION["top"],
        help="Top pixel of capture region",
    )
    parser.add_argument(
        "--left",
        type=int,
        default=DEFAULT_REGION["left"],
        help="Left pixel of capture region",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=DEFAULT_REGION["width"],
        help="Width of capture region",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=DEFAULT_REGION["height"],
        help="Height of capture region",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Seconds between scans",
    )
    parser.add_argument(
        "--history",
        type=int,
        default=DEFAULT_HISTORY,
        help="History size for duplicate suppression",
    )
    parser.add_argument(
        "--time-window",
        type=int,
        default=DEFAULT_TIME_WINDOW_MINUTES,
        help="Time window in minutes for filtering",
    )
    parser.add_argument(
        "--gpu",
        action="store_true",
        help="Enable GPU mode for EasyOCR if available",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM-based OCR correction",
    )
    parser.add_argument(
        "--no-preview",
        action="store_true",
        help="Disable the CV2 preview window",
    )
    parser.add_argument(
        "--select-region",
        action="store_true",
        help="Open an interactive screen region selector and print the selected coordinates",
    )
    parser.add_argument(
        "--view-recent",
        type=int,
        const=5,
        nargs="?",
        help="View tickers detected in the last N minutes (default: 5)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: capture one frame, show all detected tickers with context",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate mode: show tickers with raw OCR text for verification",
    )
    return parser.parse_args()


def view_recent_detections(minutes: int = 5):
    """Display tickers detected within the time window."""
    recent = load_recent_detections(minutes)
    if not recent:
        print(f"No tickers detected in the last {minutes} minutes.")
        return
    
    print(f"\nTicker detections in last {minutes} minutes:")
    print("-" * 50)
    for ticker, dt in sorted(recent.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ticker:<8} at {dt.strftime('%H:%M:%S')}")
    print("-" * 50)
    print(f"Total unique tickers: {len(recent)}")


def test_scanner(region: dict[str, int], use_llm: bool = True):
    """Test mode: capture one frame, show all detected tickers with context."""
    print(f"Testing scanner on region: {region}")
    print("=" * 60)
    
    reader = create_ocr_reader()
    llm = LLMCorrector(enabled=use_llm)
    
    # Capture frame
    frame = capture_region(region)
    print(f"Frame captured: {frame.shape[1]}x{frame.shape[0]}")
    
    # Preprocess
    processed = preprocess_frame(frame)
    
    # Get all OCR results
    results = reader.readtext(processed, detail=1, paragraph=False)
    
    print(f"\nRaw OCR detections: {len(results)}")
    print("-" * 60)
    
    # Show all raw text with confidence
    all_entries = []
    for item in results:
        if len(item) >= 3:
            bbox, text, confidence = item[0], item[1], item[2]
            text = text.strip()
            if text:
                all_entries.append((text, confidence, bbox))
    
    print("\n📝 RAW OCR TEXT (sorted by position):")
    print("-" * 60)
    for text, conf, bbox in sorted(all_entries, key=lambda x: (x[2][0][1], x[2][0][0])):
        print(f"  [{conf:.2f}] {text}")
    
    # Now show ticker extraction
    entries = scan_frame(frame, reader, detail=True, llm=llm)
    unique_tickers = {}
    for entry in entries:
        ticker = entry["ticker"]
        if ticker not in unique_tickers:
            unique_tickers[ticker] = entry
    
    print(f"\n🔍 EXTRACTED TICKERS: {len(unique_tickers)}")
    print("-" * 60)
    for ticker, entry in unique_tickers.items():
        print(f"  {ticker:<8} | section: {entry.get('section', 'N/A') or 'N/A':<20} | raw: {entry.get('text', '')[:50]}")
    
    print("\n" + "=" * 60)
    print("VALIDATION: Compare extracted tickers above with what you see on screen")
    
    # Show excluded
    excluded = set()
    for text, conf, bbox in all_entries:
        matches = TICKER_PATTERN.findall(text.upper())
        for m in matches:
            if m in EXCLUDE_TICKERS:
                excluded.add(m)
    
    if excluded:
        print(f"\n🚫 EXCLUDED (UI text): {sorted(excluded)}")
    
    return unique_tickers


def validate_scanner(region: dict[str, int], use_llm: bool = True):
    """Validate mode: show tickers with raw OCR text for verification."""
    print(f"Validating scanner on region: {region}")
    print("=" * 60)
    print("Take a screenshot of your trading platform and run this test.")
    print("=" * 60)
    
    return test_scanner(region, use_llm)


def main() -> None:
    args = parse_args()
    
    # Handle --view-recent flag
    if args.view_recent is not None:
        view_recent_detections(args.view_recent)
        return
    
    # Handle --test flag
    if args.test:
        region = {
            "top": args.top,
            "left": args.left,
            "width": args.width,
            "height": args.height,
        }
        test_scanner(region, use_llm=not args.no_llm)
        return
    
    # Handle --validate flag
    if args.validate:
        region = {
            "top": args.top,
            "left": args.left,
            "width": args.width,
            "height": args.height,
        }
        validate_scanner(region, use_llm=not args.no_llm)
        return
    
    if args.select_region:
        selected = select_screen_region()
        if selected is None:
            print("No region selected")
            return
        print(
            f"Selected region: top={selected['top']}, left={selected['left']}, width={selected['width']}, height={selected['height']}"
        )
        return

    region = {
        "top": args.top,
        "left": args.left,
        "width": args.width,
        "height": args.height,
    }
    run_screen_scanner(
        region=region,
        interval_seconds=args.interval,
        max_history=args.history,
        gpu=args.gpu,
        show_preview=not args.no_preview,
        use_llm=not args.no_llm,
        time_window_minutes=args.time_window,
    )


if __name__ == "__main__":
    main()