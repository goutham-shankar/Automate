import sys
from pathlib import Path
from datetime import datetime, date
import csv
import json
import os

# Add src to sys.path
sys.path.append(str(Path(__file__).parent / "src"))
from nse import NSE

# Stock watchlist
WATCHLIST = [
    "360ONE", "ABB", "ABBOTINDIA", "ABCAPITAL", "ABFRL", "ALKEM", "AMBER",
    "ANGELONE", "APLAPOLLO", "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY",
    "ASTRAL", "ATGL", "AUBANK", "AUROPHARMA", "BAJAJ-AUTO", "BAJAJFINSV",
    "BAJAJHLDNG", "BAJFINANCE", "BALKRISIND", "BALRAMCHIN", "BANDHANBNK",
    "BANKBARODA", "BANKINDIA", "BDL", "BERGEPAINT", "BHARATFORG",
    "BHARTIARTL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA",
    "BSE", "CAMS", "CANBK", "CANFINHOME", "CASGP", "CDSL", "CESC",
    "CGPOWER", "CHAMBLFERT", "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE",
    "COLPAL", "CONCOR", "COROMANDEL", "CROMPTON", "CUMMINSIND", "DABUR",
    "DALBHARAT", "DEEPAKNTR", "DELHIVERY", "DIVISLAB", "DIXON", "DLF",
    "DMART", "DRREDDY", "EICHERMOT", "ESCORTS", "EXIDEIND", "FEDERALBNK",
    "FORTIS", "GAIL", "GLAN", "GLENMARK", "GMRINFRA", "GODREJCP",
    "GODREJPROP", "GRASIM", "GUJGASLTD", "HAL", "HAVELLS", "HCLTECH",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDCOPPER", "HINDPETRO",
    "HINDUNILVR", "HINDZINC", "HUDCO", "ICICIGI", "ICICIPRULI", "IDFC",
    "IDFCFIRSTB", "IEX", "IGL", "IIFL", "INDHOTEL", "INDIACEM", "INDIAMART",
    "INDIGO", "INDUSINDBK", "INDUSTOWER", "INOXWIND", "IOC", "IPCALAB",
    "IREDA", "IRFC", "JINDALSTEL", "JIOFIN", "JKCEMENT", "JSWENERGY",
    "JSWSTEEL", "JUBLFOOD", "KAYNES", "KEE", "KFINTECH", "L&TFH",
    "LALPATHLAB", "LICHSGFIN", "LICI", "LODHA", "LT", "LTIM", "LTTS",
    "LUPIN", "M&M", "M&MFIN", "MANAPPURAM", "MANKIND", "MARICO", "MAZDOCK",
    "MCX", "METROPOLIS", "MFSL", "MGL", "MOTHERSON", "MPHASIS", "MUTHOOTFIN",
    "NATIONALUM", "NAVINFLUOR", "NBCC", "NESTLEIND", "NHPC", "NMDC", "NTPC",
    "NUVAMA", "OBEROIRLTY", "OFSS", "ONGC", "PAGEIND", "PATANJALI", "PEL",
    "PERSISTENT", "PETRONET", "PFC", "PGEL", "PGHH", "PHOENIXLTD",
    "PIDILITIND", "PIIND", "PNB", "PNBHOUSING", "POLYCAB", "POWERGRID",
    "POWERINDIA", "PPLPHARMA", "PREMIERENE", "PVRINOX", "RAMCOCEM",
    "RBLBANK", "RECLTD", "RVNL", "SAIL", "SAMMAANCAP", "SBICARD", "SBILIFE",
    "SBIN", "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SRF",
    "SUNPHARMA", "SUNTV", "SUZLON", "SWIGGY", "SYNGENE", "TATACHEM",
    "TATACOMM", "TATACONSUM", "TATAELXSI", "TATAPOWER", "TATASTEEL",
    "TATATECH", "TECHM", "TITAGARH", "TORNTPHARM", "TORNTPOWER", "TRENT",
    "TVSMOTOR", "UBL", "ULTRACEMCO", "UNITDSPR", "UNOMINDA", "UPL", "VBL",
    "VEDL", "VOLTAS", "WAAREEENER", "YESBANK", "ZEEL", "ETERNAL"
]

class LTPTracker:
    def __init__(self):
        self.data_file = "ltp_tracker_data.json"
        self.csv_file = "ltp_tracker_results.csv"
        self.fno_cache_file = "ltp_fno_symbols.json"
        self.today_data = self.load_today_data()
        self.is_ci = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
        self.max_symbols = int(os.environ.get("LTP_MAX_SYMBOLS", "0") or 0)
        self.request_timeout = int(os.environ.get("NSE_TIMEOUT_SECONDS", "15") or 15)

    def get_fno_symbols(self, nse):
        """Return FnO symbols from WATCHLIST using daily cache."""
        cache_today = date.today().isoformat()

        try:
            with open(self.fno_cache_file, "r") as f:
                cache = json.load(f)
            if cache.get("date") == cache_today and isinstance(cache.get("symbols"), list):
                return cache["symbols"]
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        fno_symbols = []
        for i, symbol in enumerate(WATCHLIST, start=1):
            try:
                meta = nse.equityMetaInfo(symbol)
                if meta.get("isFNOSec"):
                    fno_symbols.append(symbol)
            except Exception:
                continue

            if self.is_ci and i % 20 == 0:
                print(f"[CI] Checked FnO eligibility: {i}/{len(WATCHLIST)}", flush=True)

        with open(self.fno_cache_file, "w") as f:
            json.dump({"date": cache_today, "symbols": fno_symbols}, f, indent=2)

        if self.max_symbols > 0:
            fno_symbols = fno_symbols[: self.max_symbols]
            print(f"[INFO] LTP_MAX_SYMBOLS applied: {len(fno_symbols)} symbols", flush=True)

        return fno_symbols
        
    def load_today_data(self):
        """Load existing data for today"""
        try:
            with open(self.data_file, 'r') as f:
                data = json.load(f)
                return data
        except FileNotFoundError:
            return {}
    
    def save_today_data(self):
        """Save data to JSON"""
        with open(self.data_file, 'w') as f:
            json.dump(self.today_data, f, indent=2)
    
    def fetch_ltp_for_stocks(self):
        """Fetch current LTP for all stocks"""
        ltp_data = {}
        current_time = datetime.now().strftime("%H:%M")
        
        print(f"\n{'='*80}", flush=True)
        print(f"Fetching LTP at {current_time} ({datetime.now().strftime('%I:%M %p')})", flush=True)
        print(f"{'='*80}", flush=True)
        
        with NSE(download_folder=Path(__file__).parent, timeout=self.request_timeout) as nse:
            fno_watchlist = self.get_fno_symbols(nse)
            if self.max_symbols > 0:
                fno_watchlist = fno_watchlist[: self.max_symbols]
            print(f"FnO symbols in watchlist: {len(fno_watchlist)}", flush=True)

            for i, symbol in enumerate(fno_watchlist):
                try:
                    quote = nse.quote(symbol, type="equity")
                    ltp_raw = quote.get("priceInfo", {}).get("lastPrice")
                    if ltp_raw is None:
                        raise ValueError("Missing lastPrice in quote response")
                    ltp = float(str(ltp_raw).replace(",", ""))
                    ltp_data[symbol] = ltp
                    if self.is_ci:
                        if (i + 1) % 10 == 0 or (i + 1) == len(fno_watchlist):
                            print(f"[CI] Fetched LTP: {i+1}/{len(fno_watchlist)}", flush=True)
                    else:
                        print(f"[{i+1}/{len(fno_watchlist)}] {symbol}: {ltp}", end="\r")
                except Exception as e:
                    print(f"\n[SKIP] {symbol}: {e}", flush=True)
        
        print(f"\n[OK] Fetched {len(ltp_data)} stocks", flush=True)
        return ltp_data, current_time
    
    def process_and_display(self, ltp_data, current_time):
        """Process LTP data and display comparison"""
        timestamp = datetime.now().isoformat()
        
        # Add to history
        if current_time not in self.today_data:
            self.today_data[current_time] = {
                'timestamp': timestamp,
                'stocks': {}
            }
        
        self.today_data[current_time]['stocks'] = ltp_data
        self.save_today_data()
        
        # Display current prices
        print(f"\n{current_time} run:")
        print(f"{'-'*100}")
        
        results = []
        
        for symbol in WATCHLIST:
            if symbol not in ltp_data:
                continue
            
            current_ltp = ltp_data[symbol]
            
            # Build comparison info
            comparison_info = f"{symbol} {current_ltp}"
            
            # Find previous runs and calculate changes
            previous_run_times = [t for t in sorted(self.today_data.keys()) if t < current_time]
            
            if previous_run_times:
                prev_time = previous_run_times[-1]
                prev_data = self.today_data[prev_time]['stocks']
                
                if symbol in prev_data:
                    prev_ltp = prev_data[symbol]
                    change = current_ltp - prev_ltp
                    change_pct = (change / prev_ltp * 100) if prev_ltp != 0 else 0
                    
                    comparison_info += f" | Prev ({prev_time}): {prev_ltp}"
                    comparison_info += f" | Change: {change:+.2f} ({change_pct:+.2f}%)"
                    
                    # Find first run of the day
                    first_run_times = sorted(self.today_data.keys())
                    if first_run_times:
                        first_time = first_run_times[0]
                        first_data = self.today_data[first_time]['stocks']
                        
                        if symbol in first_data and first_time != current_time:
                            first_ltp = first_data[symbol]
                            total_change = current_ltp - first_ltp
                            total_change_pct = (total_change / first_ltp * 100) if first_ltp != 0 else 0
                            
                            comparison_info += f" | Total from {first_time}: {total_change:+.2f} ({total_change_pct:+.2f}%)"
            
            print(comparison_info)
            results.append({
                'time': current_time,
                'symbol': symbol,
                'ltp': current_ltp,
                'timestamp': timestamp
            })
        
        # Save to CSV
        self.save_to_csv(results)
        print(f"\n[SAVED] Data saved to {self.csv_file}")

        return results, timestamp

    def save_run_csv(self, results, run_label):
        """Save only current run data to a dedicated CSV file."""
        run_file = f"LTP_RUN_{date.today().strftime('%Y%m%d')}_{run_label.replace(':', '')}.csv"
        with open(run_file, 'w', newline='') as f:
            fieldnames = ['timestamp', 'time', 'symbol', 'ltp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

        print(f"[SAVED] Run CSV: {run_file}")
        return run_file

    def export_run_to_sheets(self, run_csv_file, run_label):
        """Export current run CSV to a separate worksheet tab in Google Sheets."""
        sheet_id = os.environ.get('GOOGLE_SHEET_ID')
        if not sheet_id:
            print("[INFO] GOOGLE_SHEET_ID not set. Skipping Sheets export.")
            return

        try:
            from sheets_export import export_to_sheets

            worksheet_name = f"{date.today().isoformat()}_{run_label.replace(':', '-') }"
            export_to_sheets(run_csv_file, sheet_id, custom_name=worksheet_name)
        except Exception as e:
            print(f"[ERROR] Sheets Export failed: {e}")
    
    def save_to_csv(self, results):
        """Save results to CSV"""
        file_exists = Path(self.csv_file).exists()
        
        with open(self.csv_file, 'a', newline='') as f:
            fieldnames = ['timestamp', 'time', 'symbol', 'ltp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for row in results:
                writer.writerow(row)
    
    def run_tracker(self):
        """Main tracker function"""
        try:
            ltp_data, current_time = self.fetch_ltp_for_stocks()
            results, _ = self.process_and_display(ltp_data, current_time)
            run_csv_file = self.save_run_csv(results, current_time)
            self.export_run_to_sheets(run_csv_file, current_time)
        except Exception as e:
            print(f"[ERROR] Tracker failed: {e}")

def schedule_tracker():
    """Schedule tracker at specific times"""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print("[ERROR] APScheduler not installed. Install it with: pip install apscheduler")
        return
    
    scheduler = BackgroundScheduler()
    tracker = LTPTracker()
    
    # Schedule times: 9am, 10am, 1pm, 2pm, 3pm, 3:30pm (IST)
    schedule_times = [
        ('09', '00'),  # 9:00 AM
        ('10', '00'),  # 10:00 AM
        ('13', '00'),  # 1:00 PM
        ('14', '00'),  # 2:00 PM
        ('15', '00'),  # 3:00 PM
        ('15', '30'),  # 3:30 PM
    ]
    
    for hour, minute in schedule_times:
        # Use IST timezone (Asia/Kolkata)
        scheduler.add_job(
            tracker.run_tracker,
            CronTrigger(hour=hour, minute=minute, timezone='Asia/Kolkata'),
            id=f'ltp_tracker_{hour}_{minute}'
        )
        print(f"[SCHEDULED] {hour}:{minute} IST")
    
    scheduler.start()
    print("\n[OK] LTP Tracker scheduled. Press Ctrl+C to stop.")
    
    try:
        while True:
            pass
    except KeyboardInterrupt:
        scheduler.shutdown()
        print("\n[STOPPED] Tracker stopped.")

def manual_run():
    """Run tracker immediately for testing"""
    tracker = LTPTracker()
    tracker.run_tracker()

if __name__ == "__main__":
    command = sys.argv[1].lower() if len(sys.argv) > 1 else "run"

    if command == "run":
        manual_run()
    elif command == "schedule":
        schedule_tracker()
    elif command in ("help", "-h", "--help"):
        print("Usage:")
        print("  python ltp_tracker.py run       - Run once (for cron)")
        print("  python ltp_tracker.py schedule  - Keep process alive and run at fixed times")
        print("  python ltp_tracker.py help      - Show this help")
    else:
        print(f"Unknown command: {command}")
        print("Use: python ltp_tracker.py help")
