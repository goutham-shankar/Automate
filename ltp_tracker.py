import sys
from pathlib import Path
from datetime import datetime, date
import csv
import json
import os

# Add src to sys.path
sys.path.append(str(Path(__file__).parent / "src"))
from nse import NSE

# Custom stock watchlist (user-defined)
FNO_WATCHLIST = [
    "360ONE", "BAJAJ-AUTO", "BRITANNIA", "DMART", "GMRAIRPORT",
    "HINDALCO", "ABB", "ASHOKLEY", "BDL", "CHOLAFIN",
    "ITC", "KALYANKJIL", "LUPIN", "NBCC", "NTPC",
    "ADANIGREEN", "EXIDEIND", "HDFCBANK", "HINDUNILVR", "INDHOTEL",
    "INDUSTOWER", "KEI", "PREMIERENE", "BHEL", "CONCOR",
    "INFY", "SHRIRAMFIN", "TATACONSUM", "MPHASIS", "PAGEIND",
    "LICHSGFIN", "SBILIFE", "ONGC", "PIIND", "SAIL",
    "TATAPOWER", "WAAREEENER", "SUZLON", "ULTRACEMCO", "UPL",
    "ALKEM", "ZYDUSLIFE", "BIOCON", "COFORGE", "GAIL",
    "IEX", "LAURUSLABS", "MAXHEALTH", "PFC", "POWERINDIA",
    "SOLARINDS", "TRENT", "ADANIENSOL", "BANDHANBNK", "COLPAL",
    "EICHERMOT", "GODREJCP", "HUDCO", "ADANIPORTS", "ASIANPAINT",
    "BLUESTARCO", "CROMPTON", "JINDALSTEL", "KAYNES", "MANAPPURAM",
    "NESTLEIND", "NUVAMA", "AMBUJACEM", "GLENMARK", "HDFCLIFE",
    "HINDZINC", "INDIANB", "IREDA", "PETRONET", "SBICARD",
    "BOSCHLTD", "DELHIVERY", "IOC", "SIEMENS", "TECHM",
    "NATIONALUM", "PATANJALI", "LODHA", "SHREECEM", "PERSISTENT",
    "PPLPHARMA", "SONACOMS", "TCS", "WIPRO", "SWIGGY",
    "UNIONBANK", "VBL", "AMBER", "BEL", "CGPOWER",
    "DIVISLAB", "GRASIM", "KFINTECH", "LT", "MCX",
    "PNB", "PRESTIGE", "TATASTEEL", "TVSMOTOR", "APOLLOHOSP",
    "BANKBARODA", "DABUR", "FEDERALBNK", "GODREJPROP", "ICICIBANK",
    "ANGELONE", "AUBANK", "CAMS", "CUMMINSIND", "JSWENERGY",
    "LICI", "MARICO", "NHPC", "OFSS", "ASTRAL",
    "HAL", "HEROMOTOCO", "ICICIGI", "INDIGO", "IRFC",
    "PHOENIXLTD", "SBIN", "BSE", "DRREDDY", "JSWSTEEL",
    "SUNPHARMA", "TMPV", "NAUKRI", "POLYCAB", "M&M",
    "MFSL", "PGEL", "RELIANCE", "SYNGENE", "TITAN",
    "YESBANK", "TATATECH", "MCDOWELL-N", "VOLTAS", "AXISBANK",
    "BHARATFORG", "CIPLA", "DIXON", "HCLTECH", "KOTAKBANK",
    "LTIM", "OIL", "POLICYBKR", "RBLBANK", "TIINDIA",
    "AUROPHARMA", "BPCL", "DALBHARAT", "FORTIS", "HAVELLS",
    "IDEA", "APLAPOLLO", "BAJFINANCE", "CANBK", "INOXWIND",
    "JUBLFOOD", "LTF", "MUTHOOTFIN", "NMDC", "ABCAPITAL",
    "BAJAJHLDNG", "HDFCAMC", "HINDPETRO", "IDFCFIRSTB", "INDUSINDBK",
    "JIOFIN", "PNBHOUSING", "BANKINDIA", "CDSL", "ETERNAL",
    "MANKIND", "SUPREMEIND", "MOTHERSON", "OBEROIRLTY", "RECLTD",
    "MAZDOCK", "NYKAA", "PIDILITIND", "RVNL", "TATAELXSI",
    "VEDL", "SRF", "TORNTPOWER", "UNOMINDA", "ADANIENT",
    "BAJAJFINSV", "BHARTIARTL", "COALINDIA", "DLF", "ICICIPRULI",
    "KPITTECH", "MARUTI", "PAYTM", "POWERGRID", "SAMMAANCAP",
    "TORNTPHARM"
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
        """Get live FnO symbols from NSE API."""
        try:
            print("[INFO] Fetching live FnO stocks from NSE...", flush=True)
            response = nse.listEquityStocksByIndex(index='SECURITIES IN F&O')
            data = response.get('data', [])
            fno_symbols = [stock['symbol'] for stock in data if stock.get('symbol')]
            
            if not fno_symbols:
                print("[WARNING] No FnO stocks found, falling back to static list", flush=True)
                fno_symbols = FNO_WATCHLIST.copy()
            else:
                print(f"[OK] Fetched {len(fno_symbols)} FnO stocks from NSE", flush=True)
            
            if self.max_symbols > 0:
                fno_symbols = fno_symbols[: self.max_symbols]
                print(f"[INFO] LTP_MAX_SYMBOLS applied: {len(fno_symbols)} symbols", flush=True)
            
            return fno_symbols
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch FnO list from NSE: {e}", flush=True)
            print("[INFO] Falling back to static FnO list", flush=True)
            fno_symbols = FNO_WATCHLIST.copy()
            
            if self.max_symbols > 0:
                fno_symbols = fno_symbols[: self.max_symbols]
            
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
    
    def load_previous_ltp_from_sheets(self, current_time):
        """Load previous run data from prior Google Sheets tab (for CI persistence)."""
        sheet_id = os.environ.get('GOOGLE_SHEET_ID')
        creds_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
        
        if not sheet_id or not creds_json:
            return {}, None
        
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            creds_dict = json.loads(creds_json)
            credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            client = gspread.authorize(credentials)
            spreadsheet = client.open_by_key(sheet_id)
            
            # Find previous sheet: date format is YYYY-MM-DD_HHMM
            date_part = date.today().isoformat()
            current_sheet_name = f"{date_part}_{current_time.replace(':', '-')}"
            
            # Get all worksheets and filter by today's date
            all_sheets = [ws.title for ws in spreadsheet.worksheets()]
            today_sheets = [s for s in all_sheets if s.startswith(date_part)]
            today_sheets.sort()
            
            # Find the sheet before current
            prev_sheet_name = None
            for sheet_name in reversed(today_sheets):
                if sheet_name < current_sheet_name:
                    prev_sheet_name = sheet_name
                    break
            
            if not prev_sheet_name:
                print("[INFO] No previous sheet found today")
                return {}, None
            
            # Read data from previous sheet
            worksheet = spreadsheet.worksheet(prev_sheet_name)
            records = worksheet.get_all_records()
            
            prev_data = {}
            for row in records:
                symbol = row.get('symbol')
                ltp = row.get('ltp')
                if symbol and ltp:
                    try:
                        prev_data[symbol] = float(ltp)
                    except (ValueError, TypeError):
                        continue
            
            # Extract time from sheet name (YYYY-MM-DD_HH-MM format)
            prev_time = prev_sheet_name.split('_')[1].replace('-', ':')
            
            print(f"[INFO] Loaded {len(prev_data)} symbols from sheet '{prev_sheet_name}'")
            return prev_data, prev_time
            
        except Exception as e:
            print(f"[WARNING] Could not load from Sheets: {e}")
            return {}, None
    
    def load_previous_ltp_from_csv(self, exclude_after_time=None):
        """Load most recent LTP per symbol from cumulative CSV (fallback for local runs)."""
        if not Path(self.csv_file).exists():
            print("[INFO] No previous CSV data found")
            return {}, None
        
        # Read all rows and group by time to get the most recent COMPLETE run
        time_groups = {}
        with open(self.csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                time_str = row.get('time')
                symbol = row.get('symbol')
                ltp_str = row.get('ltp')
                
                if time_str and symbol and ltp_str:
                    try:
                        ltp_val = float(ltp_str)
                        if time_str not in time_groups:
                            time_groups[time_str] = {}
                        time_groups[time_str][symbol] = ltp_val
                    except ValueError:
                        continue
        
        if not time_groups:
            print("[INFO] No valid previous data in CSV")
            return {}, None
        
        # Get the most recent time (excluding current if specified)
        sorted_times = sorted(time_groups.keys(), reverse=True)
        prev_time = None
        prev_data = {}
        
        for t in sorted_times:
            if exclude_after_time and t >= exclude_after_time:
                continue
            prev_time = t
            prev_data = time_groups[t]
            break
        
        if prev_data:
            print(f"[INFO] Loaded {len(prev_data)} symbols from CSV time {prev_time}")
        
        return prev_data, prev_time
    
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
                    price_info = quote.get("priceInfo", {})
                    
                    ltp_raw = price_info.get("lastPrice")
                    if ltp_raw is None:
                        raise ValueError("Missing lastPrice in quote response")
                    ltp = float(str(ltp_raw).replace(",", ""))
                    
                    # Get open and previous close for day's change
                    open_raw = price_info.get("open", 0)
                    prev_close_raw = price_info.get("previousClose", 0)
                    open_price = float(str(open_raw).replace(",", "")) if open_raw else None
                    prev_close = float(str(prev_close_raw).replace(",", "")) if prev_close_raw else None
                    
                    ltp_data[symbol] = {
                        'ltp': ltp,
                        'open': open_price,
                        'prev_close': prev_close
                    }
                    
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
        
        # Store only LTP values for comparison (open/prev_close not needed in history)
        self.today_data[current_time]['stocks'] = {symbol: data['ltp'] for symbol, data in ltp_data.items()}
        self.save_today_data()
        
        # Display current prices
        print(f"\n{current_time} run:")
        print(f"{'-'*100}")
        
        results = []
        previous_run_times = [t for t in sorted(self.today_data.keys()) if t < current_time]
        prev_time = previous_run_times[-1] if previous_run_times else None
        prev_data = self.today_data.get(prev_time, {}).get('stocks', {}) if prev_time else {}
        
        # If no same-day in-memory data, try loading from Google Sheets (for CI) then CSV (for local)
        if not prev_data:
            print("[INFO] No in-memory previous run, checking external sources...")
            prev_data, prev_time = self.load_previous_ltp_from_sheets(current_time)
            
            if not prev_data:
                prev_data, prev_time = self.load_previous_ltp_from_csv(exclude_after_time=current_time)
        else:
            print(f"[INFO] Using in-memory previous run from {prev_time}")
        
        for symbol in FNO_WATCHLIST:
            if symbol not in ltp_data:
                continue
            
            stock_data = ltp_data[symbol]
            current_ltp = stock_data['ltp']
            open_price = stock_data['open']
            prev_close = stock_data['prev_close']
            
            change = None
            change_pct = None
            day_change = None
            day_change_pct = None
            
            # Calculate day's change (from open)
            if open_price and open_price != 0:
                day_change = current_ltp - open_price
                day_change_pct = (day_change / open_price * 100)
            
            # Determine day status
            day_status = ""
            if day_change_pct is not None:
                if day_change_pct >= 1.0:
                    day_status = "Gainer"
                elif day_change_pct <= -1.0:
                    day_status = "Loser"
                else:
                    day_status = "Neutral"
            
            # Build comparison info
            comparison_info = f"{symbol} {current_ltp}"
            
            # Show day's change
            if day_change is not None:
                comparison_info += f" | Day's change: {day_change:+.2f} ({day_change_pct:+.2f}%)"
            
            # Find previous runs and calculate interval changes
            if prev_time and symbol in prev_data:
                prev_ltp = prev_data[symbol]
                change = current_ltp - prev_ltp
                change_pct = (change / prev_ltp * 100) if prev_ltp != 0 else 0

                comparison_info += f" | vs {prev_time}: {change:+.2f} ({change_pct:+.2f}%)"
            
            print(comparison_info)
            results.append({
                'time': current_time,
                'symbol': symbol,
                'ltp': current_ltp,
                'open': open_price if open_price else "",
                'prev_close': prev_close if prev_close else "",
                'day_change': "" if day_change is None else round(day_change, 4),
                'day_change_pct': "" if day_change_pct is None else round(day_change_pct, 4),
                'day_status': day_status,
                'timestamp': timestamp,
                'previous_time': prev_time or "",
                'change': "" if change is None else round(change, 4),
                'change_pct': "" if change_pct is None else round(change_pct, 4)
            })
        
        # Save to CSV
        self.save_to_csv(results)
        print(f"\n[SAVED] Data saved to {self.csv_file}")
        
        # Display day's movers summary
        self.display_day_movers_summary(results)

        return results, timestamp
    
    def display_day_movers_summary(self, results):
        """Display stocks that moved >= 1% from today's open"""
        print(f"\n{'='*100}")
        print(f"DAY'S MOVERS SUMMARY (Change from Open >= ±1%)")
        print(f"{'='*100}")
        
        # Filter stocks with day_change_pct data
        stocks_with_day_change = [r for r in results if r.get('day_change_pct') != ""]
        
        if not stocks_with_day_change:
            print("[INFO] No day's change data available yet (market not opened or no open prices)")
            return
        
        # Find losers (down >= 1%)
        losers = [r for r in stocks_with_day_change if r['day_change_pct'] <= -1.0]
        losers.sort(key=lambda x: x['day_change_pct'])
        
        # Find gainers (up >= 1%)
        gainers = [r for r in stocks_with_day_change if r['day_change_pct'] >= 1.0]
        gainers.sort(key=lambda x: x['day_change_pct'], reverse=True)
        
        print(f"\n🔴 LOSERS (Down >= 1%): {len(losers)} stocks")
        print(f"{'-'*100}")
        if losers:
            print(f"{'Symbol':<15} {'LTP':<10} {'Open':<10} {'Change':<10} {'Change %':<10}")
            print(f"{'-'*100}")
            for stock in losers[:20]:  # Show top 20
                print(f"{stock['symbol']:<15} {stock['ltp']:<10.2f} {stock['open']:<10.2f} "
                      f"{stock['day_change']:<10.2f} {stock['day_change_pct']:<10.2f}%")
            if len(losers) > 20:
                print(f"... and {len(losers) - 20} more")
        else:
            print("None")
        
        print(f"\n🟢 GAINERS (Up >= 1%): {len(gainers)} stocks")
        print(f"{'-'*100}")
        if gainers:
            print(f"{'Symbol':<15} {'LTP':<10} {'Open':<10} {'Change':<10} {'Change %':<10}")
            print(f"{'-'*100}")
            for stock in gainers[:20]:  # Show top 20
                print(f"{stock['symbol']:<15} {stock['ltp']:<10.2f} {stock['open']:<10.2f} "
                      f"{stock['day_change']:<10.2f} {stock['day_change_pct']:<10.2f}%")
            if len(gainers) > 20:
                print(f"... and {len(gainers) - 20} more")
        else:
            print("None")
        
        print(f"\n{'='*100}")


    def save_run_csv(self, results, run_label):
        """Save only current run data to a dedicated CSV file."""
        run_file = f"LTP_RUN_{date.today().strftime('%Y%m%d')}_{run_label.replace(':', '')}.csv"
        with open(run_file, 'w', newline='') as f:
            fieldnames = ['timestamp', 'time', 'previous_time', 'symbol', 'ltp', 'open', 'prev_close', 
                         'day_change', 'day_change_pct', 'day_status', 'change', 'change_pct']
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
            fieldnames = ['timestamp', 'time', 'previous_time', 'symbol', 'ltp', 'open', 'prev_close',
                         'day_change', 'day_change_pct', 'day_status', 'change', 'change_pct']
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
