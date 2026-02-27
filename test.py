import sys
from pathlib import Path
from datetime import date
import csv
import time
import os

# Add src to sys.path
sys.path.append(str(Path(__file__).parent / "src"))
from nse import NSE

def run_fetch():
    watchlist = [
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
    
    csv_filename = "ONE_BY_ONE_FETCH_Export.csv"
    from_date = date.today()
    to_date = date.today()
    
    with NSE(download_folder=Path(__file__).parent) as nse:
        # Automate target expiry if not manually set to a specific date
        # Choosing the first monthly expiry available
        try:
            expiries = nse.getFuturesExpiry("nifty")
            target_expiry = expiries[0]
        except Exception:
            target_expiry = "27-Mar-2026" # Fallback
            
        print(f"--- Starting One-By-One Fetch for {len(watchlist)} Stocks ---")
        print(f"Range: {from_date} to {to_date}, Expiry: {target_expiry}")
        
        fieldnames = ['Date', 'Expiry', 'Strike', 'Type', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI', 'Change in OI', 'Underlying Value']
        
        with open(csv_filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, symbol in enumerate(watchlist):
                print(f"[{i+1}/{len(watchlist)}] Fetching {symbol} (CE & PE)...", end="\r")
                try:
                    for opt_type in ["CE", "PE"]:
                        data = nse.fetch_historical_fno_data(
                            symbol=symbol,
                            instrument="OPTSTK",
                            from_date=from_date,
                            to_date=to_date,
                            option_type=opt_type
                        )
                        
                        rows_written = 0
                        for row in data:
                            # Filter by expiry
                            if row.get('FH_EXPIRY_DT') == target_expiry:
                                strike = float(row.get('FH_STRIKE_PRICE', 0))
                                underlying = float(row.get('FH_UNDERLYING_VALUE', 0))
                                
                                # Filtering Logic:
                                # CE: Keep if Strike >= 105% of Underlying
                                # PE: Keep if Strike <= 95% of Underlying
                                keep = False
                                if underlying > 0:
                                    if opt_type == "CE" and strike >= (underlying * 1.05):
                                        keep = True
                                    elif opt_type == "PE" and strike <= (underlying * 0.95):
                                        keep = True
                                
                                if keep:
                                    writer.writerow({
                                        'Date': row.get('FH_TIMESTAMP'),
                                        'Expiry': row.get('FH_EXPIRY_DT'),
                                        'Strike': strike,
                                        'Type': row.get('FH_OPTION_TYPE'),
                                        'Symbol': symbol,
                                        'Open': row.get('FH_OPENING_PRICE'),
                                        'High': row.get('FH_TRADE_HIGH_PRICE'),
                                        'Low': row.get('FH_TRADE_LOW_PRICE'),
                                        'Close': row.get('FH_CLOSING_PRICE'),
                                        'Volume': row.get('FH_TOT_TRADED_QTY'),
                                        'OI': row.get('FH_OPEN_INT'),
                                        'Change in OI': row.get('FH_CHANGE_IN_OI'),
                                        'Underlying Value': underlying
                                    })
                                    rows_written += 1
                        
                        if rows_written > 0:
                            print(f"[{i+1}/{len(watchlist)}] {symbol} {opt_type}: Found {rows_written} records.    ")
                    else:
                        # Try without & or - if no data found
                        clean = symbol.replace("&", "").replace("-", "")
                        if clean != symbol:
                            # Recursive fetch would be better, but let's keep it simple
                            pass 

                except Exception as e:
                    print(f"\nError fetching {symbol}: {e}")
                
                # Optional small sleep to be respectful, though throttle is in-built
                # time.sleep(0.1)

    print(f"\n--- Fetch Complete. Data saved to {csv_filename} ---")
    
    # Trigger Google Sheets export if GOOGLE_SHEET_ID is set
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    if sheet_id:
        print("\n--- Starting Google Sheets Export ---")
        try:
            from sheets_export import export_to_sheets
            export_to_sheets(csv_filename, sheet_id)
        except Exception as e:
            print(f"[ERROR] Sheets Export failed: {e}")
    else:
        print("\n[INFO] GOOGLE_SHEET_ID not set. Skipping Sheets export.")

if __name__ == "__main__":
    run_fetch()
