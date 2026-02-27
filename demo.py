import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, date
import pandas as pd
import csv
from collections import defaultdict

# Add src to sys.path to import nse
sys.path.append(str(Path(__file__).parent / "src"))

from nse import NSE

def run_demo():
    cwd = Path(__file__).parent
    
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
        "VEDL", "VOLTAS", "WAAREEENER", "YESBANK", "ZEEL", "ZOMATO"
    ]

    print("--- Initializing NSE API for Watchlist Scan ---")
    
    with NSE(download_folder=cwd) as nse:
        scan_dates = [date(2026, 2, 24), date(2026, 2, 23)]
        target_expiry = "2026-03-30" # Udiff format for 30 Mar
        all_results = []
        found_symbols = set()
        
        for dt in scan_dates:
            print(f"\nProcessing Bhavcopy for {dt.strftime('%d-%b-%Y')}...")
            try:
                file_path = nse.fnoBhavcopy(datetime.combine(dt, datetime.min.time()))
                df = pd.read_csv(file_path)
                
                # Create a normalization map for the watchlist
                # Try prefix match or removing special chars
                bhav_tickers = set(df['TckrSymb'].unique())
                normalized_watchlist = {}
                for s in watchlist:
                    if s in bhav_tickers:
                        normalized_watchlist[s] = s
                    else:
                        # Common NSE F&O mapping quirks
                        clean = s.replace("&", "").replace("-", "")
                        if clean in bhav_tickers:
                            normalized_watchlist[s] = clean
                        elif s == "GMRINFRA" and "GMRAIRPORT" in bhav_tickers:
                            normalized_watchlist[s] = "GMRAIRPORT"
                        elif s == "KEE" and "KEI" in bhav_tickers:
                            normalized_watchlist[s] = "KEI"
                        elif s == "L&TFH" and "LTF" in bhav_tickers:
                            normalized_watchlist[s] = "LTF"
                
                target_tickers = list(normalized_watchlist.values())
                
                # Filter for Watchlist, OPTSTK, CE, and Target Expiry
                mask = (
                    (df['TckrSymb'].isin(target_tickers)) &
                    (df['FinInstrmTp'] == 'STO') &
                    (df['OptnTp'] == 'CE') &
                    (df['XpryDt'] == target_expiry)
                )
                
                filtered_df = df[mask].copy()
                
                # Reverse the normalized map for reporting
                rev_map = {v: k for k, v in normalized_watchlist.items()}
                
                for _, row in filtered_df.iterrows():
                    sym = row['TckrSymb']
                    found_symbols.add(rev_map.get(sym, sym))
                    all_results.append({
                        'Date': row['TradDt'],
                        'Expiry': row['XpryDt'],
                        'Strike': row['StrkPric'],
                        'Type': row['OptnTp'],
                        'Symbol': rev_map.get(sym, sym),
                        'NSE_Ticker': sym,
                        'Open': row['OpnPric'],
                        'High': row['HghPric'],
                        'Low': row['LwPric'],
                        'Close': row['ClsPric'],
                        'Volume': row['TtlTradgVol'],
                        'OI': row['OpnIntrst'],
                        'Change in OI': row['ChngInOpnIntrst'],
                        'Underlying Value': row['UndrlygPric']
                    })
                print(f"  Found {len(filtered_df)} records for {len(filtered_df['TckrSymb'].unique())} symbols.")
                
            except Exception as e:
                print(f"  Error processing {dt}: {e}")

        if all_results:
            # Sort: Date Desc, Symbol Asc, Strike Asc
            def sort_key(x):
                dt_obj = datetime.strptime(x['Date'], "%Y-%m-%d")
                return (-dt_obj.timestamp(), x['Symbol'], float(x['Strike']))
            
            all_results.sort(key=sort_key)
            
            # Export to CSV
            csv_filename = "WATCHLIST_SCAN_Export.csv"
            keys = all_results[0].keys()
            with open(csv_filename, 'w', newline='') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(all_results)
            
            print(f"\n--- Scan Complete. Total records: {len(all_results)} ---")
            
            missing = sorted(list(set(watchlist) - found_symbols))
            if missing:
                print(f"--- F&O Data NOT found for {len(missing)} stocks (might not be in F&O segment): ---")
                print(", ".join(missing[:20]) + ("..." if len(missing) > 20 else ""))
            
            print(f"--- Data exported to {csv_filename} ---")
            
            # Show top sample in terminal
            print(f"\n{'Date':<12} {'Symbol':<10} {'Strike':<8} {'Close':<8} {'OI':<10}")
            for r in all_results[:15]:
                print(f"{r['Date']:<12} {r['Symbol']:<10} {r['Strike']:<8} {r['Close']:<8} {r['OI']:<10}")
            print("...")
        else:
            print("\nNo data found for the given criteria.")

if __name__ == "__main__":
    run_demo()
