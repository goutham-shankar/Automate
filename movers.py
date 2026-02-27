import sys
from pathlib import Path
from nse import NSE

# Add src to sys.path
sys.path.append(str(Path(__file__).parent / "src"))

def format_row(s):
    """Safe formatting for stock data row."""
    symbol = s.get('symbol', 'N/A')
    try:
        last_price = float(s.get('lastPrice', 0))
        p_change = float(s.get('pChange', 0))
        return f"  - {symbol:<12}: {last_price:>10.2f} ({p_change:>+6.2f}%)"
    except (ValueError, TypeError):
        return f"  - {symbol:<12}: Data Error"

def suggest_movers():
    """
    Fetch and suggest highly moving stocks from NIFTY 50 and F&O segments.
    """
    print("\n" + "="*50)
    print("      NSE HIGHLY MOVING STOCKS SUGGESTER")
    print("="*50)

    try:
        with NSE(download_folder=Path(__file__).parent) as nse:
            # 1. Fetch NIFTY 50 Top Gainers and Losers
            print("\n--- [ NIFTY 50 ] ---")
            nifty_data = nse.listEquityStocksByIndex(index="NIFTY 50")
            
            gainers = nse.gainers(nifty_data, count=5)
            losers = nse.losers(nifty_data, count=5)

            print("\n--- Top 5 Gainers ---")
            for s in gainers:
                print(format_row(s))
            
            print("\n--- Top 5 Losers ---")
            for s in losers:
                print(format_row(s))

            # 2. Fetch F&O Top Movers (De-facto highly active stocks)
            print("\n" + "-"*30)
            print("--- [ F&O SECURITIES ] ---")
            fno_data = nse.listEquityStocksByIndex(index="SECURITIES IN F&O")
            
            fno_gainers = nse.gainers(fno_data, count=5)
            fno_losers = nse.losers(fno_data, count=5)

            print("\n--- Top 5 F&O Gainers ---")
            for s in fno_gainers:
                print(format_row(s))
            
            print("\n--- Top 5 F&O Losers ---")
            for s in fno_losers:
                print(format_row(s))

    except Exception as e:
        print(f"\n[ERROR] Error fetching data: {e}")
    
    print("\n" + "="*50)
    print("   Data fetched successfully. Happy Trading!")
    print("="*50 + "\n")

if __name__ == "__main__":
    suggest_movers()
