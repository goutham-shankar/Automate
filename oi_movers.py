import sys
from pathlib import Path
from nse import NSE

# Add src to sys.path
sys.path.append(str(Path(__file__).parent / "src"))

def get_pe_ce_oi(nse, symbol):
    """
    Fetch total PE and CE Open Interest for a given symbol.
    """
    try:
        data = nse.optionChain(symbol)
        filtered = data.get('filtered', {})
        pe_tot = filtered.get('PE', {}).get('totOI', 0)
        ce_tot = filtered.get('CE', {}).get('totOI', 0)
        pcr = round(pe_tot / ce_tot, 2) if ce_tot > 0 else 0
        return pe_tot, ce_tot, pcr
    except Exception:
        return 0, 0, 0

def format_oi_row(s, pe_ce_info=None):
    """Safe formatting for OI data with optional PE/CE breakdown."""
    if not s: return "  - Data Error"
    symbol = s.get('symbol', 'N/A')
    try:
        change_oi_pct = float(s.get('avgInOI', 0))
        underlying_val = float(s.get('underlyingValue', 0))
        
        row = f"  - {symbol:<12}: {underlying_val:>10.2f} (OI Chg: {change_oi_pct:>+6.2f}%)"
        
        if pe_ce_info:
            pe_tot, ce_tot, pcr = pe_ce_info
            sentiment = "Bullish" if pcr > 1.1 else "Bearish" if pcr < 0.9 else "Neutral"
            row += f"\n    [PE: {pe_tot:,} | CE: {ce_tot:,} | PCR: {pcr} ({sentiment})]"
            
        return row
    except (ValueError, TypeError):
        return f"  - {symbol:<12}: Data Error"

def suggest_oi_movers():
    """
    Fetch and suggest stocks with significant Open Interest (OI) changes.
    """
    print("\n" + "="*80)
    print("      NSE OPEN INTEREST (OI) STOCK SUGGESTER WITH SENTIMENT")
    print("="*80)

    try:
        with NSE(download_folder=Path(__file__).parent) as nse:
            # Fetch OI Spurts (Underlyings)
            print("Fetching OI spurts data...")
            res = nse._req(f"{nse.base_url}/live-analysis-oi-spurts-underlyings").json()
            data = [item for item in (res.get('data', []) if isinstance(res, dict) else res) if item]
            
            if data:
                # 1. OI Buildup (Gainers)
                print("\n--- TOP 5 STOCKS WITH HIGHEST OI BUILDUP (F&O SENTIMENT) ---")
                gainers = sorted(data, key=lambda x: float(x.get('avgInOI', 0)), reverse=True)
                for s in gainers[:5]:
                    symbol = s.get('symbol')
                    print(f"Analyzing {symbol}...")
                    pe_ce = get_pe_ce_oi(nse, symbol)
                    print(format_oi_row(s, pe_ce))

                # 2. OI Liquidation (Losers)
                print("\n--- TOP 5 STOCKS WITH HIGHEST OI LIQUIDATION ---")
                losers = sorted(data, key=lambda x: float(x.get('avgInOI', 0)))
                for s in losers[:5]:
                    symbol = s.get('symbol')
                    print(f"Analyzing {symbol}...")
                    pe_ce = get_pe_ce_oi(nse, symbol)
                    print(format_oi_row(s, pe_ce))
            else:
                print("\nNo OI data available at the moment. Market might be closed.")

    except Exception as e:
        print(f"\n[ERROR] Error fetching OI data: {e}")
    
    print("\n" + "="*80)
    print("   High OI Change indicates trend momentum. PCR indicates sentiment.")
    print("="*80 + "\n")

if __name__ == "__main__":
    suggest_oi_movers()
