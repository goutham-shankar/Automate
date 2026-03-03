import pandas as pd
from datetime import date, timedelta
from nse import NSE
import time
from typing import List, Dict, Tuple


class OTMAnalyzer:
    def __init__(self):
        self.nse = NSE(download_folder="./cache", server=False)

    def get_spot_price(self, symbol: str) -> float:
        """Get current spot price for a given symbol"""
        try:
            quote_data = self.nse.quote(symbol)
            return quote_data['priceInfo']['lastPrice']
        except Exception as e:
            print(f"Error getting live quote for {symbol}: {e}")
            # Fallback: get from historical data if live quote fails
            try:
                hist_data = self.nse.fetch_equity_historical_data(
                    symbol=symbol,
                    # Look at last 5 days in case of weekends/holidays
                    from_date=date.today() - timedelta(days=5),
                    to_date=date.today()
                )
                if hist_data:
                    # Find the most recent entry with closing price
                    for entry in reversed(hist_data):
                        if 'CH_CLOSING_PRICE' in entry:
                            return entry['CH_CLOSING_PRICE']
            except Exception as hist_error:
                print(
                    f"Error getting historical data for {symbol}: {hist_error}")
            return None

    def get_options_data(self, symbol: str, from_date: date, to_date: date, expiry: date) -> pd.DataFrame:
        """Fetch options data for a symbol within date range"""
        all_data = []

        # Get CE options
        try:
            ce_data = self.nse.fetch_historical_fno_data(
                symbol=symbol,
                instrument='OPTSTK',
                from_date=from_date,
                to_date=to_date,
                expiry=expiry,
                option_type='CE'
            )
            for item in ce_data:
                item['option_type'] = 'CE'
                all_data.append(item)
        except Exception as e:
            print(f"Error fetching CE data for {symbol}: {e}")

        # Get PE options
        try:
            pe_data = self.nse.fetch_historical_fno_data(
                symbol=symbol,
                instrument='OPTSTK',
                from_date=from_date,
                to_date=to_date,
                expiry=expiry,
                option_type='PE'
            )
            for item in pe_data:
                item['option_type'] = 'PE'
                all_data.append(item)
        except Exception as e:
            print(f"Error fetching PE data for {symbol}: {e}")

        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        if not df.empty:
            df['FH_TIMESTAMP'] = pd.to_datetime(
                df['FH_TIMESTAMP'], format='%d-%b-%Y')
            df['FH_STRIKE_PRICE'] = pd.to_numeric(df['FH_STRIKE_PRICE'])
            df['FH_OPEN_INT'] = pd.to_numeric(df['FH_OPEN_INT'])

        return df

    def identify_otm_options(self, df: pd.DataFrame, spot_price: float) -> pd.DataFrame:
        """Filter dataframe to only include OTM options"""
        if df.empty:
            return df

        # For CE: Strike Price > Spot Price (Out of the money)
        # For PE: Strike Price < Spot Price (Out of the money)
        otm_mask = (
            ((df['option_type'] == 'CE') & (df['FH_STRIKE_PRICE'] > spot_price)) |
            ((df['option_type'] == 'PE') & (df['FH_STRIKE_PRICE'] < spot_price))
        )

        return df[otm_mask]

    def analyze_oi_growth(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze Open Interest growth trends for each strike/option type combination"""
        if df.empty:
            return pd.DataFrame()

        # Sort by timestamp to ensure proper chronological order
        df_sorted = df.sort_values(
            ['FH_STRIKE_PRICE', 'option_type', 'FH_TIMESTAMP'])

        # Group by strike price and option type to analyze trends
        grouped = df_sorted.groupby(['FH_STRIKE_PRICE', 'option_type'])

        results = []

        for (strike, opt_type), group in grouped:
            # Calculate OI differences between consecutive days
            group = group.copy()
            group['oi_diff'] = group['FH_OPEN_INT'].diff()
            group['oi_pct_change'] = group['FH_OPEN_INT'].pct_change()

            # Calculate cumulative OI growth
            avg_daily_increase = group['oi_diff'].mean()
            total_growth = group['FH_OPEN_INT'].iloc[-1] - \
                group['FH_OPEN_INT'].iloc[0]
            total_pct_growth = group['FH_OPEN_INT'].iloc[-1] / \
                group['FH_OPEN_INT'].iloc[0] - \
                1 if group['FH_OPEN_INT'].iloc[0] != 0 else 0

            results.append({
                'symbol': group['FH_SYMBOL'].iloc[0],
                'strike_price': strike,
                'option_type': opt_type,
                'initial_oi': group['FH_OPEN_INT'].iloc[0],
                'final_oi': group['FH_OPEN_INT'].iloc[-1],
                'total_growth': total_growth,
                'total_pct_growth': total_pct_growth,
                'avg_daily_increase': avg_daily_increase,
                'num_trading_days': len(group),
                'consistency_score': self.calculate_consistency_score(group)
            })

        return pd.DataFrame(results)

    def calculate_consistency_score(self, group: pd.DataFrame) -> float:
        """Calculate a score representing how consistently OI has grown"""
        oi_changes = group['FH_OPEN_INT'].diff().dropna()
        positive_changes = oi_changes[oi_changes > 0]

        if len(oi_changes) == 0:
            return 0.0

        # Consistency score: % of days with positive OI growth
        consistency = len(positive_changes) / len(oi_changes)

        # Weight by magnitude of average increase
        avg_increase = positive_changes.mean() if len(positive_changes) > 0 else 0

        return consistency * avg_increase if avg_increase > 0 else 0

    def scan_symbols(self, symbols: List[str], from_date: date, to_date: date, expiry: date) -> pd.DataFrame:
        """Main scanning function to analyze all symbols in the watchlist"""
        all_results = []

        for symbol in symbols:
            print(f"Processing {symbol}...")

            # Get spot price
            spot_price = self.get_spot_price(symbol)
            if spot_price is None or spot_price <= 0:
                print(
                    f"Could not get valid spot price for {symbol}, skipping...")
                continue

            # Get options data
            options_df = self.get_options_data(
                symbol, from_date, to_date, expiry)

            # Filter for OTM options only
            otm_df = self.identify_otm_options(options_df, spot_price)

            # Analyze OI growth
            oi_analysis = self.analyze_oi_growth(otm_df)

            if not oi_analysis.empty:
                all_results.append(oi_analysis)

            # Respect rate limits
            time.sleep(0.5)  # Add delay between symbol requests

        if all_results:
            final_results = pd.concat(all_results, ignore_index=True)
            # Sort by consistency score and total growth
            final_results = final_results.sort_values(
                ['consistency_score', 'total_growth'],
                ascending=False
            )
            return final_results
        else:
            return pd.DataFrame()
