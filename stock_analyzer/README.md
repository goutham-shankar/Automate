# OTM Options Analysis Tool

A web-based automated stock market analysis tool that scans Indian stocks to find Out-of-the-Money (OTM) options with steadily growing Open Interest (OI).

## Features

- Scans predefined list of Indian stocks for OTM options
- Identifies options with consistent OI growth over time
- Provides consistency scoring and growth metrics
- Web-based interface with progress tracking
- Handles NSE API rate limits gracefully

## Requirements

- Python 3.8+
- Dependencies listed in requirements.txt

## Installation

1. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

2. Ensure you have the NSE India API module available (this project assumes it's in the parent directory under `src/nse/`)

## Usage

Run the application:
```
python run_server.py
```

Then navigate to `http://127.0.0.1:5000` in your web browser.

## How It Works

1. The tool iterates through a predefined watchlist of Indian stocks
2. For each stock, it fetches the current spot price
3. It identifies OTM options based on the criteria:
   - For Calls (CE): Strike Price > Spot Price
   - For Puts (PE): Strike Price < Spot Price
4. It analyzes the Open Interest growth over the specified date range
5. Results are sorted by consistency score and total growth

## Parameters

- From Date: Start date for analysis (default: 2026-01-01)
- To Date: End date for analysis (default: 2026-03-30)
- Expiry Date: Options expiry date (default: 2026-03-30)

## Output

The results table shows:
- Symbol: Stock symbol
- Strike Price: Option strike price
- Type: Option type (CE/PE)
- Initial OI: Opening Open Interest value
- Final OI: Closing Open Interest value
- Total Growth: Absolute OI growth
- % Growth: Percentage OI growth
- Consistency Score: Score representing how consistently OI has grown