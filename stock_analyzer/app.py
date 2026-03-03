from flask import Flask, render_template, jsonify, request
from analyzer import OTMAnalyzer
from datetime import date
import threading
import time

app = Flask(__name__)
analyzer = OTMAnalyzer()

# Global variable to store scan results and status
scan_results = None
scan_status = "idle"  # idle, running, completed, error
scan_progress = 0


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/start_scan', methods=['POST'])
def start_scan():
    global scan_status, scan_results, scan_progress

    if scan_status == "running":
        return jsonify({"error": "Scan already in progress"}), 400

    # Parse parameters from request
    data = request.json
    from_date_str = data.get('from_date', '2026-01-01')
    to_date_str = data.get('to_date', '2026-03-30')
    expiry_str = data.get('expiry_date', '2026-03-30')  # Default to end date

    from_date = date.fromisoformat(from_date_str)
    to_date = date.fromisoformat(to_date_str)
    expiry_date = date.fromisoformat(expiry_str)

    # Define watchlist
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

    # Start scan in a separate thread to avoid blocking
    scan_status = "running"
    scan_progress = 0

    def run_scan():
        global scan_status, scan_results, scan_progress
        try:
            scan_results = analyzer.scan_symbols(
                watchlist, from_date, to_date, expiry_date)
            scan_status = "completed"
        except Exception as e:
            scan_status = "error"
            print(f"Scan error: {str(e)}")
        finally:
            scan_progress = 100

    thread = threading.Thread(target=run_scan)
    thread.start()

    return jsonify({"status": "started", "message": "Scan initiated"})


@app.route('/api/scan_status')
def get_scan_status():
    global scan_status, scan_results, scan_progress
    return jsonify({
        "status": scan_status,
        "progress": scan_progress,
        "results_available": scan_results is not None
    })


@app.route('/api/results')
def get_results():
    global scan_results
    if scan_results is not None:
        # Convert DataFrame to JSON-serializable format
        results_dict = scan_results.to_dict('records')
        return jsonify(results_dict)
    else:
        return jsonify({"error": "No results available"}), 404


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
