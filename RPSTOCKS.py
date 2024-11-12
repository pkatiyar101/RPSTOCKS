import yfinance as yf
import xlwings as xw
import time
import sys
import requests
import random
import pytz
import pandas as pd
import os
import numpy as np
import json
import concurrent.futures
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from nsepython import nsefetch  # Use nsefetch for fetching NSE data
from http.cookiejar import CookieJar
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

SLEEPTIME = 30  # Sleep time in seconds
SHEET_SWING = 'SWING'
SHEET_PCR = 'SHEET_PCR'
SHEET_NIFTY = 'Nifty'
SHEET_FINAL = 'FINAL'
SHEET_ALL = 'YFIN1'
MAX_WORKERS = 10
MAX_RETRIES = 3  # Max retries for fetching data
INTERVALS = '1d'
EXCEL_FILE = 'YFin.xls'  # Replace with your actual file path
COUNTDOWN_SHEET_NAME = 'YFIN1'
SHEET_FNOSW = 'FNOSW'



def print_progress_bar(iteration, total, length=40):
    percent = ("{0:.2f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = '*' * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r[{bar}] {percent}%  {iteration} of {total} tickers completed')
    sys.stdout.flush()
    

def write_headers(sheet):
    headers = [
        'Symbol',  'close|15', 'Signal','SignalPE', 'REALL', 'Chart'
    ]
    sheet.range("B2:H2").value = headers  # Write headers to row 2


######################################################
def update_countdown_in_excel(workbook, remaining_time):
    """Update countdown timer in the Excel sheet."""
    sheet = workbook.sheets['SWING']
    sheet.range('AG1').value = f"Time remaining: {remaining_time} seconds"
    sheet.range('AG2').value = f"Time remaining: {remaining_time} seconds"
    sheet.range('AG3').value = f"Time remaining: {remaining_time} seconds"
    

#########################  pcrn() PCR NIFTY ##########

SELECTED_EXPIRY_INDEX = 0  # Change this to select the desired expiry date (0-based index)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.93 Mobile Safari/537.36"
]


def fetch_pcr(symbol, row_index):
    # Custom replacements for specific symbols
    if symbol == 'M&M':
        symbol = 'M%26M'
    elif symbol == 'M&MFIN':
        symbol = 'M%26MFIN'

    # Construct the appropriate URL
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}" if symbol in ["NIFTY", "BANKNIFTY"] else f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"

    # Create a session with retry logic
    session = requests.Session()
    
    retry_strategy = Retry(
        total=5,  # Total retries
        backoff_factor=1,  # Time between retries (exponential backoff)
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]  # Retry only for GET requests
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    # Set headers and try to access the website
    session.get("https://www.nseindia.com", headers={"User-Agent": random.choice(USER_AGENTS)})

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()  # Check for HTTP errors
    except requests.exceptions.SSLError as e:
        print(f"SSL error for {symbol}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

    json_data = response.json()
    data = json_data.get("records", {}).get("data", [])
    expiry_dates = json_data.get("records", {}).get("expiryDates", [])

    if not expiry_dates:
        print(f"No expiry dates found for {symbol}. Skipping.")
        return None  # Return None to indicate failure

    selected_expiry_date = expiry_dates[SELECTED_EXPIRY_INDEX]
    changeinOpenInterest_ce = changeinOpenInterest_pe = 0 

    for i in data:
        for j, k in i.items():
            if j in ["CE", "PE"]:
                info = k
                if info["expiryDate"] == selected_expiry_date:
                    if j == "CE":
                        changeinOpenInterest_ce += info.get("changeinOpenInterest", 0)
                    elif j == "PE":
                        changeinOpenInterest_pe += info.get("changeinOpenInterest", 0)

    # Calculate PCR (Put/Call Ratio)
    pcr_coi = (changeinOpenInterest_pe / changeinOpenInterest_ce) if changeinOpenInterest_ce else 0
    # Adjust PCR based on the condition
    if pcr_coi < 1:
        pcr_coi = pcr_coi - 1
    
    return (symbol, round(pcr_coi, 2))

def pcrn():
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_PCR]
    sheet.range('B3').clear_contents()
    
    
    # Get the symbols dynamically
    symbols = [cell.value for cell in sheet.range('B3') if cell.value is not None]
    total_symbols = len(symbols)
    
    results = []
    
    # Thread pool for fetching PCR data
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_pcr, symbol, i + 1): symbol for i, symbol in enumerate(symbols)}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result:
                results.append(result)
            print_progress_bar(i + 1, total_symbols)
            
            # Update progress in Excel
            sheet.range('AG1').value = f"FETCH PCR DATA ALL F&O: {i + 1} of {total_symbols} completed"
            sheet.range('AG2').value = f"FETCH PCR DATA ALL F&O: {i + 1} of {total_symbols} completed"
            sheet.range('AG3').value = f"FETCH PCR DATA ALL F&O: {i + 1} of {total_symbols} completed"
    # Write results to Excel all at once
    if results:
        nse_sheet1 = wb.sheets[SHEET_PCR]
        headers = ['Symbol', 'PCR']
        nse_sheet1.range('B2').value = headers
        nse_sheet1.range('B3').value = results  # Assuming results start from row 
       
##########  date verification #################################

def dat_verification():
    file_name = 'YFin.xls'
    sheet_name = 'SY_SHEET'
    
    # Open the workbook and select the sheets
    wb = xw.Book(file_name)
    sheet_all = wb.sheets[sheet_name]
    sheet_all1 = wb.sheets['SWING']
    
    # Get the current date as a formatted string
    end_time = time.localtime()
    current_date_str = time.strftime('%Y-%m-%d', end_time)  # Use a consistent date format
    
    # Get the date from cell B1 and convert it to a string
    to = sheet_all.range('B1').value
    to_date = None
    
    # Check if the date in B1 is a valid date
    if isinstance(to, (int, float)):
        to_date = datetime(*xw.utils.datetime.from_excel(to).timetuple()[:3]).strftime('%Y-%m-%d')
    elif isinstance(to, datetime):
        to_date = to.strftime('%Y-%m-%d')
    
    # Compare dates and update if necessary
    if to_date == current_date_str:
        print("Date is already up to date. Exiting...")
        return
    else:
        # Update relevant cells in 'SWING' sheet
        sheet_all1.range('G1').value = current_date_str
        print("-------   FETCH CASH BEST STOCKS   ------")
        for cell in ['AG1', 'AG2', 'AG3']:
            sheet_all1.range(cell).value = "FETCH CASH BEST STOCKS"
        
        # Perform additional analysis steps
        print("-------   CASH FUNDAMENTAL ANALYSIS   ------")
        for cell in ['AG1', 'AG2', 'AG3']:
            sheet_all1.range(cell).value = "CASH FUNDAMENTAL ANALYSIS"
        
        # Run the `far()` function since the date is not up to date
        far()
        
        # Update date in 'SY_SHEET'
        sheet_all.range('B1').value = current_date_str
        print("Date updated to current date.")


def far():
    file_name = 'YFin.xls'
    sheet_name = 'SY_SHEET'
    
    try:
        wb = xw.Book(file_name)
        sheet_all = wb.sheets[sheet_name]
        
        stock_symbols = sheet_all.range("A1:A2000").value
        sheet_all.range("B4:K2000").clear_contents()  # Corrected this line

        # Replacement dictionary
        replacements = {
            "M_MFIN": "M&MFIN",
            "M_M": "M&M",
            "BAJAJ_AUTO": "BAJAJ-AUTO"
        }
        
        # Replace stock symbols using the replacements dictionary
        stock_symbols = [replacements.get(symbol, symbol) for symbol in stock_symbols]
        
        # Filter out None values
        stock_symbols = [symbol for symbol in stock_symbols if symbol]
        
        all_metrics = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_symbol = {executor.submit(fetch_chartink_data, stock_symbol): stock_symbol for stock_symbol in stock_symbols}
            
            for future in as_completed(future_to_symbol):
                stock_symbol = future_to_symbol[future]
                try:
                    metrics = future.result()
                    if metrics:
                        altman_z = round(calculate_altman_z(metrics), 2)
                        piotroski_f = calculate_piotroski_f_score(metrics)
                        graham_number = round(calculate_graham_number(metrics), 2)
                        lynch_value = round(calculate_lynch_fair_value(metrics), 2)
                        sloan_ratio = round(calculate_sloan_ratio(metrics), 2)
                        nse_ltp = round(metrics["Nse LTP"], 2)
    
                        #buy_signal = (altman_z > 3 and graham_number < nse_ltp and 
                                      #lynch_value < 100 and sloan_ratio < 1)
                        buy_signal = (
                                        (graham_number < nse_ltp) and 
                                        (lynch_value < 100 ) and 
                                        (sloan_ratio < 1))
                        sell_signal = (altman_z < 3 and graham_number > nse_ltp )
                        
                        if buy_signal:
                            signal = "Buy"
                        elif sell_signal:
                            signal = "Sell"
                        else:
                            signal = None
                        
                        metrics_result = {
                            "Symbol": stock_symbol,
                            "Altman Z-Score": altman_z,
                            "Piotroski F-Score": piotroski_f,
                            "Graham Number": graham_number,
                            "Peter Lynch Fair Value": lynch_value,
                            "Sloan Ratio": sloan_ratio,
                            "Nse LTP": nse_ltp,
                            "SIGNAL": signal  # Store the signal
                        }
                        all_metrics.append(metrics_result)
                        logging.info(f"Processed {stock_symbol}")
    
                except Exception as e:
                    logging.error(f"{stock_symbol} generated an exception: {e}")
    
        # Save the metrics to Excel and JSON
        save_metrics_to_excel(all_metrics, file_name, sheet_name)
        save_metrics_to_json(all_metrics)

    except Exception as e:
        print(f"An error occurred: {e}")


def save_metrics_to_excel(data_dict, file_name='YFin.xls', sheet_name='SY_SHEET'):
    wb = xw.Book(file_name)
    sheet_all = wb.sheets[sheet_name]
    sheet_all1 = wb.sheets['SWING']
    # Clear existing data in the sheet
  
    sheet_all.range("B2:I2000").clear_contents()
    #sheet_all.range("J2:J2000").clear_contents()  # Clear J column for buy signals

    # Prepare headers
    headers = ["Symbol", "Altman Z-Score", "Graham Number", "Peter Lynch Fair Value", "Sloan Ratio", "Nse LTP"]
    sheet_all.range("B3").value = headers  # Write headers in the first row

    # Write data starting from the second row
    for i, metrics in enumerate(data_dict, start=4):
        sheet_all.range(f"B{i}").value = metrics["Symbol"]
        sheet_all.range(f"C{i}").value = metrics["Altman Z-Score"]
        sheet_all.range(f"D{i}").value = metrics["Graham Number"]
        sheet_all.range(f"E{i}").value = metrics["Peter Lynch Fair Value"]
        sheet_all.range(f"F{i}").value = metrics["Sloan Ratio"]
        sheet_all.range(f"G{i}").value = metrics["Nse LTP"]
        
        if metrics.get("SIGNAL") == "Buy":
            sheet_all.range(f"H{i}").value = metrics["Symbol"]  # Save symbol in column J for Buy
        elif metrics.get("SIGNAL") == "Sell":
            sheet_all.range(f"I{i}").value = metrics["Symbol"]  # Save symbol in column K for Sell
      
            
    sheet_all.range("J4:K2000").clear_contents()
    sheet_all1.range("A4:A2000").clear_contents()
    # Prepare headers
    headers1 = ["BUY", "SELL" ]
    sheet_all.range("H3").value = headers1  # Write headers in the first row

    # Prepare lists for buy and sell signals
    buy_signals = []
    sell_signals = []

    # Write data and capture buy/sell signals
    for metrics in data_dict:
        row_data = [
            metrics["Symbol"],
    
        ]
        if metrics.get("SIGNAL") == "Buy":
            buy_signals.append((metrics["Symbol"], row_data))
        elif metrics.get("SIGNAL") == "Sell":
            sell_signals.append((metrics["Symbol"], row_data))

    # Write buy signals
    start_row = 4
    for symbol, row_data in buy_signals:
        sheet_all.range(f"J{start_row}").value = symbol  # Write symbol in the buy signal column
        start_row += 1

    # Write sell signals
    start_row = 4
    for symbol, row_data in sell_signals:
        sheet_all.range(f"K{start_row}").value = symbol  # Write symbol in the sell signal column
        start_row += 1

   
    start_row = 4
    for symbol, row_data in buy_signals:
        sheet_all1.range(f"A{start_row}").value = symbol  # Write symbol in the sell signal column
        start_row += 1
    # Save the workbook
    wb.save()

###  CASH STOCKS #########################################################


# Configure logging
logging.basicConfig(level=logging.INFO, filename='app.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_chartink_data(stock_symbol):
    url = f'https://chartink.com/fundamentals/{stock_symbol}.html'
    for attempt in range(MAX_RETRIES):
        response = requests.get(url)
        if response.status_code == 200:
            break
        logging.warning(f"Attempt {attempt + 1} failed for {stock_symbol}. Retrying...")
    else:
        logging.error(f"Failed to retrieve data for {stock_symbol} after {MAX_RETRIES} attempts.")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    data = {}

    try:
        rows = soup.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                key = cols[0].text.strip()
                value = cols[1].text.strip().replace(',', '').replace('\n', '').strip()
                data[key] = value

        metrics_data = {
            "Earning Per Share": float(data.get("Earning Per Share\n                                                 Full Year Earning Per Share", "0")) or 0.0,
            "Book Value": float(data.get("Book Value\n                                                 Book value", "0")) or 0.0,
            "Net Income": float(data.get("Net Profit\n                                                 Full Year Net Profit", "0")) or 0.0,
            "Total Assets": float(data.get("Sales Turnover\n                                                 Full Year Net Sales", "0")) or 0.0,
            "Total Liabilities": float(data.get("Loans\n                                                 Total loans", "0")) or 0.0,
            "Sales": float(data.get("Sales Turnover\n                                                 Full Year Net Sales", "0")) or 0.0,
            "Retained Earnings": float(data.get("Reserves\n                                                Total Reserve", "0")) or 0.0,
            "Market Value of Equity": float(data.get("Market cap\n                                                 BSE / NSE Market Cap", "0").replace("₹", "").replace(",", "").strip()) if "₹" in data.get("Market cap\n                                                 BSE / NSE Market Cap", "") else 0,
            "Cash Flow from Operations": float(data.get("Full Year CPS\n                                                 Full Year Cash Per Share", "0")) * float(data.get("Sales Turnover\n                                                 Full Year Net Sales", "0")) if data.get("Full Year CPS\n                                                 Full Year Cash Per Share") and data.get("Sales Turnover\n                                                 Full Year Net Sales") else 0,
            "Nse LTP": float(data.get("Price\n                                                 NSE Current market price","0")) or 0.0
        }

        data["Symbol"] = stock_symbol  # Add stock symbol to the data
        return metrics_data
    except Exception as e:
        logging.error(f"Error parsing data for {stock_symbol}: {e}")
        return None

def calculate_altman_z(metrics):
    if all(key in metrics for key in ["Total Assets", "Total Liabilities", "Market Value of Equity", "Sales", "Retained Earnings", "Net Income", "Nse LTP"]):
        if metrics["Total Liabilities"] == 0:
            logging.warning(f"Total Liabilities for {metrics['Symbol']} is zero. Cannot calculate Altman Z-Score.")
            return None
        z_score = (
            1.2 * (metrics["Net Income"] / metrics["Total Assets"]) +
            1.4 * (metrics["Retained Earnings"] / metrics["Total Assets"]) +
            3.3 * (metrics["Net Income"] / metrics["Total Assets"]) +
            0.6 * (metrics["Market Value of Equity"] / metrics["Total Liabilities"]) +
            1.0 * (metrics["Sales"] / metrics["Total Assets"])
        )
        return z_score
    else:
        logging.warning("Missing data for Altman Z-Score calculation.")
        return None

def calculate_piotroski_f_score(metrics):
    score = 0
    if "Net Income" in metrics and metrics["Net Income"] > 0:
        score += 1
    if "Cash Flow from Operations" in metrics and metrics["Cash Flow from Operations"] > 0:
        score += 1
    if metrics.get("Cash Flow from Operations", 0) > metrics.get("Net Income", 0):
        score += 1
    # Add additional conditions for other Piotroski criteria...
    return score

def calculate_graham_number(metrics):
    if "Earning Per Share" in metrics and "Book Value" in metrics:
        try:
            graham_number = (22.5 * metrics["Earning Per Share"] * metrics["Book Value"]) ** 0.5
            return abs(graham_number)  # Return the absolute value
        except ValueError as ve:
            logging.error(f"ValueError: {ve} in calculating Graham Number for {metrics['Symbol']}")
            return None
        except Exception as e:
            logging.error(f"Error calculating Graham Number: {e}")
            return None
    else:
        logging.warning("Missing data for Graham Number calculation.")
        return None

def calculate_lynch_fair_value(metrics):
    if "Earning Per Share" in metrics and "Market Value of Equity" in metrics:
        peg_ratio = (metrics["Market Value of Equity"] / metrics["Earning Per Share"]) / (metrics.get("Sales Growth", 1) or 1)  # Avoid division by zero
        lynch_value = metrics["Earning Per Share"] * (1 + peg_ratio)
        return lynch_value
    else:
        logging.warning("Missing data for Peter Lynch Fair Value calculation.")
        return None

def calculate_sloan_ratio(metrics):
    if "Cash Flow from Operations" in metrics and "Net Income" in metrics:
        sloan_ratio = (metrics["Net Income"] - metrics["Cash Flow from Operations"]) / metrics["Total Assets"]
        return sloan_ratio
    else:
        logging.warning("Missing data for Sloan Ratio calculation.")
        return None

# Function to save all metrics to a JSON file
def save_metrics_to_json(all_metrics, json_file='metrics_data.json'):
    with open(json_file, 'w') as f:
        json.dump(all_metrics, f, indent=4)
    logging.info(f"Metrics saved to JSON file: {json_file}")
            
####### PCR DATA ALL FNO  ########################

def pcr1():
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_PCR]
    sheet.range('B4:C300').clear_contents()
    
    
    print("-------   FETCH PCR DATA FOR ALL F & O   ------")
    
    # Get the symbols dynamically
    symbols = [cell.value for cell in sheet.range('A4:A200') if cell.value is not None]
    total_symbols = len(symbols)
    
    results = []
    
    # Thread pool for fetching PCR data
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_pcr, symbol, i + 1): symbol for i, symbol in enumerate(symbols)}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result:
                results.append(result)
            print_progress_bar(i + 1, total_symbols)
            
            # Update progress in Excel
            sheet.range('AG1').value = f"FETCH PCR DATA ALL F&O: {i + 1} of {total_symbols} completed"
            sheet.range('AG2').value = f"FETCH PCR DATA ALL F&O: {i + 1} of {total_symbols} completed"
            sheet.range('AG3').value = f"FETCH PCR DATA ALL F&O: {i + 1} of {total_symbols} completed"
    # Write results to Excel all at once
    if results:
        nse_sheet1 = wb.sheets[SHEET_PCR]
        #headers = ['Symbol', 'PCR']
        #nse_sheet1.range('B2').value = headers
        nse_sheet1.range('B4').value = results  # Assuming results start from row 
        

####### NIFTY OPTION CHAIN  ########################
def niftychain():
    with xw.App(visible=False) as app:
        wb = xw.Book(EXCEL_FILE)  # Open workbook
        sheet = wb.sheets[SHEET_NIFTY]  # Access the sheet
        SY = sheet.range("A1").value  # Read symbol from A1
        if SY:
            symbols = [SY]  # List of symbols, based on A1 value
            # Fetch PCR data
            pcr_results = fetch_all_option_chain(symbols)
            # Write the data to the Excel file
            if pcr_results:
                nifty_option_to_excel(pcr_results, EXCEL_FILE, SHEET_NIFTY)
                print(f"Option Chain written to {SHEET_NIFTY} sheet in {EXCEL_FILE}")
            else:
                print("No data to write to Excel.")
        else:
            print("Symbol in A1 is empty")

def fetch_all_option_chain(symbols):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_option_chain, symbol): symbol for symbol in symbols}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.extend(result)  # Append the list of strikes data to the results
    return results

def nifty_option_to_excel(data, file_path, sheet_name):
    df = pd.DataFrame(data)  # Convert the data into a DataFrame
    with xw.App(visible=False) as app:  # Use xlwings to open Excel
        wb = xw.Book(file_path)  # Open the workbook
        sheet = wb.sheets[sheet_name]  # Access the desired sheet
        
        # Fetch symbol from cell A1
        symbol_cell = sheet.range("A1").value
        if not symbol_cell:
            print("No symbol found in A1. Exiting.")
            return
        
        # Clear the range where the results will be written
        sheet.range("B2:T500").clear_contents()
        sheet.range("B2").value = df  # Write the DataFrame to the sheet
        
        # Save the workbook
        wb.save()
        
        
SELECTED_EXPIRY_INDEX = 0  # Change this to select the desired expiry date (0-based index)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.93 Mobile Safari/537.36"
]

def fetch_option_chain(symbol):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}" if symbol in ["NIFTY", "BANKNIFTY"] else f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"

    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.get("https://www.nseindia.com", headers={"User-Agent": random.choice(USER_AGENTS)})

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

    try:
        json_data = response.json()
        data = json_data.get("records", {}).get("data", [])
        expiry_dates = json_data.get("records", {}).get("expiryDates", [])
        
        if not expiry_dates:
            print(f"No expiry dates found for {symbol}. Skipping.")
            return None
        
        selected_expiry_date = expiry_dates[SELECTED_EXPIRY_INDEX]  # Select the desired expiry date

        # Collect all strikes data
        result = []
        for record in data:
            ce_data = record.get("CE", {})
            pe_data = record.get("PE", {})
            if ce_data.get("expiryDate") == selected_expiry_date or pe_data.get("expiryDate") == selected_expiry_date:
                result.append({
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "expiry": selected_expiry_date,
                    "strike": ce_data.get("strikePrice") or pe_data.get("strikePrice"),
                    "CE_open_interest": ce_data.get("openInterest", None),
                    "CE_change_in_oi": ce_data.get("changeinOpenInterest", None),
                    "CE_last_price": ce_data.get("lastPrice", None),
                    "CE_Change_price": ce_data.get("change", None),
                    "CE_volume": ce_data.get("totalTradedVolume", None),
                    "CE_iv": ce_data.get("impliedVolatility", None),
                    "CE_under_LTP": ce_data.get("underlyingValue", None),
                    
                    "PE_open_interest": pe_data.get("openInterest", None),
                    "PE_change_in_oi": pe_data.get("changeinOpenInterest", None),
                    "PE_last_price": pe_data.get("lastPrice", None),
                    "PE_Change_price": pe_data.get("change", None),
                    "PE_volume": pe_data.get("totalTradedVolume", None),
                    "PE_iv": pe_data.get("impliedVolatility", None),
                    "PE_under_LTP": pe_data.get("underlyingValue", None),
                })
        return result

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON for {symbol}: {e}")
        return None
        
################ FNO INTRADAY #########################

def fetch_trading_data12():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_ALL]
        sheet.range('B3:K700').clear()

        symbols = sheet.range("A1:A200").value  # Get symbols from A1 to A700
        symbols = [s for s in symbols if s]  # Filter out empty symbols
        #total_symbols = len(symbols)

        true_tickers = []  # List to store tickers with True signals
        original_symbols = []  # List to store original symbols for "True" tickers
        results = []  # List to store fetched data
        
        true_tickersPE = []
        original_symbolsPE = []  # List to store original symbols for "TruePE" tickers
        #resultsPE = []  # List to store fetched data

        # Process symbol using concurrent futures
        def process_symbol(symbol):
            return fetch_trading_data11(symbol)

        # Use ThreadPoolExecutor for concurrent fetching
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_symbol, symbols))

        # Filter results and update the Excel sheet in one go
        row = 3
        filtered_results = []
        for result in results:
            if result:
                symbol1, close, signal, recommend_all, signalPE = result
                filtered_results.append([symbol1, close, signal, recommend_all, signalPE])
                if signal == "True":
                    true_tickers.append(symbol1)
                    original_symbols.append(symbol1.replace('NSE:', ''))  # Store original symbol without 'NSE:'
                
                if signalPE == "TruePE":
                    true_tickersPE.append(symbol1)
                    original_symbolsPE.append(symbol1.replace('NSE:', ''))  # Store original symbol without 'NSE:'
                    
        # Write data to Excel in bulk
        if filtered_results:
            sheet.range(f"B{row}:F{row + len(filtered_results) - 1}").value = filtered_results

        # Write the "True" tickers to column H and original symbols to column L
        #sheet.range("H3:K300").clear_contents()  # Clear the previous "True" tickers and related data
        if true_tickers:
            sheet.range("H3").options(transpose=True).value = true_tickers

            for row_offset, (symbol, original_symbol) in enumerate(zip(true_tickers, original_symbols), start=3):
                result = next(res for res in filtered_results if res[0] == symbol)
                if result:
                    _, close, _, recommend_all, _ = result
                    sheet.range(f"I{row_offset}").value = recommend_all
                    sheet.range(f"J{row_offset}").value = create_tradingview_link(symbol)
                    sheet.range(f"K{row_offset}").value = original_symbol
        time.sleep(1)
        # Handle "TruePE" tickers
        sheet.range("Z3:AC300").clear_contents()
        if true_tickersPE:
             
            sheet.range("Z3").options(transpose=True).value = true_tickersPE

            for row_offset, (symbol, original_symbolPE) in enumerate(zip(true_tickersPE, original_symbolsPE), start=3):
                resultPE = next(res for res in filtered_results if res[0] == symbol)
                if resultPE:
                    _, close, _, recommend_all, _ = resultPE
                    sheet.range(f"AA{row_offset}").value = recommend_all
                    sheet.range(f"AB{row_offset}").value = create_tradingview_link(symbol)
                    sheet.range(f"AC{row_offset}").value = original_symbolPE  # Write original symbol in column L
        
        wb.save()
    except Exception as e:
        print(f"An error occurred during data fetching: {e}")


def fetch_trading_data11(symbol):
    url = "https://scanner.tradingview.com/symbol"
    symbol1 = f'NSE:{symbol.replace("NSE_EQ:", "").replace("-", "_").replace("&", "_").upper()}'
    params = {
        "symbol": symbol1,
        "fields": "Recommend.All|15,close|15,Ichimoku.BLine|15,RSI|15,RSI[1]|15,AO|15,AO[1]|15,open|15,",
        "no_404": "True"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Ensure a valid response
        data = response.json()
        if data and isinstance(data, dict):
            close = round(data.get('close|15', 0), 2)
            recommend_all = round(data.get('Recommend.All|15', 0), 2)
            ichimoku_bline = round(data.get('Ichimoku.BLine|15', 0), 2)
            rsi = round(data.get('RSI|15', 0), 2)
            rsi1 = round(data.get('RSI[1]|15', 0), 2)
            ao = round(data.get('AO|15', 0), 2)
            ao1 = round(data.get('AO[1]|15', 0), 2)
            open = round(data.get('open|15', 0), 2)
            
                        
            # Check for signal condition
            signal = "True" if (close > open and ao > ao1 and ao > 0 and rsi > rsi1 and rsi > 50 and rsi < 70 and close > ichimoku_bline and recommend_all > 0.2) else "False"
            signalPE = "TruePE" if (ao < ao1 and ao < 0 and rsi < 50 and rsi < rsi1 and close < ichimoku_bline and recommend_all < -0.2) else "False"
            return (symbol1, close, signal, recommend_all, signalPE)
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching data for {symbol1}: {e}")
    return None


def create_tradingview_link(symbol):
    """Create a TradingView chart link."""
    base_url = "https://in.tradingview.com/chart/?symbol="
    return f'=HYPERLINK("{base_url}{symbol}", "CHART")'

##########  COPY PASTE ##############

#########################################

def copy_pest():
    # Open the workbook and select the sheets
    wb = xw.Book(EXCEL_FILE)
    copy_sheet = wb.sheets['FINAL']
    paste_sheet = wb.sheets['COPY']
    
    # Get the current time in the required formats
    current_time1 = datetime.now().strftime('%m%d%y%H%M%S')
    current_time = datetime.now().strftime('%H:%M')
    sigtime = datetime.now().strftime('%d%m-%H%M')
   
    # Get the last row in column B that contains data
    last_row = paste_sheet.range('B' + str(paste_sheet.cells.last_cell.row)).end('up').row
    next_row = last_row + 1
    
    # Copy the values from the FINAL sheet AS1:AX1
    values_a = copy_sheet.range('AS1:AX1').value
    
    # Paste the current time in the first column and values from AS1:AX1
    paste_sheet.range(f'C{next_row}').value = current_time
    paste_sheet.range(f'B{next_row}').value = current_time1
    paste_sheet.range(f'D{next_row}').value = values_a
    
    # Get the last row in column Z that contains data
    last_rowc = paste_sheet.range('Z' + str(paste_sheet.cells.last_cell.row)).end('up').row
    next_row3 = last_rowc + 1
    
    # Copy the values from the FINAL sheet AD4:AQ6
    values_b = copy_sheet.range('AD4:AQ6').value
    
    # Concatenate sigtime with each value row and paste them into the new row
    for i, row in enumerate(values_b):
        paste_sheet.range(f'Z{next_row3 + i}').value = [sigtime] + row

    # Save the workbook after pasting
    wb.save()

################  FNO INTRADAY Y FINANCE ########


# Define a function to prepare tickers
def prepare_tickers(tickers):
    prepared_tickers = []
    for ticker in tickers:
        if pd.isna(ticker):
            continue
        ticker = str(ticker)
        # Custom replacements
        if ticker == "NIFTY":
            prepared_tickers.append("^NSEI")
        elif ticker == "BANKNIFTY":
            prepared_tickers.append("^NSEBANK")
        elif ticker == "NAM_INDIA":
            prepared_tickers.append("NAM-INDIA.NS")
        elif ticker == "BAJAJ_AUTO":
            prepared_tickers.append("BAJAJ-AUTO.NS")
        elif ticker == "SURANAT_P":
            prepared_tickers.append("SURANAT&P.NS")
        elif ticker == "MRO_TEK":
            prepared_tickers.append("MRO-TEK.NS")
        elif ticker == "M_MFIN":
            prepared_tickers.append("M&MFIN.NS")
        else:
            # Replace '_' with '&' and append '.NS'
            ticker = ticker.replace('_', '&') + ".NS"
            prepared_tickers.append(ticker)
    return prepared_tickers

# Function to download data for a ticker
def download_dataFNOI(ticker):
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(interval='5m', period='5d')
        if data.empty:
            print(f"No data available for {ticker}")
            return None
        return data
    except Exception as e:
        print(f"Failed to download data for {ticker}: {e}")
        return None

def main_scr():
    # Load Excel and get tickers
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_ALL]
    tickers = sheet.range('K1:K200').value
    tickers = prepare_tickers([ticker for ticker in tickers if ticker is not None])

    # Download data in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_dataFNOI, tickers))

    # Filter valid data and concatenate
    valid_results = [result for result in results if result is not None]
    if not valid_results:
        print("No valid data to process. Exiting.")
        return

    # Combine data into a single DataFrame
    try:
        combined_data = pd.concat(valid_results, keys=[ticker for ticker, result in zip(tickers, results) if result is not None])
    except ValueError as e:
        print(f"Error concatenating data: {e}")
        return

    # RSI Calculation
    def calculate_rsi(data, window=14):
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    # Calculate RSI and other metrics
    combined_data['RSI'] = calculate_rsi(combined_data)
    combined_data['RSI_MA'] = combined_data['RSI'].rolling(window=14).mean()
    combined_data['Prev_RSI'] = combined_data['RSI'].shift(1)
    combined_data['Prev_RSI_MA'] = combined_data['RSI_MA'].shift(1)

    # Condition Check
    combined_data['Condition'] = (
        (combined_data['RSI'] > combined_data['RSI_MA']) &
        (combined_data['Prev_RSI'] <= combined_data['Prev_RSI_MA'])
    )

    # Get last entries per ticker
    last_entries = combined_data.groupby(level=0).last()
    tickers_meeting_condition = last_entries[last_entries['Condition']].index.get_level_values(0).unique()

    # Output to Excel
    sheet.range('L2:T200').clear_contents()
    sheet.range('L2').value = last_entries[['Close', 'RSI', 'RSI_MA', 'Prev_RSI', 'Prev_RSI_MA', 'Condition']]

    # Write tickers that meet the condition to column S
    sheet.range('S3').value = [[ticker] for ticker in tickers_meeting_condition]
    print("Script completed successfully.")

    def replace_ticker(ticker):
        # Define the mapping replacements
        replacements = {
            "^NSEBANK": "BANKNIFTY",
            "^NSEI": "NIFTY",
            "BAJAJ-AUTO.NS": "BAJAJ_AUTO",
            "M&MFIN.NS": "M_MFIN",
            "MRO-TEK.NS": "MRO_TEK",
            "NAM-INDIA.NS": "NAM_INDIA",
            "SURANAT&P.NS": "SURANAT_P"
        }
    
        # Replace ticker if it matches a predefined key in replacements
        if ticker in replacements:
            return replacements[ticker]
        # If ticker contains ".NS", remove it
        ticker = ticker.replace('.NS', '')
        # Replace '&' with '_'
        ticker = ticker.replace('&', '_')
        return ticker
    
    # Apply replacement function to tickers meeting the condition
    modified_tickers = [replace_ticker(ticker) for ticker in tickers_meeting_condition]
    
    # Write the modified tickers to column U starting from U3
    if len(modified_tickers) > 0:
        sheet.range('T3').value = [[ticker] for ticker in modified_tickers]
    else:
        print("No tickers meet the condition.")   

##### FNO INTRADAY RECO ALL  TRADING #################

def fetch_trading_fno1():
    # Open the Excel file and fetch the symbols
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_ALL]
    symbols = sheet.range("T1:T200").value  # Fetch tickers from the sheet
    
    # Filter out any empty cells in the range
    symbols = [symbol for symbol in symbols if symbol]
    
    # Fetching data for each symbol and storing in a list
    data_list = [fetch_trading_fno(symbol) for symbol in symbols]
    
    # Converting the data list to a DataFrame for tabular display
    df = pd.DataFrame(data_list)
    
    # Display the DataFrame in a tabular format
    #print(df)
    
    # Write the DataFrame back to a specified range (U3:X200)
    sheet_output = wb.sheets[SHEET_ALL]
    sheet_output.range("U2:X200").clear_contents()  # Clear only the intended range
    sheet_output.range("U2").value = df  # Write DataFrame starting at U3


def fetch_trading_fno(symbol):
    """Fetch trading data for a single symbol from TradingView API."""
    url = "https://scanner.tradingview.com/symbol"
    # Format symbol for TradingView
    symbol1 = f'NSE:{symbol.replace("NSE_EQ:", "").replace("-", "_").replace("&", "_").upper()}'
    
    params = {
        "symbol": symbol1,
        "fields": "Recommend.All|15,close|15",
        "no_404": "True"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        try:
            data = response.json()
            close = data.get('close|15')
            recommend_all = data.get('Recommend.All|15')
            
            # Round values if they exist
            if close is not None:
                close = round(float(close), 2)
            if recommend_all is not None:
                recommend_all = round(float(recommend_all), 2)
            
            return {'Symbol': symbol, 'Close|15': close, 'Recommend.All|15': recommend_all}
        
        except (ValueError, KeyError, TypeError) as e:
            return {'Symbol': symbol, 'Error': f"Error processing data: {e}"}
    
    else:
        return {'Symbol': symbol, 'Error': f"Error fetching data. Status code: {response.status_code}"}
    
########3   FNO Y FINANCE INTRADAY PUT ############

def main_fnoPE():
    # Load Excel and get tickers
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_ALL]
    tickers = sheet.range('AC1:AC200').value
    tickers = prepare_tickers([ticker for ticker in tickers if ticker is not None])

    # Download data in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_dataFNOI, tickers))

    # Filter valid data and concatenate
    valid_results = [result for result in results if result is not None]
    if not valid_results:
        print("No valid data to process. Exiting.")
        return

    # Combine data into a single DataFrame
    try:
        combined_data = pd.concat(valid_results, keys=[ticker for ticker, result in zip(tickers, results) if result is not None])
    except ValueError as e:
        print(f"Error concatenating data: {e}")
        return

    # RSI Calculation
    def calculate_rsi(data, window=14):
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    # Calculate RSI and other metrics
    combined_data['RSI'] = calculate_rsi(combined_data)
    combined_data['RSI_MA'] = combined_data['RSI'].rolling(window=14).mean()
    combined_data['Prev_RSI'] = combined_data['RSI'].shift(1)
    combined_data['Prev_RSI_MA'] = combined_data['RSI_MA'].shift(1)

    # Condition Check
    combined_data['Condition'] = (
        (combined_data['RSI'] < combined_data['RSI_MA']) &
        (combined_data['Prev_RSI'] >= combined_data['Prev_RSI_MA'])
    )

    # Get last entries per ticker
    last_entries = combined_data.groupby(level=0).last()
    tickers_meeting_condition = last_entries[last_entries['Condition']].index.get_level_values(0).unique()

    # Output to Excel
    sheet.range('AD2:AL200').clear_contents()
    sheet.range('AD2').value = last_entries[['Close', 'RSI', 'RSI_MA', 'Prev_RSI', 'Prev_RSI_MA', 'Condition']]

    # Write tickers that meet the condition to column S
    sheet.range('AK3').value = [[ticker] for ticker in tickers_meeting_condition]
    print("Script completed successfully.")

    def replace_ticker(ticker):
        # Define the mapping replacements
        replacements = {
            "^NSEBANK": "BANKNIFTY",
            "^NSEI": "NIFTY",
            "BAJAJ-AUTO.NS": "BAJAJ_AUTO",
            "M&MFIN.NS": "M_MFIN",
            "MRO-TEK.NS": "MRO_TEK",
            "NAM-INDIA.NS": "NAM_INDIA",
            "SURANAT&P.NS": "SURANAT_P"
        }
    
        # Replace ticker if it matches a predefined key in replacements
        if ticker in replacements:
            return replacements[ticker]
        # If ticker contains ".NS", remove it
        ticker = ticker.replace('.NS', '')
        # Replace '&' with '_'
        ticker = ticker.replace('&', '_')
        return ticker
    
    # Apply replacement function to tickers meeting the condition
    modified_tickers = [replace_ticker(ticker) for ticker in tickers_meeting_condition]
    
    # Write the modified tickers to column U starting from U3
    if len(modified_tickers) > 0:
        sheet.range('AL3').value = [[ticker] for ticker in modified_tickers]
    else:
        print("No tickers meet the condition.")   


############### FNO INTRADAY PE ######################################################


def fetch_trading_data12PE():
    # Open the Excel file and fetch the symbols
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_ALL]
    symbols = sheet.range("AL1:AL200").value  # Fetch tickers from the sheet
    
    # Filter out any empty cells in the range
    symbols = [symbol for symbol in symbols if symbol]
    
    # Fetching data for each symbol and storing in a list
    data_list = [fetch_trading_fno(symbol) for symbol in symbols]
    
    # Converting the data list to a DataFrame for tabular display
    df = pd.DataFrame(data_list)
    
    # Display the DataFrame in a tabular format
    #print(df)
    
    # Write the DataFrame back to a specified range (U3:X200)
    sheet_output = wb.sheets[SHEET_ALL]
    sheet_output.range("AM2:AP200").clear_contents()  # Clear only the intended range
    sheet_output.range("AM2").value = df  # Write DataFrame starting at U3

#######  FNO SWING ################
def fetch_trading_dataFNO():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_FNOSW]
        sheet.range('B3:AU700').clear()

        symbols = sheet.range("A1:A700").value  # Get symbols from A1 to A700
        symbols = [s for s in symbols if s]  # Filter out empty symbols
   

        true_tickers = []  # List to store tickers with True signals
        original_symbols = []  # List to store original symbols for "True" tickers
        results = []  # List to store fetched data
        
        true_tickersPE = []
        original_symbolsPE = []  # List to store original symbols for "TruePE" tickers
        
        # Process symbol using concurrent futures
        def process_symbol(symbol):
            return fetch_trading_dataFNO1(symbol)

        # Use ThreadPoolExecutor for concurrent fetching
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_symbol, symbols))

        # Filter results and update the Excel sheet in one go
        row = 3
        filtered_results = []
        for result in results:
            if result:
                symbol1, close, signal, recommend_all, signalPE = result
                filtered_results.append([symbol1, close, signal, recommend_all, signalPE])
                if signal == "True":
                    true_tickers.append(symbol1)
                    original_symbols.append(symbol1.replace('NSE:', ''))  # Store original symbol without 'NSE:'
                
                if signalPE == "TruePE":
                    true_tickersPE.append(symbol1)
                    original_symbolsPE.append(symbol1.replace('NSE:', ''))  # Store original symbol without 'NSE:'
                    
        # Write data to Excel in bulk
        if filtered_results:
            sheet.range(f"B{row}:F{row + len(filtered_results) - 1}").value = filtered_results

        # Write the "True" tickers to column H and original symbols to column L
        sheet.range("H3:K300").clear_contents()  # Clear the previous "True" tickers and related data
        if true_tickers:
            sheet.range("H3").options(transpose=True).value = true_tickers

            for row_offset, (symbol, original_symbol) in enumerate(zip(true_tickers, original_symbols), start=3):
                result = next(res for res in filtered_results if res[0] == symbol)
                if result:
                    _, close, _, recommend_all, _ = result
                    sheet.range(f"I{row_offset}").value = recommend_all
                    sheet.range(f"J{row_offset}").value = create_tradingview_link(symbol)
                    sheet.range(f"K{row_offset}").value = original_symbol
        
        # Handle "TruePE" tickers
        sheet.range("AC3:AF300").clear_contents()
        if true_tickersPE:
             
            sheet.range("AC3").options(transpose=True).value = true_tickersPE

            for row_offset, (symbol, original_symbolPE) in enumerate(zip(true_tickersPE, original_symbolsPE), start=3):
                resultPE = next(res for res in filtered_results if res[0] == symbol)
                if resultPE:
                    _, close, _, recommend_all, _ = resultPE
                    sheet.range(f"AD{row_offset}").value = recommend_all
                    sheet.range(f"AE{row_offset}").value = create_tradingview_link(symbol)
                    sheet.range(f"AF{row_offset}").value = original_symbolPE  # Write original symbol in column L
        
        wb.save()
    except Exception as e:
        print(f"An error occurred during data fetching: {e}")

def fetch_trading_dataFNO1(symbol):
    url = "https://scanner.tradingview.com/symbol"
    symbol1 = f'NSE:{symbol.replace("NSE_EQ:", "").replace("-", "_").replace("&", "_").upper()}'
    params = {
        "symbol": symbol1,
        "fields": "Recommend.All,close|15,Ichimoku.BLine|15",
        "no_404": "True"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Ensure a valid response
        data = response.json()
        if data and isinstance(data, dict):
            close = round(data.get('close|15', 0), 2)
            recommend_all = round(data.get('Recommend.All', 0), 2)
            ichimoku_bline = round(data.get('Ichimoku.BLine|15', 0), 2)
            
            # Check for signal condition
            signal = "True" if (close > ichimoku_bline and recommend_all > 0.4) else "False"
            signalPE = "TruePE" if (close < ichimoku_bline and recommend_all < -0.40) else "False"
            return (symbol1, close, signal, recommend_all, signalPE)
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching data for {symbol1}: {e}")
    return None

############# FNO SW Y FINANCE CALL #############################


# Function to prepare tickers for processing


# Constants
SHEET_ALL = 'YFIN1'
SHEET_FNOSW = 'FNOSW'
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9
VOLUME_MA_WINDOW = 21

def mainFNOS():
    # Function to prepare tickers for processing
    def prepare_tickers(tickers):
        prepared_tickers = []
        for ticker in tickers:
            if pd.isna(ticker):
                continue
            ticker = str(ticker)
            if ticker == "NIFTY":
                prepared_tickers.append("^NSEI")
            elif ticker == "BANKNIFTY":
                prepared_tickers.append("^NSEBANK")
            elif ticker == "NAM_INDIA":
                prepared_tickers.append("NAM-INDIA.NS")
            elif ticker == "BAJAJ_AUTO":
                prepared_tickers.append("BAJAJ-AUTO.NS")
            elif ticker == "SURANAT_P":
                prepared_tickers.append("SURANAT&P.NS")
            elif ticker == "MRO_TEK":
                prepared_tickers.append("MRO-TEK.NS")
            elif ticker == "M_MFIN":
                prepared_tickers.append("M&MFIN.NS")
            else:
                ticker = ticker.replace('_', '&')
                prepared_tickers.append(ticker + ".NS")
        return prepared_tickers

    def calculate_rsi(data, window=14):
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def calculate_kd(data, k_window=14, d_window=3):
        low_min = data['Low'].rolling(window=k_window).min()
        high_max = data['High'].rolling(window=k_window).max()
        k = 100 * ((data['Close'] - low_min) / (high_max - low_min))
        d = k.rolling(window=d_window).mean()
        return k, d

    def calculate_macd(data):
        ema_short = data['Close'].ewm(span=MACD_SHORT, adjust=False).mean()
        ema_long = data['Close'].ewm(span=MACD_LONG, adjust=False).mean()
        macd_line = ema_short - ema_long
        signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
        macd_histogram = macd_line - signal_line
        return macd_line, signal_line, macd_histogram

    def download_data(ticker):
        stock = yf.Ticker(ticker)
        data = stock.history(interval='15m', period='1mo')
        if data.empty:
            print(f"No data for {ticker}")
            return None
        return data

    # Load Excel file and sheet
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_FNOSW]
    
    # Get tickers from Excel
    tickers = sheet.range('K1:K200').value
    tickers = prepare_tickers(tickers)
    
    # Download data in parallel using ThreadPoolExecutor
    data = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_data, tickers))
    
    # Filter out None results
    valid_results = [result for result in results if result is not None]
    if not valid_results:
        print("No valid data was downloaded.")
    else:
        combined_data = pd.concat(valid_results, keys=[ticker for ticker, result in zip(tickers, results) if result is not None])
        
        # Calculate indicators
        combined_data['RSI'] = calculate_rsi(combined_data)
        combined_data['K'], combined_data['D'] = calculate_kd(combined_data)
        combined_data['MACD'], combined_data['MACD_Signal'], combined_data['MACD_Hist'] = calculate_macd(combined_data)
        combined_data['Volume_MA21'] = combined_data['Volume'].rolling(window=VOLUME_MA_WINDOW).mean()
        combined_data['Volume_Spike'] = combined_data['Volume_MA21'] * 1.5

        # Last entries for each ticker
        last_entries = combined_data.groupby(combined_data.index.get_level_values(0)).last()
        
        # Define conditions
        last_entries['Condition'] = (
            (last_entries['RSI'] > last_entries['RSI'].rolling(window=14).mean()) &  # RSI greater than its 14-period MA
            (last_entries['RSI'].shift(1) <= last_entries['RSI'].rolling(window=14).mean().shift(1)) &
            
            (last_entries['K'] > last_entries['D']) &                               # %K above %D
            #(last_entries['Volume'] > last_entries['Volume_Spike']) &               # Volume greater than Volume Spike
            (last_entries['MACD'] > last_entries['MACD_Signal'])                    # MACD above MACD Signal
        )

        # Tickers meeting condition
        tickers_meeting_condition = last_entries[last_entries['Condition']].index.get_level_values(0).unique()

        # Replace tickers with mappings for those meeting condition
        def replace_ticker(ticker):
            replacements = {
                "^NSEBANK": "BANKNIFTY",
                "^NSEI": "NIFTY",
                "BAJAJ-AUTO.NS": "BAJAJ_AUTO",
                "M&MFIN.NS": "M_MFIN",
                "MRO-TEK.NS": "MRO_TEK",
                "NAM-INDIA.NS": "NAM_INDIA",
                "SURANAT&P.NS": "SURANAT_P"
            }
            return replacements.get(ticker, ticker.replace('.NS', '').replace('&', '_'))
        
        modified_tickers = [replace_ticker(ticker) for ticker in tickers_meeting_condition]

        # Output to Excel
        sheet.range('L2:T200').clear_contents()
        sheet.range('L2').value = last_entries[['Close', 'RSI', 'K', 'D', 'MACD', 'Condition']]
        
        # Write modified tickers to column Y starting from Y3
        if modified_tickers:
            sheet.range('S3').value = [[ticker] for ticker in modified_tickers]
        else:
            print("No tickers meet the condition.")
        
        wb.save(EXCEL_FILE)
        print("Last entries and tickers meeting condition written to Excel.")

        
def fetch_trading_dataFNOS():
    # Open the Excel file and fetch the symbols
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_FNOSW]
    symbols = sheet.range("S1:S200").value  # Fetch tickers from the sheet
    
    # Filter out any empty cells in the range
    symbols = [symbol for symbol in symbols if symbol]
    
    # Fetching data for each symbol and storing in a list
    data_list = [fetch_trading_fno(symbol) for symbol in symbols]
    
    # Converting the data list to a DataFrame for tabular display
    df = pd.DataFrame(data_list)
    
    # Display the DataFrame in a tabular format
    #print(df)
    
    # Write the DataFrame back to a specified range (U3:X200)
    sheet_output = wb.sheets[SHEET_FNOSW]
    sheet_output.range("T2:W200").clear_contents()  # Clear only the intended range
    sheet_output.range("T2").value = df  # Write DataFrame starting at U3
      

############ FNO SWING  PUT ####################################################

# Function to prepare tickers for processing
def mainFNOSP():
    # Function to prepare tickers for processing
    def prepare_tickers(tickers):
        prepared_tickers = []
        for ticker in tickers:
            if pd.isna(ticker):
                continue
            ticker = str(ticker)
            if ticker == "NIFTY":
                prepared_tickers.append("^NSEI")
            elif ticker == "BANKNIFTY":
                prepared_tickers.append("^NSEBANK")
            elif ticker == "NAM_INDIA":
                prepared_tickers.append("NAM-INDIA.NS")
            elif ticker == "BAJAJ_AUTO":
                prepared_tickers.append("BAJAJ-AUTO.NS")
            elif ticker == "SURANAT_P":
                prepared_tickers.append("SURANAT&P.NS")
            elif ticker == "MRO_TEK":
                prepared_tickers.append("MRO-TEK.NS")
            elif ticker == "M_MFIN":
                prepared_tickers.append("M&MFIN.NS")
            else:
                ticker = ticker.replace('_', '&')
                prepared_tickers.append(ticker + ".NS")
        return prepared_tickers

    def calculate_rsi(data, window=14):
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def calculate_kd(data, k_window=14, d_window=3):
        low_min = data['Low'].rolling(window=k_window).min()
        high_max = data['High'].rolling(window=k_window).max()
        k = 100 * ((data['Close'] - low_min) / (high_max - low_min))
        d = k.rolling(window=d_window).mean()
        return k, d

    def calculate_macd(data):
        ema_short = data['Close'].ewm(span=MACD_SHORT, adjust=False).mean()
        ema_long = data['Close'].ewm(span=MACD_LONG, adjust=False).mean()
        macd_line = ema_short - ema_long
        signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
        macd_histogram = macd_line - signal_line
        return macd_line, signal_line, macd_histogram

    def download_data(ticker):
        stock = yf.Ticker(ticker)
        data = stock.history(interval='15m', period='1mo')
        if data.empty:
            print(f"No data for {ticker}")
            return None
        return data

    # Load Excel file and sheet
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_FNOSW]
    
    # Get tickers from Excel
    tickers = sheet.range('AF1:AF200').value
    tickers = prepare_tickers(tickers)
    
    # Download data in parallel using ThreadPoolExecutor
    data = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_data, tickers))
    
    # Filter out None results
    valid_results = [result for result in results if result is not None]
    if not valid_results:
        print("No valid data was downloaded.")
    else:
        combined_data = pd.concat(valid_results, keys=[ticker for ticker, result in zip(tickers, results) if result is not None])
        
        # Calculate indicators
        combined_data['RSI'] = calculate_rsi(combined_data)
        combined_data['K'], combined_data['D'] = calculate_kd(combined_data)
        combined_data['MACD'], combined_data['MACD_Signal'], combined_data['MACD_Hist'] = calculate_macd(combined_data)
        combined_data['Volume_MA21'] = combined_data['Volume'].rolling(window=VOLUME_MA_WINDOW).mean()
        combined_data['Volume_Spike'] = combined_data['Volume_MA21'] * 1.5

        # Last entries for each ticker
        last_entries = combined_data.groupby(combined_data.index.get_level_values(0)).last()
        
        # Define conditions
        last_entries['Condition'] = (
            (last_entries['RSI'] < last_entries['RSI'].rolling(window=14).mean()) &  # RSI greater than its 14-period MA
            (last_entries['RSI'].shift(1) >= last_entries['RSI'].rolling(window=14).mean().shift(1)) &
            
            (last_entries['K'] < last_entries['D']) &                               # %K above %D
            #(last_entries['Volume'] > last_entries['Volume_Spike']) &               # Volume greater than Volume Spike
            (last_entries['MACD'] < last_entries['MACD_Signal'])                    # MACD above MACD Signal
        )

        # Tickers meeting condition
        tickers_meeting_condition = last_entries[last_entries['Condition']].index.get_level_values(0).unique()

        # Replace tickers with mappings for those meeting condition
        def replace_ticker(ticker):
            replacements = {
                "^NSEBANK": "BANKNIFTY",
                "^NSEI": "NIFTY",
                "BAJAJ-AUTO.NS": "BAJAJ_AUTO",
                "M&MFIN.NS": "M_MFIN",
                "MRO-TEK.NS": "MRO_TEK",
                "NAM-INDIA.NS": "NAM_INDIA",
                "SURANAT&P.NS": "SURANAT_P"
            }
            return replacements.get(ticker, ticker.replace('.NS', '').replace('&', '_'))
        
        modified_tickers = [replace_ticker(ticker) for ticker in tickers_meeting_condition]

        # Output to Excel
        sheet.range('AG2:AN200').clear_contents()
        sheet.range('AG2').value = last_entries[['Close', 'RSI', 'K', 'D', 'MACD', 'Condition']]
        
        # Write modified tickers to column Y starting from Y3
        if modified_tickers:
            sheet.range('AN3').value = [[ticker] for ticker in modified_tickers]
        else:
            print("No tickers meet the condition.")
        
        wb.save(EXCEL_FILE)
        print("Last entries and tickers meeting condition written to Excel.")

        
def fetch_trading_dataFNOSP():
    # Open the Excel file and fetch the symbols
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_FNOSW]
    symbols = sheet.range("AN1:AN200").value  # Fetch tickers from the sheet
    
    # Filter out any empty cells in the range
    symbols = [symbol for symbol in symbols if symbol]
    
    # Fetching data for each symbol and storing in a list
    data_list = [fetch_trading_fno(symbol) for symbol in symbols]
    
    # Converting the data list to a DataFrame for tabular display
    df = pd.DataFrame(data_list)
    
    # Display the DataFrame in a tabular format
    #print(df)
    
    # Write the DataFrame back to a specified range (U3:X200)
    sheet_output = wb.sheets[SHEET_FNOSW]
    sheet_output.range("AQ2:AT200").clear_contents()  # Clear only the intended range
    sheet_output.range("AQ2").value = df  # Write DataFrame starting at U3
      
###############  CASH  ###################################################


# Fetch trading data for a single symbol
# Fetch trading data for a single symbol
def fetch_trading_dataS(symbol):
    url = "https://scanner.tradingview.com/symbol"
    symbol1 = f'NSE:{symbol.replace("NSE_EQ:", "").replace("-", "_").replace("&", "_").upper()}'
    params = {
        "symbol": symbol1,
        "fields": "Recommend.All,Recommend.All|1M,Recommend.All|1W,Recommend.All|15,close|15,Ichimoku.BLine|15",
        "no_404": "True"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Ensure a valid response
        data = response.json()
        
        if data and isinstance(data, dict):
            # Get values with defaults to avoid NoneType comparisons
            close = data.get('close|15', 0) or 0
            recommend_all = data.get('Recommend.All', 0) or 0
            recommend_allw = data.get('Recommend.All|1W', 0) or 0
            recommend_allm = data.get('Recommend.All|1M', 0) or 0
            recommend_all15 = data.get('Recommend.All|15', 0) or 0
            ichimoku_bline = data.get('Ichimoku.BLine|15', 0) or 0
            
            # Check for signal condition
            signal = "True" if (close > ichimoku_bline and recommend_allm > 0.4 and recommend_allw > 0.4 and recommend_all > 0.4 and recommend_all15 > 0.2) else "False"
            return (symbol1, close, signal, recommend_all)
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching data for {symbol1}: {e}")
    return None


# Main function to fetch and save data

def fetch_trading_dataS1():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_SWING]
        sheet.range('B3:F700').clear()
        sheet.range("H3:W300").clear_contents()
        # Get symbols from A1 to A700
        symbols = sheet.range("A1:A3000").value  
        symbols = [s for s in symbols if s]  # Filter out empty symbols
        
        true_tickers = []  # List to store tickers with True signals
        original_symbols = []  # List to store original symbols for "True" tickers
        filtered_results = []  # List to store fetched data

        # Process symbol using concurrent futures
        def process_symbol(symbol):
            return fetch_trading_dataS(symbol)

        # Use ThreadPoolExecutor for concurrent fetching
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_symbol, symbols))

        # Filter results and prepare data for Excel
        for result in results:
            if result:
                symbol1, close, signal, recommend_all = result
                filtered_results.append([symbol1, close, signal, recommend_all])
                if signal == "True":
                    true_tickers.append(symbol1)
                    original_symbols.append(symbol1.replace('NSE:', ''))  # Store original symbol without 'NSE:' prefix

        # Write filtered results to Excel
        if filtered_results:
            sheet.range(f"B3:E{3 + len(filtered_results) - 1}").value = filtered_results

        # Write the "True" tickers to column H and original symbols to column K
        sheet.range("H3:W300").clear_contents()  # Clear previous "True" tickers and related data
        if true_tickers:
            sheet.range("H3").options(transpose=True).value = true_tickers

            # Add original symbols in column K and related data in columns I, J
            for row_offset, (symbol, original_symbol) in enumerate(zip(true_tickers, original_symbols), start=3):
                result = next(res for res in filtered_results if res[0] == symbol)
                if result:
                    _, close, _, recommend_all = result
                    sheet.range(f"I{row_offset}").value = recommend_all
                    sheet.range(f"J{row_offset}").value = create_tradingview_link(symbol)
                    sheet.range(f"K{row_offset}").value = original_symbol  # Write original symbol in column K
        
        wb.save()
    except Exception as e:
        print(f"An error occurred during data fetching: {e}")
                       

######## CASH Y FINANCE ###########


# Function to prepare tickers for processing
def main_CS():
      # Function to prepare tickers for processing
       def prepare_tickers(tickers):
           prepared_tickers = []
           for ticker in tickers:
               if pd.isna(ticker):
                   continue
               ticker = str(ticker)
               if ticker == "NIFTY":
                   prepared_tickers.append("^NSEI")
               elif ticker == "BANKNIFTY":
                   prepared_tickers.append("^NSEBANK")
               elif ticker == "NAM_INDIA":
                   prepared_tickers.append("NAM-INDIA.NS")
               elif ticker == "BAJAJ_AUTO":
                   prepared_tickers.append("BAJAJ-AUTO.NS")
               elif ticker == "SURANAT_P":
                   prepared_tickers.append("SURANAT&P.NS")
               elif ticker == "MRO_TEK":
                   prepared_tickers.append("MRO-TEK.NS")
               elif ticker == "M_MFIN":
                   prepared_tickers.append("M&MFIN.NS")
               else:
                   ticker = ticker.replace('_', '&')
                   prepared_tickers.append(ticker + ".NS")
           return prepared_tickers

       def calculate_rsi(data, window=14):
           delta = data['Close'].diff()
           gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
           loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
           rs = gain / loss
           return 100 - (100 / (1 + rs))

       def calculate_kd(data, k_window=14, d_window=3):
           low_min = data['Low'].rolling(window=k_window).min()
           high_max = data['High'].rolling(window=k_window).max()
           k = 100 * ((data['Close'] - low_min) / (high_max - low_min))
           d = k.rolling(window=d_window).mean()
           return k, d

       def calculate_macd(data):
           ema_short = data['Close'].ewm(span=MACD_SHORT, adjust=False).mean()
           ema_long = data['Close'].ewm(span=MACD_LONG, adjust=False).mean()
           macd_line = ema_short - ema_long
           signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
           macd_histogram = macd_line - signal_line
           return macd_line, signal_line, macd_histogram

       def download_data(ticker):
           stock = yf.Ticker(ticker)
           data = stock.history(interval='15m', period='1mo')
           if data.empty:
               print(f"No data for {ticker}")
               return None
           return data

       # Load Excel file and sheet
       wb = xw.Book(EXCEL_FILE)
       sheet = wb.sheets[SHEET_SWING]
       
       # Get tickers from Excel
       tickers = sheet.range('K1:K200').value
       tickers = prepare_tickers(tickers)
       
       # Download data in parallel using ThreadPoolExecutor
       data = {}
       with ThreadPoolExecutor(max_workers=5) as executor:
           results = list(executor.map(download_data, tickers))
       
       # Filter out None results
       valid_results = [result for result in results if result is not None]
       if not valid_results:
           print("No valid data was downloaded.")
       else:
           combined_data = pd.concat(valid_results, keys=[ticker for ticker, result in zip(tickers, results) if result is not None])
           
           # Calculate indicators
           combined_data['RSI'] = calculate_rsi(combined_data)
           combined_data['K'], combined_data['D'] = calculate_kd(combined_data)
           combined_data['MACD'], combined_data['MACD_Signal'], combined_data['MACD_Hist'] = calculate_macd(combined_data)
           combined_data['Volume_MA21'] = combined_data['Volume'].rolling(window=VOLUME_MA_WINDOW).mean()
           combined_data['Volume_Spike'] = combined_data['Volume_MA21'] * 1.5

           # Last entries for each ticker
           last_entries = combined_data.groupby(combined_data.index.get_level_values(0)).last()
           
           # Define conditions
           last_entries['Condition'] = (
               (last_entries['RSI'] > last_entries['RSI'].rolling(window=14).mean()) &  # RSI greater than its 14-period MA
               (last_entries['RSI'].shift(1) <= last_entries['RSI'].rolling(window=14).mean().shift(1)) &
               
               #(last_entries['K'] > last_entries['D']) &                               # %K above %D
               (last_entries['Volume'] > last_entries['Volume_Spike'])               # Volume greater than Volume Spike
               #(last_entries['MACD'] > last_entries['MACD_Signal'])                    # MACD above MACD Signal
           )

           # Tickers meeting condition
           tickers_meeting_condition = last_entries[last_entries['Condition']].index.get_level_values(0).unique()

           # Replace tickers with mappings for those meeting condition
           def replace_ticker(ticker):
               replacements = {
                   "^NSEBANK": "BANKNIFTY",
                   "^NSEI": "NIFTY",
                   "BAJAJ-AUTO.NS": "BAJAJ_AUTO",
                   "M&MFIN.NS": "M_MFIN",
                   "MRO-TEK.NS": "MRO_TEK",
                   "NAM-INDIA.NS": "NAM_INDIA",
                   "SURANAT&P.NS": "SURANAT_P"
               }
               return replacements.get(ticker, ticker.replace('.NS', '').replace('&', '_'))
           
           modified_tickers = [replace_ticker(ticker) for ticker in tickers_meeting_condition]

           # Output to Excel
           sheet.range('L2:T200').clear_contents()
           sheet.range('L2').value = last_entries[['Close', 'RSI', 'K', 'D', 'MACD', 'Condition']]
           
           # Write modified tickers to column Y starting from Y3
           if modified_tickers:
               sheet.range('S3').value = [[ticker] for ticker in modified_tickers]
           else:
               print("No tickers meet the condition.")
           
           wb.save(EXCEL_FILE)
           print("Last entries and tickers meeting condition written to Excel.")
        

########### CASH TRADING RECO #########

def fetch_trading_dataCS():
    # Open the Excel file and fetch the symbols
    wb = xw.Book(EXCEL_FILE)
    sheet = wb.sheets[SHEET_SWING]
    symbols = sheet.range("S1:S200").value  # Fetch tickers from the sheet
    
    # Filter out any empty cells in the range
    symbols = [symbol for symbol in symbols if symbol]
    
    # Fetching data for each symbol and storing in a list
    data_list = [fetch_trading_fno(symbol) for symbol in symbols]
    
    # Converting the data list to a DataFrame for tabular display
    df = pd.DataFrame(data_list)
    
    # Display the DataFrame in a tabular format
    #print(df)
    
    # Write the DataFrame back to a specified range (U3:X200)
    sheet_output = wb.sheets[SHEET_SWING]
    sheet_output.range("V2:Y200").clear_contents()  # Clear only the intended range
    sheet_output.range("V2").value = df  # Write DataFrame starting at U3
          
##############################  END  ##################


if __name__ == "__main__":
    print(" ")
    print(" ")
    print("        ><(((º> ----------------------------------------------- <º)))>< ",)
    print("      ><(((º> Program designed by RPSTOCKS - KATIYAR - HLD ***    <º)))><",)
    print("    ><(((º>                      xxxxxxxxxxx                        <º)))><",)   
    print("      ><(((º>              Trust your own research.             <º)))><",)
    print("        ><(((º> ----------------------------------------------- <º)))>< ")
    print(" ")
    print(" ")
    print(" No any Change in XLS sheet")
    print(" ")
    print(" ")
   
while True:
    wb = xw.Book(EXCEL_FILE)
    
    sheet = wb.sheets['SWING']
    start_time = time.localtime()
    ts = time.strftime('%H:%M:%S', start_time)
    sheet.range('B1').value = ts
    
      
    pcrn()   #NIFTY PCR
    
    print("-------   FETCH CASH BEST STOCKS   ------")
    sheet.range('AG1').value = " FETCH CASH BEST STOCKS"
    sheet.range('AG2').value = " FETCH CASH BEST STOCKS"
    sheet.range('AG3').value = " FETCH CASH BEST STOCKS"
    
    dat_verification ()
    #far()
    #trading_reco1m
    
    print("-------   FETCH PCR DATA FOR ALL F & O   ------")
    sheet.range('AG1').value = "FETCH PCR DATA FOR ALL F & O"
    sheet.range('AG2').value = "FETCH PCR DATA FOR ALL F & O"
    sheet.range('AG3').value = "FETCH PCR DATA FOR ALL F & O"
    pcr1()
    
    # Fetch Nifty and BankNifty data
    print("-------   FETCH PCR , OPTION CHAIN DATA NIFTY    ------")
    sheet.range('AG1').value = "FETCH PCR DATA NIFTY AND BANKNIFTY"
    sheet.range('AG2').value = "FETCH PCR DATA NIFTY AND BANKNIFTY"
    sheet.range('AG3').value = "FETCH PCR DATA NIFTY AND BANKNIFTY"
    
    niftychain()
    time.sleep(1)
   
    # Perform other fetch operations and update Excel
    print("-------   FETCH F&O DATA   ------")
    sheet.range('AG1').value = "FETCH F&O DATA"
    sheet.range('AG2').value = "FETCH F&O DATA"
    sheet.range('AG3').value = "FETCH F&O DATA"
    
    fetch_trading_data12()  # Uncomment if needed
    time.sleep(1)
    
    copy_pest()
    
    print("-------   FETCH F&O CALL INTRADAY SIGNAL   ------")
    sheet.range('AG1').value = "FETCH F&O CALL INTRADAY SIGNAL"
    sheet.range('AG2').value = "FETCH F&O CALL INTRADAY SIGNAL"
    sheet.range('AG3').value = "FETCH F&O CALL INTRADAY SIGNAL"
    
    main_scr()  # Uncomment if needed
    fetch_trading_fno1()
    
    print("-------   FETCH F&O PUT INTRADAY SIGNAL   ------")
    sheet.range('AG1').value = "FETCH F&O PUT INTRADAY SIGNAL"
    sheet.range('AG2').value = "FETCH F&O PUT INTRADAY SIGNAL"
    sheet.range('AG3').value = "FETCH F&O PUT INTRADAY SIGNAL"
    
    main_fnoPE()  # Uncomment if needed
    fetch_trading_data12PE()
    
    print("-------   FATCH F&O SWING DATA   ------")
    sheet.range('AG1').value = "- FATCH F&O SWING DATA -"
    sheet.range('AG2').value = "- FATCH F&O SWING DATA -"
    sheet.range('AG3').value = "- FATCH F&O SWING DATA -"
    
    fetch_trading_dataFNO()
    
   
    print("-------   FATCH F&O - CALL - SWING SIGNAL   ------")
    sheet.range('AG1').value = "-FATCH F&O - CALL - SWING SIGNAL -"
    sheet.range('AG2').value = "-FATCH F&O - CALL - SWING SIGNAL -"
    sheet.range('AG3').value = "-FATCH F&O - CALL - SWING SIGNAL -"
    
    mainFNOS()
    fetch_trading_dataFNOS()
    
    print("-------  FATCH F&O - PUT - SWING SIGNAL  ------")
    sheet.range('AG1').value = "- FATCH F&O - PUT - SWING SIGNAL -"
    sheet.range('AG2').value = "- FATCH F&O - PUT - SWING SIGNAL -"
    sheet.range('AG3').value = "- FATCH F&O - PUT - SWING SIGNAL -"
    
    mainFNOSP()
    fetch_trading_dataFNOSP()
    
        
    sheet.range('C1').clear_contents()
    symbols = sheet.range("A1:A3000").value  # Get symbols from A1 to A200
    total_symbols = len([s for s in symbols if s]) 
    
    
    print(f"SCAN Total symbols: {total_symbols} - FACHING IN BACKGROUND ")
    print("------------------------------------------------------")
    
    sheet.range('AG1').value = f"SCAN Total symbols: {total_symbols} - FACHING IN BACKGROUND "
    sheet.range('AG2').value = f"SCAN Total symbols: {total_symbols} - FACHING IN BACKGROUND "
    sheet.range('AG3').value = f"SCAN Total symbols: {total_symbols} - FACHING IN BACKGROUND "
    
    fetch_trading_dataS1()
    
       
    sheet.range('AG1').value = "-------   FATCH -- CASH -  SWING SIGNAL   ------"
    sheet.range('AG2').value = "-------   FATCH -- CASH -  SWING SIGNAL   ------"
    sheet.range('AG3').value = "-------   FATCH -- CASH -  SWING SIGNAL   ------"
    
    
    main_CS()
    
    
    sheet.range('AG1').value = "-------   CALCULATE REALL VALUE   ------"
    sheet.range('AG2').value = "-------   CALCULATE REALL VALUE   ------"
    sheet.range('AG3').value = "-------   CALCULATE REALL VALUE   ------"
    
    fetch_trading_dataCS()
    
    time.sleep(1)
    end_time = time.localtime()
    te = time.strftime('%H:%M:%S', end_time)
    sheet.range('C1').value = te
    print(" - - - ")
    
    
    # Sleep with countdown and restart loop after sleep
    try:
        sheet.range('AG1').value = f"Sleeping for {SLEEPTIME} seconds..."
        sheet.range('AG2').value = f"Sleeping for {SLEEPTIME} seconds..."
        sheet.range('AG3').value = f"Sleeping for {SLEEPTIME} seconds..."
        print(f"Sleeping for {SLEEPTIME} seconds...")
        
        for remaining in range(SLEEPTIME, 0, -1):
            update_countdown_in_excel(wb, remaining)
            time.sleep(1)
        
        update_countdown_in_excel(wb, "Updating data...")
    
    except Exception as e:
        print(f"An error occurred: {e}")