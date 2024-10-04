# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 07:48:42 2024

@author: DGM-QA
"""


import requests
import pandas as pd
import xlwings as xw
import time
from datetime import datetime, timedelta
import yfinance as yf
import pytz
import numpy as np
import concurrent.futures
import sys

# Constants
SHEET_ALL = 'YFIN1'
SHEET_SWING = 'SWING'
SHEET_FNOSW = 'FNOSW'
INTERVALS = '1d'
SLEEPTIME = 60  # Sleep time in seconds
MAX_RETRIES = 3  # Max retries for fetching data
EXCEL_FILE = 'YFin.xls'  # Replace with your actual file path
COUNTDOWN_SHEET_NAME = 'YFIN1'
PE_SHEET_NAME = 'YFINPE'
SWING_LIST = 'LIST'
SHEET_FINAL = 'FINAL'
MAX_WORKERS = 10  # Number of threads for concurrent fetching


def open_xls_and_hide_ribbon(file_path, sheet_name):
    app = xw.App(visible=True)  # Open Excel application
    wb = app.books.open(file_path)  # Open the specified workbook
    app.api.ExecuteExcel4Macro('SHOW.TOOLBAR("Ribbon", False)')  # Hide the ribbon

    # Activate the specified sheet by name
    try:
        sheet = wb.sheets[sheet_name]
        sheet.activate()  # Activate the sheet
        print(f"Activated sheet: {sheet_name}")
    except KeyError:
        print(f"Sheet '{sheet_name}' not found in the workbook.")

    return app, wb



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



#########################


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

# Main function to fetch and save data
# Main function to fetch and save data
def fetch_trading_data12():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_ALL]
        sheet.range('B3:AU700').clear()

        symbols = sheet.range("A1:A700").value  # Get symbols from A1 to A700
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

        
########################################



def create_tradingview_link(symbol):
    """Create a TradingView chart link."""
    base_url = "https://in.tradingview.com/chart/?symbol="
    return f'=HYPERLINK("{base_url}{symbol}", "CHART")'

#####################################

def fetch_tickers(sheet):
    """Read tickers from the specified range in the Excel sheet."""
    return sheet.range("K1:K200").value  # Adjust the range as needed

def fetch_tickersPe(sheet1):
    """Read tickers from the specified range in the Excel sheet."""
    return sheet1.range("AF1:AF200").value  # Adjust the range as needed

def prepare_tickers(tickers):
    # Initialize an empty list to hold the prepared tickers
    prepared_tickers = []
    
    # Iterate through the tickers fetched from the sheet
    for ticker in tickers:
        if ticker:
            # Replace specific tickers with their desired values
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
                  
                
            else:
                # Append ".NS" to other tickers
                prepared_tickers.append(ticker + ".NS")
    
    return prepared_tickers

#def prepare_tickers(tickers):
    #"""Prepare tickers for yfinance."""
    #return [ticker + ".NS" for ticker in tickers if ticker]  # Ensure correct ticker format and skip empty cells


def fetch_historical_data(ticker, days_back):
    """Fetch historical price data from yfinance with a 5-minute interval."""
    end_date = datetime.now(pytz.utc)  # Ensure end_date is timezone-aware
    start_date = end_date - timedelta(days=int(days_back))
    
    # Ensure start_date is timezone-aware
    start_date = start_date.replace(tzinfo=pytz.utc)
    
    date_format = '%Y-%m-%d'
    
    # Fetch data with a 5-minute interval
    data = yf.download(ticker, start=start_date.strftime(date_format), end=end_date.strftime(date_format), interval='5m')
    
    if not data.empty:
        # Convert index to timezone-aware if necessary
        if data.index.tzinfo is None:
            data.index = data.index.tz_localize(pytz.utc)
        
        # Sorting data by index (timestamp)
        data = data.sort_index(ascending=True)
        
        # Collecting the last 'days_back' number of days of data
        end_datetime = datetime.now(pytz.utc)
        start_datetime = end_datetime - timedelta(days=int(days_back))
        
        # Ensure start_datetime is timezone-aware
        start_datetime = start_datetime.replace(tzinfo=pytz.utc)
        
        # Filtering data to only include the requested range
        filtered_data = data.loc[start_datetime:end_datetime]
        
        prices = filtered_data['Close'].tolist()
        volumes = filtered_data['Volume'].tolist()
    else:
        prices = []
        volumes = []
    
    return prices, volumes


def fetch_last_quotes(prepared_tickers):
    """Fetch the last close price and volume for each ticker."""
    data_dict = {'Ticker': [], 'Last Close': [], 'Last Volume': []}
    for ticker in prepared_tickers:
        ticker_yahoo = yf.Ticker(ticker)
        data = ticker_yahoo.history()
        last_quote = data['Close'].iloc[-1] if not data.empty else None
        last_volume = data['Volume'].iloc[-1] if not data.empty else None
        data_dict['Ticker'].append(ticker)
        data_dict['Last Close'].append(last_quote)
        data_dict['Last Volume'].append(last_volume)
    return data_dict

def calculate_required_average(prices, last_value):
    """Calculate the average of historical data and the current last value (price or volume)."""
    total_values = sum(prices) + last_value if last_value else sum(prices)
    total_days = len(prices) + 1 if last_value else len(prices)
    combined_avg = total_values / total_days if total_days > 0 else 0
    return round(combined_avg, 2)

def save_to_excel(df, cell, sheet_name=COUNTDOWN_SHEET_NAME):
    """Save data to an Excel sheet."""
    wb = xw.Book(EXCEL_FILE)
    if sheet_name not in [sheet.name for sheet in wb.sheets]:
        wb.sheets.add(sheet_name)
    sheet = wb.sheets[sheet_name]
    sheet.range(cell).value = df
    wb.save(EXCEL_FILE)
    print(f"Data written to Excel sheet '{sheet_name}' successfully.")
    
def save_to_excelPe(df, cell, sheet_name=COUNTDOWN_SHEET_NAME):
    """Save data to an Excel sheet."""
    wb1 = xw.Book(EXCEL_FILE)
    if sheet_name not in [sheet.name for sheet in wb1.sheets]:
        wb1.sheets.add(sheet_name)
    sheet = wb1.sheets[sheet_name]
    sheet.range(cell).value = df
    wb1.save(EXCEL_FILE)
    print(f"Data written to Excel sheet '{sheet_name}' successfully.")
    

def fetch_trading_dataSW1(true_tickers):
    try:
        

        url = "https://scanner.tradingview.com/symbol"

        # Write headers if not already present
       
        reall_data = {}  # Dictionary to store REALL values

        for index, symbol in enumerate(true_tickers, start=1):
            if symbol is None:
                continue

            # Clean up the symbol
            if symbol.startswith('NSE_EQ:'):
                symbol = symbol.replace('NSE_EQ:', '')

            symbol = symbol.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'

            params_15 = {
                "symbol": symbol1,
                "fields": "Recommend.All|15,",
                "no_404": "True"
            }

            response_15 = requests.get(url, params=params_15)

            if response_15.status_code == 200:
                try:
                    data_15 = response_15.json()
                except ValueError:
                    print(f"Response not in JSON format for symbol {symbol1}: {response_15.text}")
                    continue

                if data_15 and isinstance(data_15, dict):
                    REALL = data_15.get('Recommend.All|15', 0)
                    reall_data[symbol1] = REALL  # Store REALL value

        return reall_data  # Return the dictionary of REALL values

    except Exception as e:
        print(f"An error occurred during data fetching: {e}")
        return {}


def main_scr():
    
# Main script
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[COUNTDOWN_SHEET_NAME]
        sheet.range("M3:AA200").clear() 
        
        # Read and prepare tickers
        tickers = fetch_tickers(sheet)
        prepared_tickers = prepare_tickers(tickers)
        
        total_tickers = len(prepared_tickers)
        
        # Fetch last quotes and convert to DataFrame
        data_dict = fetch_last_quotes(prepared_tickers)
        
        # Initialize a list to store results
        results = []
        
        # Process each ticker
        true_tickers = []  # List to store tickers with a "True" signal
        
        for i, ticker in enumerate(prepared_tickers):
            prices_1, volumes_1 = fetch_historical_data(ticker, 3)
            prices_2, volumes_2 = fetch_historical_data(ticker, 4)
            prices_3, volumes_3 = fetch_historical_data(ticker, 8)
            
            last_close = data_dict['Last Close'][data_dict['Ticker'].index(ticker)]
    
            
           # Calculate RSI from the last 14 days of close prices
            historical_data = yf.download(ticker, period='5d', interval='5m')  # Fetch 14 days of data
            close_prices = historical_data['Close']
            
            delta = close_prices.diff(1)
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            rsi_ma = rsi.rolling(window=14).mean()

            rsi_current = rsi.iloc[-1] if not rsi.empty else None
            rsi_ma_current = rsi_ma.iloc[-1] if not rsi_ma.empty else None
            
            prev_rsi = rsi.iloc[-2] if len(rsi) > 1 else None
            prev_rsi_ma = rsi_ma.iloc[-2] if len(rsi_ma) > 1 else None
            
            signal = "True" if (rsi_current > rsi_ma_current and prev_rsi <= prev_rsi_ma) else "False"
                     
            
            if signal == "True":
                true_tickers.append(ticker)  # Add ticker to the list if signal is "True"
                
            results.append({
                'Ticker': ticker,
                'Last Close': round(last_close, 2),
                'RSI': round(rsi_current, 2),
                'RSI MA': round(rsi_ma_current, 2),
                #'SMA 13 5m': required_avg_3,
                #'Last Volume': round(last_volume, 2),
                #'Vol Avg 5 5m': volume_avg_1,
                #'Vol Avg 9 5m': volume_avg_2,
                #'Vol Avg 13 5m': volume_avg_3,
                'Signal': signal
            })
            
            print(f"{i + 1} of {total_tickers} completed. Processing in the background...")
    
        # Convert the list of dictionaries to a DataFrame
        results_df = pd.DataFrame(results)
        
        # Save all results to the Excel file
        save_to_excel(results_df, "M2")
        
        # Save only the "True" tickers to the "YFIN1" sheet in column AA
        sheet.range("T3:W100").clear_contents()
        if true_tickers:
                
            wb.sheets[COUNTDOWN_SHEET_NAME].range("T3").options(transpose=True).value = true_tickers
        
        # Create TradingView links and save to Excel
        # Assume `true_tickers` contains a list of tickers you want to process
        for i, ticker in enumerate(true_tickers):
            # Clean and prepare the symbol
            single_data = fetch_historical_data_bulk([ticker], interval=INTERVALS)
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'
            reall_data = fetch_trading_dataSW1(true_tickers)
            
            
            headers = ['Symbol',  'close', 'Reall' , 'SYMBEL' ]
            wb.sheets[COUNTDOWN_SHEET_NAME].range("T2").value = headers
            single_data = fetch_historical_data_bulk([ticker], interval=INTERVALS)
                      
            if ticker in single_data:
                last_close = single_data[ticker]['Close'].iloc[-1]
                
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'  # Fixed missing closing quote
            
            # Create TradingView link
                      
            
            # Write link to the Excel sheet
            wb.sheets[COUNTDOWN_SHEET_NAME].range(f"U{3 + i}").value = round(last_close, 2),
            #wb.sheets[COUNTDOWN_SHEET_NAME].range(f"V{3 + i}").value = link
            wb.sheets[COUNTDOWN_SHEET_NAME].range(f"W{3 + i}").value = symbol1
            reall_value = reall_data.get(symbol1, None)
            wb.sheets[COUNTDOWN_SHEET_NAME].range(f"V{3 + i}").value = reall_value
            
            #for row in range(3, len(true_tickers) + 3):
                                
                #formula = f'=INDEX(YFIN1!F:F, MATCH(W{row}, YFIN1!B:B, 0))'
                #sheet.range(f"V{row}").formula = formula
                
            print (true_tickers)
            pass
    except Exception as e:
                   print(f"An error occurred in main_scr: {e}")
                   
                   print("Background processing complete. Data saved successfully.")
                


def main_scrPe():
    # Main script
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[COUNTDOWN_SHEET_NAME]
        sheet.range("AG3:AV200").clear()  # Clear previous data in the specified range

        # Read and prepare tickers
        tickers = fetch_tickersPe(sheet)
        prepared_tickers = prepare_tickers(tickers)

        total_tickers = len(prepared_tickers)

        # Fetch last quotes and convert to DataFrame
        data_dict = fetch_last_quotes(prepared_tickers)

        # Initialize a list to store results
        results = []

        # List to store tickers with a "True" signal
        true_tickers = []

        for i, ticker in enumerate(prepared_tickers):
            # Fetch historical data
            prices_1, volumes_1 = fetch_historical_data(ticker, 4)
            prices_2, volumes_2 = fetch_historical_data(ticker, 8)
            prices_3, volumes_3 = fetch_historical_data(ticker, 12)

            # Get last close and volume
            last_close = data_dict['Last Close'][data_dict['Ticker'].index(ticker)]
            last_volume = data_dict['Last Volume'][data_dict['Ticker'].index(ticker)]

            # Check if last_close or last_volume is None and handle accordingly
            last_close = last_close if last_close is not None else 0
            last_volume = last_volume if last_volume is not None else 0


            # Calculate RSI from the last 14 days of close prices
            historical_data = yf.download(ticker, period='5d', interval='5m')
            close_prices = historical_data['Close']

            delta = close_prices.diff(1)
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            rsi_ma = rsi.rolling(window=14).mean()

            rsi_current = rsi.iloc[-1] if not rsi.empty else None
            rsi_ma_current = rsi_ma.iloc[-1] if not rsi_ma.empty else None

            prev_rsi = rsi.iloc[-2] if len(rsi) > 1 else None
            prev_rsi_ma = rsi_ma.iloc[-2] if len(rsi_ma) > 1 else None

            signal = "True" if (rsi_current and rsi_ma_current and rsi_current < rsi_ma_current and prev_rsi >= prev_rsi_ma) else "False"

            if signal == "True":
                true_tickers.append(ticker)  # Add ticker to the list if signal is "True"

            results.append({
                'Ticker': ticker,
                'Last Close': round(last_close, 2) if last_close is not None else "N/A",
                'RSI': round(rsi_current, 2) if rsi_current is not None else "N/A",
                'RSI MA': round(rsi_ma_current, 2) if rsi_ma_current is not None else "N/A",
                'Signal': signal
            })

            print(f"{i + 1} of {total_tickers} completed. Processing in the background...")

        # Convert the list of dictionaries to a DataFrame
        results_df = pd.DataFrame(results)

        # Save all results to the Excel file
        save_to_excelPe(results_df, "AH2")

        # Save only the "True" tickers to the "YFIN1" sheet in column AA
        #sheet.range("AO3:AR100").clear_contents()
        if true_tickers:
                
            wb.sheets[COUNTDOWN_SHEET_NAME].range("AO3").options(transpose=True).value = true_tickers
        
        # Create TradingView links and save to Excel
        # Assume `true_tickers` contains a list of tickers you want to process
        for i, ticker in enumerate(true_tickers):
            # Clean and prepare the symbol
            single_data = fetch_historical_data_bulk([ticker], interval=INTERVALS)
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'
            reall_data = fetch_trading_dataSW1(true_tickers)
            
            
            headers = ['Symbol',  'close', 'Reall' , 'SYMBEL' ]
            wb.sheets[COUNTDOWN_SHEET_NAME].range("AO2").value = headers
            single_data = fetch_historical_data_bulk([ticker], interval=INTERVALS)
                      
            if ticker in single_data:
                last_close = single_data[ticker]['Close'].iloc[-1]
                
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'  # Fixed missing closing quote
            
            # Create TradingView link
        
            
            
            # Write link to the Excel sheet
            wb.sheets[COUNTDOWN_SHEET_NAME].range(f"AP{3 + i}").value = round(last_close, 2),
            #wb.sheets[COUNTDOWN_SHEET_NAME].range(f"V{3 + i}").value = link
            wb.sheets[COUNTDOWN_SHEET_NAME].range(f"AR{3 + i}").value = symbol1
            reall_value = reall_data.get(symbol1, None)
            wb.sheets[COUNTDOWN_SHEET_NAME].range(f"AQ{3 + i}").value = reall_value
            
            #for row in range(3, len(true_tickers) + 3):
                                
                #formula = f'=INDEX(YFIN1!F:F, MATCH(W{row}, YFIN1!B:B, 0))'
                #sheet.range(f"V{row}").formula = formula
                
            print (true_tickers)
            pass
    except Exception as e:
                   print(f"An error occurred in main_scr: {e}")
                   
                   print("Background processing complete. Data saved successfully.")
                

############################3  SWING #############



# Fetch trading data for a single symbol
def fetch_trading_dataS(symbol):
    url = "https://scanner.tradingview.com/symbol"
    symbol1 = f'NSE:{symbol.replace("NSE_EQ:", "").replace("-", "_").replace("&", "_").upper()}'
    params = {
        "symbol": symbol1,
        "fields": "Recommend.All,Recommend.All|15,close|15,Ichimoku.BLine|15",
        "no_404": "True"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Ensure a valid response
        data = response.json()
        if data and isinstance(data, dict):
            close = round(data.get('close|15', 0), 2)
            recommend_all = round(data.get('Recommend.All', 0), 2)
            recommend_all15 = round(data.get('Recommend.All|15', 0), 2)
            ichimoku_bline = round(data.get('Ichimoku.BLine|15', 0), 2)
            
# Check for signal condition
            signal = "True" if (close > ichimoku_bline and recommend_all > 0.4 and recommend_all15 > 0.4) else "False"
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

        symbols = sheet.range("A1:A1700").value  # Get symbols from A1 to A700
        symbols = [s for s in symbols if s]  # Filter out empty symbols
        

        true_tickers = []  # List to store tickers with True signals
        original_symbols = []  # List to store original symbols for "True" tickers
        results = []  # List to store fetched data

        # Process symbol using concurrent futures
        def process_symbol(symbol):
            return fetch_trading_dataS(symbol)

        # Use ThreadPoolExecutor for concurrent fetching
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_symbol, symbols))

        # Filter results and update the Excel sheet in one go
        row = 3
        filtered_results = []
        for result in results:
            if result:
                symbol1, close, signal, recommend_all = result
                filtered_results.append([symbol1, close, signal, recommend_all])
                if signal == "True":
                    true_tickers.append(symbol1)
                    original_symbols.append(symbol1.replace('NSE:', ''))  # Store original symbol without 'NSE:' prefix

        # Write data to Excel in bulk
        if filtered_results:
            sheet.range(f"B{row}:E{row + len(filtered_results) - 1}").value = filtered_results

        # Write the "True" tickers to column H and original symbols to column L
        sheet.range("H3:K300").clear_contents()  # Clear the previous "True" tickers and related data
        if true_tickers:
            sheet.range("H3").options(transpose=True).value = true_tickers

            # Add original symbols in column L and related data in columns I, J
            for row_offset, (symbol, original_symbol) in enumerate(zip(true_tickers, original_symbols), start=3):
                # Find the corresponding result for the symbol
                result = next(res for res in filtered_results if res[0] == symbol)
                if result:
                    _, close, _, recommend_all = result
                    #formula = f'=INDEX(A:A, MATCH(H{row_offset}, B:B, 0))'
                    #sheet.range(f"K{row_offset}").formula = formula
                    sheet.range(f"I{row_offset}").value = recommend_all
                    sheet.range(f"J{row_offset}").value = create_tradingview_link(symbol)
                    sheet.range(f"K{row_offset}").value = original_symbol  # Write original symbol in column L
        
        wb.save()
    except Exception as e:
        print(f"An error occurred during data fetching: {e}")

        
########################################

def prepare_tickersSW():
    """Prepare tickers for fetching data from Excel."""
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_SWING]
        tickers = sheet.range("K1:K700").value
        prepared_tickersS = []

        for ticker in tickers:
            if ticker:
                if ticker == "NIFTY":
                    prepared_tickersS.append("^NSEI")
                elif ticker == "BANKNIFTY":
                    prepared_tickersS.append("^NSEBANK")
                elif ticker == "NAM_INDIA":
                    prepared_tickersS.append("NAM-INDIA.NS")
                elif ticker == "BAJAJ_AUTO":
                    prepared_tickersS.append("BAJAJ-AUTO.NS")
                elif ticker == "SURANAT_P":
                    prepared_tickersS.append("SURANAT&P.NS")      
                    
                else:
                    # Replace '_' with '&' in place and append '.NS'
                    ticker = ticker.replace('_', '&')
                    prepared_tickersS.append(ticker + ".NS")
                    
        return prepared_tickersS

    except Exception as e:
        print(f"Error while preparing tickers: {e}")
        return []


def fetch_historical_data_bulk(tickers, interval='1d', days_back=300):
    """Fetch historical data for multiple tickers in bulk using yfinance."""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days_back)
    
    # Join tickers into a single string separated by spaces
    tickers_str = ' '.join(tickers)
    
    try:
        # Download data for all tickers at once
        data = yf.download(tickers_str, start=start_date, end=end_date, interval=interval, group_by='ticker', threads=True)
        
        # Handle the case when multiple tickers are fetched
        if isinstance(data.columns, pd.MultiIndex):
            # Multiple tickers
            successful_data = {}
            problematic_tickers = []
            for ticker in tickers:
                if ticker in data.columns.levels[0]:
                    ticker_data = data[ticker].dropna()
                    if not ticker_data.empty:
                        successful_data[ticker] = ticker_data
                    else:
                        problematic_tickers.append(ticker)
                else:
                    problematic_tickers.append(ticker)
        else:
            # Single ticker
            successful_data = {tickers[0]: data.dropna()} if not data.empty else {}
            problematic_tickers = [tickers[0]] if data.empty else []
        
        if problematic_tickers:
            print(f"Tickers with missing or problematic data: {problematic_tickers}")
        
        return successful_data
    
    except Exception as e:
        print(f"Error fetching bulk data: {e}")
        return {}



def calculate_indicators(data):
    """Calculate technical indicators for each ticker's historical data."""
    indicators = {}
    for ticker, df in data.items():
        if df.empty:
            continue
        
        # Calculate MACD
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        
        # Calculate Stochastic Oscillator
        low_min = df['Low'].rolling(window=14).min()
        high_max = df['High'].rolling(window=14).max()
        k = 100 * ((df['Close'] - low_min) / (high_max - low_min))
        d = k.rolling(window=3).mean()

        # Calculate Volume Average
        volume_avg_21 = df['Volume'].rolling(window=21).mean()

        # Calculate RSI from the last 14 days of close prices
        historical_data = yf.download(ticker, period='1mo', interval='15m')  # Fetch 14 days of data
        close_prices = historical_data['Close']
        
        delta = close_prices.diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi_ma = rsi.rolling(window=14).mean()
       
        rsi_current = rsi.iloc[-1] if not rsi.empty else None
        rsi_ma_current = rsi_ma.iloc[-1] if not rsi_ma.empty else None
        
        prev_rsi = rsi.iloc[-2] if len(rsi) > 1 else None
        prev_rsi_ma = rsi_ma.iloc[-2] if len(rsi_ma) > 1 else None
         
        # Calculate Average True Range (ATR)
        true_range = pd.concat([
            df['High'] - df['Low'],
            np.abs(df['High'] - df['Close'].shift()),
            np.abs(df['Low'] - df['Close'].shift())
        ], axis=1).max(axis=1)
        atr = true_range.rolling(window=14).mean()
        
        # Detect volume spike
        volume_spike = df['Volume'].iloc[-1] > 1.5 * volume_avg_21.iloc[-1]


        # Save indicator results
        indicators[ticker] = {
            'Last Close': df['Close'].iloc[-1],
            'MACD': macd.iloc[-1],
            'MACD Signal': signal.iloc[-1],
            'Stochastic K': k.iloc[-1],
            'Stochastic D': d.iloc[-1],
            'Volume Avg 21': volume_avg_21.iloc[-1],
            'Volume Spike': volume_spike,
            'RSI': rsi_current,
            'RSI_MA' : rsi_ma_current,
            'PRV_RSI' : prev_rsi,
            'PREV_RSI_MA' : prev_rsi_ma,
            'ATR': atr.iloc[-1]
        }
    return indicators


def mainS():
    """Main function to execute the trading analysis process."""
    try:
        # Preparation and Data Fetching
        prepared_tickers = prepare_tickersSW()
        total_tickers = len(prepared_tickers)
        data = fetch_historical_data_bulk(prepared_tickers, interval=INTERVALS)
        indicators = calculate_indicators(data)
        
        results = []
        true_tickers = []
        
        for i, (ticker, ind) in enumerate(indicators.items()):
            last_close = ind.get('Last Close')
            macd = ind.get('MACD')
            macd_signal = ind.get('MACD Signal')
            k = ind.get('Stochastic K')
            d = ind.get('Stochastic D')
            atr = ind.get('ATR')
            #volume_spike = data[ticker]['Volume'].iloc[-1] > 1.5 * ind.get('Volume Avg 21', 0)
            volume_spike = ind.get('Volume Spike')
            
            
            rsi1 = ind.get('RSI')
            rsi_ma1 = ind.get('RSI_MA')
            #prv_rsi1 = ind.get('PRV_RSI')
            #prv_rsi_ma = ind.get('PREV_RSI_MA')
            
            
            # Determine signal condition
            if rsi1 is not None and rsi_ma1 is not None:
                signal = "True" if (
                    rsi1 > rsi_ma1 and 
                    rsi1 > 50 and
                    macd > macd_signal and 
                    volume_spike and
                    k > d
                ) else "False"
            else:
                signal = "False"
            
            if signal == "True":
                true_tickers.append(ticker)
            
            # Append results
            results.append({
                'Ticker': ticker,
                'Last Close': round(last_close, 2) if last_close else None,
                'Signal': signal,
               
            })
            
            # Progress Update
            print(f"{i + 1} of {total_tickers} completed. Processing in the background...")
            
       
        
        # Save results to the Excel file
        results_df = pd.DataFrame(results)
        wb = xw.Book(EXCEL_FILE)
        sheet_swing = wb.sheets[SHEET_SWING]
        
        # Write results to the "SWING" sheet
        sheet_swing.range('N2:P600').clear_contents()
        sheet_swing.range('N2').options(index=False, header=True).value = results_df
        
        # Write "True" tickers to the "YFINCE" section in the "SWING" sheet
        sheet_swing.range('T3:Z200').clear_contents()  # Clear previous data
        if true_tickers:
            
            headers = ['Symbol', 'Close', 'TGT', 'SL', 'SYMBOL', 'Chart', 'REALL']
            sheet_swing.range('T2').value = headers
            sheet_swing.range('T3').options(transpose=False).value = [[ticker] for ticker in true_tickers]
        
        for i, ticker in enumerate(true_tickers):
            # Fetch historical data for the current true ticker
            
            single_data = fetch_historical_data_bulk([ticker], interval=INTERVALS)
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'
            
            if ticker in single_data:
                last_close = single_data[ticker]['Close'].iloc[-1]
                atr = indicators[ticker].get('ATR', 0)
                link = create_tradingview_link(symbol1)
                
                # Write to Excel
                sheet_swing.range(f"U{3 + i}").value = round(last_close, 2)
                sheet_swing.range(f"V{3 + i}").value = round(last_close + 2 * atr, 2)
                sheet_swing.range(f"W{3 + i}").value = round(last_close - 1.5 * atr, 2)
                sheet_swing.range(f"X{3 + i}").value = symbol1
                sheet_swing.range(f"Y{3 + i}").value = link
                
        
        wb.save()
        print("Data saved successfully.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

def fetch_trading_dataSW():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_SWING]
        
        url = "https://scanner.tradingview.com/symbol"
        
        symbols = sheet.range("X3:X200").value  # Get symbols from A1 to A200
        total_symbols = len([s for s in symbols if s])  # Count non-empty symbols
        
        for index, symbol in enumerate(symbols, start=1):  # Start writing data from row 3
            if symbol is None:
                continue

            if symbol.startswith('NSE:'):
                symbol = symbol.replace('NSE:', '')
    
            symbol = symbol.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            
            symbol1 = f'NSE:{symbol}'
            
            params_15 = {
                "symbol": symbol1,
                "fields": "Recommend.All|15,",
                "no_404": "True"
            }

            response_15 = requests.get(url, params=params_15)

            if response_15.status_code == 200:
                try:
                    data_15 = response_15.json()
                except ValueError:
                    print(f"Response not in JSON format for symbol {symbol1}: {response_15.text}")
                    continue

                if not data_15 or not isinstance(data_15, dict):
                    print(f"No valid data received for {symbol1}")
                    continue

                def round_data(data):
                    if isinstance(data, dict):
                        return {k: round_data(v) for k, v in data.items()}
                    elif isinstance(data, list):
                        return [round_data(elem) for elem in data]
                    elif isinstance(data, (int, float)):
                        return round(data, 2)
                    return data

                rounded_data_15 = round_data(data_15)

                REALL = rounded_data_15.get('Recommend.All|15', 0)
             
                excel_data = [
                    REALL,
                ]

                sheet.range(f"Z{2 + index}").value = excel_data
                
                print_progress_bar(index + 1, total_symbols)
                
                
    except Exception as e:
        print(f"An error occurred: {e}")
        


###############   FNO   SWING  #################



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
            signal = "True" if (close > ichimoku_bline and recommend_all > 0.2) else "False"
            signalPE = "TruePE" if (close < ichimoku_bline and recommend_all < 0) else "False"
            return (symbol1, close, signal, recommend_all, signalPE)
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching data for {symbol1}: {e}")
    return None

# Main function to fetch and save data
# Main function to fetch and save data
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

        
########################################

def prepare_tickersFNO():
    """Prepare tickers for fetching data from Excel."""
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_FNOSW]
        tickers = sheet.range("K1:K700").value
        prepared_tickersS = []

        for ticker in tickers:
            if ticker:
                if ticker == "NIFTY":
                    prepared_tickersS.append("^NSEI")
                elif ticker == "BANKNIFTY":
                    prepared_tickersS.append("^NSEBANK")
                elif ticker == "NAM_INDIA":
                    prepared_tickersS.append("NAM-INDIA.NS")
                elif ticker == "BAJAJ_AUTO":
                    prepared_tickersS.append("BAJAJ-AUTO.NS") 
                elif ticker == "SURANAT_P":
                    prepared_tickersS.append("SURANAT&P.NS")      
                else:
                    # Replace '_' with '&' in place and append '.NS'
                    ticker = ticker.replace('_', '&')
                    prepared_tickersS.append(ticker + ".NS")
                    
        return prepared_tickersS

    except Exception as e:
        print(f"Error while preparing tickers: {e}")
        return []


def fetch_historical_data_bulkFNO(tickers, interval='1d', days_back=300):
    """Fetch historical data for multiple tickers in bulk using yfinance."""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days_back)
    
    # Join tickers into a single string separated by spaces
    tickers_str = ' '.join(tickers)
    
    try:
        # Download data for all tickers at once
        data = yf.download(tickers_str, start=start_date, end=end_date, interval=interval, group_by='ticker', threads=True)
        
        # Handle the case when multiple tickers are fetched
        if isinstance(data.columns, pd.MultiIndex):
            # Multiple tickers
            successful_data = {}
            problematic_tickers = []
            for ticker in tickers:
                if ticker in data.columns.levels[0]:
                    ticker_data = data[ticker].dropna()
                    if not ticker_data.empty:
                        successful_data[ticker] = ticker_data
                    else:
                        problematic_tickers.append(ticker)
                else:
                    problematic_tickers.append(ticker)
        else:
            # Single ticker
            successful_data = {tickers[0]: data.dropna()} if not data.empty else {}
            problematic_tickers = [tickers[0]] if data.empty else []
        
        if problematic_tickers:
            print(f"Tickers with missing or problematic data: {problematic_tickers}")
        
        return successful_data
    
    except Exception as e:
        print(f"Error fetching bulk data: {e}")
        return {}



def calculate_indicatorsFNO(data):
    """Calculate technical indicators for each ticker's historical data."""
    indicators = {}
    for ticker, df in data.items():
        if df.empty:
            continue
        
        # Calculate MACD
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        
        # Calculate Stochastic Oscillator
        low_min = df['Low'].rolling(window=14).min()
        high_max = df['High'].rolling(window=14).max()
        k = 100 * ((df['Close'] - low_min) / (high_max - low_min))
        d = k.rolling(window=3).mean()

        # Calculate Volume Average
        volume_avg_21 = df['Volume'].rolling(window=21).mean()

        # Calculate RSI from the last 14 days of close prices
        historical_data = yf.download(ticker, period='1mo', interval='15m')  # Fetch 14 days of data
        close_prices = historical_data['Close']
        
        delta = close_prices.diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi_ma = rsi.rolling(window=14).mean()
       
        rsi_current = rsi.iloc[-1] if not rsi.empty else None
        rsi_ma_current = rsi_ma.iloc[-1] if not rsi_ma.empty else None
        
        prev_rsi = rsi.iloc[-2] if len(rsi) > 1 else None
        prev_rsi_ma = rsi_ma.iloc[-2] if len(rsi_ma) > 1 else None
         
        # Calculate Average True Range (ATR)
        true_range = pd.concat([
            df['High'] - df['Low'],
            np.abs(df['High'] - df['Close'].shift()),
            np.abs(df['Low'] - df['Close'].shift())
        ], axis=1).max(axis=1)
        atr = true_range.rolling(window=14).mean()
        
        # Detect volume spike
        volume_spike = df['Volume'].iloc[-1] > 1.5 * volume_avg_21.iloc[-1]


        # Save indicator results
        indicators[ticker] = {
            'Last Close': df['Close'].iloc[-1],
            'MACD': macd.iloc[-1],
            'MACD Signal': signal.iloc[-1],
            'Stochastic K': k.iloc[-1],
            'Stochastic D': d.iloc[-1],
            'Volume Avg 21': volume_avg_21.iloc[-1],
            'Volume Spike': volume_spike,
            'RSI': rsi_current,
            'RSI_MA' : rsi_ma_current,
            'PRV_RSI' : prev_rsi,
            'PREV_RSI_MA' : prev_rsi_ma,
            'ATR': atr.iloc[-1]
        }
    return indicators


def mainFNO():
    """Main function to execute the trading analysis process."""
    try:
        # Preparation and Data Fetching
        prepared_tickers = prepare_tickersFNO()
        total_tickers = len(prepared_tickers)
        data = fetch_historical_data_bulkFNO(prepared_tickers, interval=INTERVALS)
        indicators = calculate_indicatorsFNO(data)
        
        results = []
        true_tickers = []
        
        for i, (ticker, ind) in enumerate(indicators.items()):
            last_close = ind.get('Last Close')
            macd = ind.get('MACD')
            macd_signal = ind.get('MACD Signal')
            k = ind.get('Stochastic K')
            d = ind.get('Stochastic D')
            atr = ind.get('ATR')
            #volume_spike = data[ticker]['Volume'].iloc[-1] > 1.5 * ind.get('Volume Avg 21', 0)
            volume_spike = ind.get('Volume Spike')
            
            
            rsi1 = ind.get('RSI')
            rsi_ma1 = ind.get('RSI_MA')
            #prv_rsi1 = ind.get('PRV_RSI')
            #prv_rsi_ma = ind.get('PREV_RSI_MA')
            
            
            # Determine signal condition
            if rsi1 is not None and rsi_ma1 is not None:
                signal = "True" if (
                    rsi1 > rsi_ma1 and 
                    rsi1 > 50 and
                    macd > macd_signal and 
                    volume_spike and
                    k > d
                ) else "False"
            else:
                signal = "False"
            
            if signal == "True":
                true_tickers.append(ticker)
            
            # Append results
            results.append({
                'Ticker': ticker,
                'Last Close': round(last_close, 2) if last_close else None,
                'Signal': signal,
               
            })
            
            # Progress Update
            print(f"{i + 1} of {total_tickers} completed. Processing in the background...")
            
       
        
        # Save results to the Excel file
        results_df = pd.DataFrame(results)
        wb = xw.Book(EXCEL_FILE)
        sheet_swing = wb.sheets[SHEET_FNOSW]
        
        # Write results to the "SWING" sheet
        sheet_swing.range('N2:P600').clear_contents()
        sheet_swing.range('N2').options(index=False, header=True).value = results_df
        
        # Write "True" tickers to the "YFINCE" section in the "SWING" sheet
        sheet_swing.range('T3:Z200').clear_contents()  # Clear previous data
        if true_tickers:
            
            headers = ['Symbol', 'Close', 'TGT', 'SL', 'SYMBOL', 'Chart', 'REALL']
            sheet_swing.range('T2').value = headers
            sheet_swing.range('T3').options(transpose=False).value = [[ticker] for ticker in true_tickers]
        
        for i, ticker in enumerate(true_tickers):
            # Fetch historical data for the current true ticker
            
            single_data = fetch_historical_data_bulkFNO([ticker], interval=INTERVALS)
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'
            
            if ticker in single_data:
                last_close = single_data[ticker]['Close'].iloc[-1]
                atr = indicators[ticker].get('ATR', 0)
                link = create_tradingview_link(symbol1)
                
                # Write to Excel
                sheet_swing.range(f"U{3 + i}").value = round(last_close, 2)
                sheet_swing.range(f"V{3 + i}").value = round(last_close + 2 * atr, 2)
                sheet_swing.range(f"W{3 + i}").value = round(last_close - 1.5 * atr, 2)
                sheet_swing.range(f"X{3 + i}").value = symbol1
                sheet_swing.range(f"Y{3 + i}").value = link
                
        
        wb.save()
        print("Data saved successfully.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

def fetch_trading_dataFNO2():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_FNOSW]
        
        url = "https://scanner.tradingview.com/symbol"
        symbols = sheet.range("X3:X200").value  # Get symbols from X3 to X200
        total_symbols = len([s for s in symbols if s])  # Count non-empty symbols
        
        for index, symbol in enumerate(symbols, start=1):  # Start writing data from row 3
            if not symbol or not symbol.startswith('NSE:'):
                continue

            symbol = symbol.replace('NSE:', '').replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'
            params_15 = {"symbol": symbol1, "fields": "Recommend.All|15", "no_404": "True"}

            try:
                response_15 = requests.get(url, params=params_15)
                response_15.raise_for_status()  # Raise an error for bad responses
                data_15 = response_15.json()

                REALL = data_15.get('Recommend.All|15', 0) if isinstance(data_15, dict) else 0
                sheet.range(f"Z{2 + index}").value = [REALL]
            except requests.exceptions.RequestException as e:
                print(f"Request error for {symbol1}: {e}")
            except ValueError as ve:
                print(f"JSON decoding error for {symbol1}: {ve}")
                
            print_progress_bar(index + 1, total_symbols)
        
    except Exception as e:
        print(f"An error occurred: {e}")



#############      FNO   PUT      ############


def prepare_tickersFNOP():
    """Prepare tickers for fetching data from Excel."""
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_FNOSW]
        tickers = sheet.range("AF1:AF700").value
        prepared_tickersS = []

        for ticker in tickers:
            if ticker:
                if ticker == "NIFTY":
                    prepared_tickersS.append("^NSEI")
                elif ticker == "BANKNIFTY":
                    prepared_tickersS.append("^NSEBANK")
                elif ticker == "NAM_INDIA":
                    prepared_tickersS.append("NAM-INDIA.NS")
                elif ticker == "BAJAJ_AUTO":
                    prepared_tickersS.append("BAJAJ-AUTO.NS")  
                elif ticker == "SURANAT_P":
                    prepared_tickersS.append("SURANAT&P.NS")    
                    
                    
                else:
                    # Replace '_' with '&' in place and append '.NS'
                    ticker = ticker.replace('_', '&')
                    prepared_tickersS.append(ticker + ".NS")
                    
        return prepared_tickersS

    except Exception as e:
        print(f"Error while preparing tickers: {e}")
        return []


def mainSP():
    """Main function to execute the trading analysis process."""
    try:
        # Preparation and Data Fetching
        prepared_tickers = prepare_tickersFNOP()
        total_tickers = len(prepared_tickers)
        data = fetch_historical_data_bulkFNO(prepared_tickers, interval=INTERVALS)
        indicators = calculate_indicatorsFNO(data)
        
        results = []
        true_tickers = []
        
        for i, (ticker, ind) in enumerate(indicators.items()):
            last_close = ind.get('Last Close')
            macd = ind.get('MACD')
            macd_signal = ind.get('MACD Signal')
            k = ind.get('Stochastic K')
            d = ind.get('Stochastic D')
            atr = ind.get('ATR')
            #volume_spike = data[ticker]['Volume'].iloc[-1] > 1.5 * ind.get('Volume Avg 21', 0)
            volume_spike = ind.get('Volume Spike')
            
            
            rsi1 = ind.get('RSI')
            rsi_ma1 = ind.get('RSI_MA')
            #prv_rsi1 = ind.get('PRV_RSI')
            #prv_rsi_ma = ind.get('PREV_RSI_MA')
            
            # Determine signal condition
            if rsi1 is not None and rsi_ma1 is not None:
                signal = "True" if (
                    rsi1 < rsi_ma1 and 
                    rsi1 < 50 and
                    macd < macd_signal and 
                    volume_spike and
                    k < d
                ) else "False"
            else:
                signal = "False"
            
            if signal == "True":
                true_tickers.append(ticker)
            
            # Append results
            results.append({
                'Ticker': ticker,
                'Last Close': round(last_close, 2) if last_close else None,
                'Signal': signal,
                
            })
            
            # Progress Update
            print(f"{i + 1} of {total_tickers} completed. Processing in the background...")
            
            
        
        
        # Save results to the Excel file
        results_df = pd.DataFrame(results)
        wb = xw.Book(EXCEL_FILE)
        sheet_swing = wb.sheets[SHEET_FNOSW]
        
        # Write results to the "SWING" sheet
        sheet_swing.range('AH2:AK600').clear_contents()
        sheet_swing.range('AH2').options(index=False, header=True).value = results_df
        
        # Write "True" tickers to the "YFINCE" section in the "SWING" sheet
        sheet_swing.range('AN3:AT200').clear_contents()  # Clear previous data
        if true_tickers:
            
            headers = ['Symbol', 'Close', 'TGT', 'SL', 'SYMBOL', 'Chart', 'REALL']
            sheet_swing.range('AN2').value = headers
            sheet_swing.range('AN3').options(transpose=False).value = [[ticker] for ticker in true_tickers]
        
        for i, ticker in enumerate(true_tickers):
            # Fetch historical data for the current true ticker
            
            single_data = fetch_historical_data_bulk([ticker], interval=INTERVALS)
            symbol = ticker.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            symbol1 = f'NSE:{symbol}'
            
            if ticker in single_data:
                last_close = single_data[ticker]['Close'].iloc[-1]
                atr = indicators[ticker].get('ATR', 0)
                link = create_tradingview_link(symbol1)
                
                # Write to Excel
                sheet_swing.range(f"AO{3 + i}").value = round(last_close, 2)
                sheet_swing.range(f"AP{3 + i}").value = round(last_close + 2 * atr, 2)
                sheet_swing.range(f"AQ{3 + i}").value = round(last_close - 1.5 * atr, 2)
                sheet_swing.range(f"AR{3 + i}").value = symbol1
                sheet_swing.range(f"AS{3 + i}").value = link
                
        
        wb.save()
        print("Data saved successfully.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

############### REALL PUT

def fetch_trading_dataSWPUT():
    try:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets[SHEET_FNOSW]
        
        url = "https://scanner.tradingview.com/symbol"
        
        symbols = sheet.range("AR3:AR200").value  # Get symbols from A1 to A200
        total_symbols = len([s for s in symbols if s])  # Count non-empty symbols
        
        for index, symbol in enumerate(symbols, start=1):  # Start writing data from row 3
            if symbol is None:
                continue

            if symbol.startswith('NSE:'):
                symbol = symbol.replace('NSE:', '')
    
            symbol = symbol.replace('-', '_').replace('&', '_').replace('.NS', '').upper()
            
            symbol1 = f'NSE:{symbol}'
            
            params_15 = {
                "symbol": symbol1,
                "fields": "Recommend.All|15,",
                "no_404": "True"
            }

            response_15 = requests.get(url, params=params_15)

            if response_15.status_code == 200:
                try:
                    data_15 = response_15.json()
                except ValueError:
                    print(f"Response not in JSON format for symbol {symbol1}: {response_15.text}")
                    continue

                if not data_15 or not isinstance(data_15, dict):
                    print(f"No valid data received for {symbol1}")
                    continue

                def round_data(data):
                    if isinstance(data, dict):
                        return {k: round_data(v) for k, v in data.items()}
                    elif isinstance(data, list):
                        return [round_data(elem) for elem in data]
                    elif isinstance(data, (int, float)):
                        return round(data, 2)
                    return data

                rounded_data_15 = round_data(data_15)

                REALL = rounded_data_15.get('Recommend.All|15', 0)
             
                excel_data = [
                    REALL,
                ]

                sheet.range(f"AT{2 + index}").value = excel_data
                
                print_progress_bar(index + 1, total_symbols)
                
                
    except Exception as e:
        print(f"An error occurred: {e}")
        



#########################################

def copy_pest():
    # Open the workbook and select the sheets
    wb = xw.Book(EXCEL_FILE)
    copy_sheet = wb.sheets['FINAL']
    paste_sheet = wb.sheets['COPY']
    
   
    # Get the last row in column B that contains data
    last_row = paste_sheet.range('B' + str(paste_sheet.cells.last_cell.row)).end('up').row
    last_rowc = paste_sheet.range('W' + str(paste_sheet.cells.last_cell.row)).end('up').row
    
    # Copy the values from the FINAL sheet AS1:AV1
    values_a = copy_sheet.range('AS1:AV1').value
    values_b = copy_sheet.range('AD4:AQ6').value
    
    # Calculate the next empty row to paste the values
    next_row = last_row + 1
    next_row3 = last_rowc + 3
    
    # Get the current time in HH:MM:SS format
    current_time1 = datetime.now().strftime('%m%d%y%H%M%S')
    current_time = datetime.now().strftime('%H:%M')
    sigtime = datetime.now().strftime('%d%m-%H%M')
    
    # Paste the current time in the first column
    paste_sheet.range(f'C{next_row}').value = current_time
    paste_sheet.range(f'B{next_row}').value = current_time1
    paste_sheet.range(f'W{next_row3}').value = sigtime
    
    #range_b = paste_sheet.range('B:B')

    # Change the format of the cells in Column B to Number
    #range_b.number_format = '0'
    # Paste the values horizontally in the same row starting from column B
    paste_sheet.range(f'D{next_row}').value = values_a  # Values from AS1:AV1
    paste_sheet.range(f'X{next_row3}').value = values_b
    # Save the workbook after pasting
    wb.save()
   # wb.close()  # Close the workbook to free up resources
   
#################
def update_countdown_in_excel(workbook, remaining_time):
    """Update countdown timer in the Excel sheet."""
    sheet = workbook.sheets[COUNTDOWN_SHEET_NAME]
    sheet.range('AC1').value = f"Time remaining: {remaining_time} seconds"
  

if __name__ == "__main__":
    print("------------------------------------------------------")
    print("*** Program designed by RPSTOCKS - KATIYAR - HLD ***")
    print("For Educational Purpose Only. Trust your own research.")
    print("------------------------------------------------------")
    
    #app, wb = open_xls_and_hide_ribbon(EXCEL_FILE, SHEET_FINAL)
    
    
    
    wb = xw.Book(EXCEL_FILE)

    while True:
        wb = xw.Book(EXCEL_FILE)
        sheet = wb.sheets['SWING']
        print("-------   FATCH F&O DATA   ------")
        sheet.range('AG1').value = "-------   FATCH F&O DATA   ------"
        fetch_trading_data12()
        
        copy_pest()
        print("-------   FATCH F&O CALL INTRADAY SIGNAL   ------")
        sheet.range('AG1').value = "-------   FATCH F&O CALL INTRADAY SIGNAL   ------"
        main_scr()
        
        print("-------   FATCH F&O PUT INTRADAY SIGNAL   ------")
        sheet.range('AG1').value = "-------   FATCH F&O PUT INTRADAY SIGNAL   ------"
        main_scrPe()
        
        print("-------   FATCH F&O SWING SIGNAL   ------")
        sheet.range('AG1').value = "-------   FATCH F&O - CALL - SWING SIGNAL   ------"
        fetch_trading_dataFNO()
        mainFNO()
        fetch_trading_dataFNO2()
        
        sheet.range('AG1').value = "-------   FATCH F&O - PUT - SWING SIGNAL   ------"
        mainSP()
        fetch_trading_dataSWPUT()
        print(" ------  Wait for  SWING Stocks - 3 Minute -------- ")
        try:
            wb = xw.Book(EXCEL_FILE)
            sheet = wb.sheets[SHEET_SWING]
            
            symbols = sheet.range("A1:A1700").value  # Get symbols from A1 to A200
            total_symbols = len([s for s in symbols if s]) 
            print(f"SCAN Total symbols: {total_symbols} - FACHING IN BACKGROUND ")
            print("------------------------------------------------------")
            
            sheet.range('AG1').value = f"SCAN Total symbols: {total_symbols} - FACHING IN BACKGROUND "
            
            fetch_trading_dataS1()
            sheet.range('AG1').value = "-------   FATCH -- CASH -  SWING SIGNAL   ------"
            mainS()
            fetch_trading_dataSW()
        
            sheet.range('AG1').value = f"Sleeping for {SLEEPTIME} seconds..."
            print(f"Sleeping for {SLEEPTIME} seconds...")
            
            # Start countdown timer and update Excel
            for remaining in range(SLEEPTIME, 0, -1):
                update_countdown_in_excel(wb, remaining)
                time.sleep(1)
        
            update_countdown_in_excel(wb, "Updating data...")  # Clear message before next update
        
        except Exception as e:
            print(f"An error occurred: {e}")
            SLEEPTIME