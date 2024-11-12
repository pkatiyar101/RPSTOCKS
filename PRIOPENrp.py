import os
import pandas as pd
import json
import xlwings as xw
from datetime import datetime
import requests
from requests import Session

# Set up headers and session for cookie management
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'application/json',
    'Referer': 'https://www.nseindia.com/',
}

session = Session()

def fetch_nse_data(url):
    """Fetch data from NSE with automatic cookie handling."""
    try:
        # Fetch NSE homepage to get cookies
        session.get('https://www.nseindia.com', headers=HEADERS)
        # Now make the API request with the cookies
        response = session.get(url, headers=HEADERS)
        response.raise_for_status()  # Check if the request was successful
        return response.json()  # Return the JSON payload
    except requests.exceptions.HTTPError as err:
        print(f"Error: Status Code {err.response.status_code}")
        return None

def all_priopen():
    """Fetch NSE pre-open market data, filter it, and save it to an Excel file."""
    EXCEL_FILE = 'YFin.xls'
    JSON_FILE = f"ALL_{datetime.today().strftime('%Y-%m-%d')}.json"
    url = 'https://www.nseindia.com/api/market-data-pre-open?key=ALL'
    
    # Fetch data
    payload = fetch_nse_data(url)
    
    if payload is None:
        print("Failed to fetch data from NSE.")
        return
    
    # Convert the response to a DataFrame
    data = pd.DataFrame(payload['data'])
    
    # Save data to JSON file
    with open(JSON_FILE, 'w') as json_file:
        json.dump(data.to_dict(orient='records'), json_file, indent=4)
    print(f'Data Fetched Date {JSON_FILE}')
    
    # Initialize lists to hold filtered data
    results = []
    results1 = []
    true_tickers = []
    
    # Filter the data based on the conditions
    for item in payload['data']:
        if 'metadata' in item and 'detail' in item:
            symbol = item['metadata'].get('symbol')
            results.append({'Symbol': symbol})
        
            metadata = item['metadata']
            detail = item['detail']['preOpenMarket']
             
            last_price = metadata.get('lastPrice')
            ch_price = metadata.get('pChange')
            total_sell_quantity = detail['totalSellQuantity']
            total_buy_quantity = detail['totalBuyQuantity']
            
            lastUpdateTime = detail['lastUpdateTime']
            # Define the signalCE logic
            #if (ch_price > 0 and total_buy_quantity > total_sell_quantity):
                #signalCE = "True"
            if (ch_price > -0.10 and total_buy_quantity > total_sell_quantity) :
                signalCE = "True"
                true_tickers.append(symbol)
            else:
                signalCE = "False"
             
            # Append the results
            results1.append({
                'Symbol': symbol,
                'Last Price': last_price,
                'Signal': signalCE
            })
     
    # Convert the results into DataFrames
    final_result1 = pd.DataFrame(true_tickers)
    final_result = pd.DataFrame(results)
    
    # Write the final result to the Excel sheet
    wb = xw.Book(EXCEL_FILE)
    sheet_swing = wb.sheets['LIST']
    sheet_swing1 = wb.sheets['SY_SHEET']
    
    # Clear existing contents and write new data
    sheet_swing.range('A3:B2000').clear_contents()
    sheet_swing.range('B3').value = final_result.values
    sheet_swing.range('A3').value = final_result1.values
    sheet_swing1.range('A3').value = final_result1.values
    
    if lastUpdateTime:
        sheet_swing1.range('B2').value = f"CASH Data - {lastUpdateTime}"
        
    print("CASH SYMBOL written in sheet successfully.")
    print(f"Pri Open Market Data -  {lastUpdateTime}")
    
    wb.save()

def FO_priopen1():
    """Fetch NSE pre-open market data, filter it, and save it to an Excel file."""
    EXCEL_FILE = 'YFin.xls'
    JSON_FILE = f"FO_{datetime.today().strftime('%Y-%m-%d')}.json"
    url = 'https://www.nseindia.com/api/market-data-pre-open?key=FO'
    
    # Fetch data
    payload = fetch_nse_data(url)
    
    if payload is None:
        print("Failed to fetch data from NSE.")
        return
    
    # Convert the response to a DataFrame
    data = pd.DataFrame(payload['data'])
    
    # Save data to JSON file
    with open(JSON_FILE, 'w') as json_file:
        json.dump(data.to_dict(orient='records'), json_file, indent=4)
    print(f'Data Fetched Date {JSON_FILE}')
    
    # Initialize lists to hold filtered data
    results = []
    
    
    # Filter the data based on the conditions
    for item in payload['data']:
        if 'metadata' in item and 'detail' in item:
            detail = item['detail']['preOpenMarket']
            lastUpdateTime = detail['lastUpdateTime']
            
            
            symbol = item['metadata'].get('symbol')
          
            results.append({'Symbol': symbol})
        
            
    # Convert the results into DataFrames
    final_result = pd.DataFrame(results)
    
    # Write the final result to the Excel sheet
    wb = xw.Book(EXCEL_FILE)
    sheet_FO = wb.sheets['SHEET_PCR']
    sheet_FOSW = wb.sheets['FNOSW']
    sheet_FO1 = wb.sheets['YFIN1']
    
    # Clear existing contents and write new data
    sheet_FO.range('A4:A500').clear_contents()
    sheet_FO.range('A4').value = final_result.values
    
    sheet_FOSW.range('A3:A500').clear_contents()
    sheet_FOSW.range('A3').value = final_result.values
    
    sheet_FO1.range('A4:A500').clear_contents()
    sheet_FO1.range('A4').value = final_result.values
    
    if lastUpdateTime:
        sheet_FO1.range('B1').value = f"FNO Symbol - {lastUpdateTime}"
        
    print("F & O SYMBOL written in sheet successfully.")
    print(f"FNO Symbol Data -  {lastUpdateTime}")
    wb.save()


def delete_j():
    """Delete JSON files generated for today's data."""
    json_files = [
        f"ALL_{datetime.today().strftime('%Y-%m-%d')}.json",
        f"FO_{datetime.today().strftime('%Y-%m-%d')}.json",
    ]

    for json_file in json_files:
        try:
            os.remove(json_file)
            print(f"Deleted: {json_file}")
        except FileNotFoundError:
            print(f"File not found: {json_file}")

if __name__ == "__main__":
    try:
        all_priopen()
        
    except Exception as e:
        print(f"Error in all_priopen: {e}")
    
    try:
        FO_priopen1()
    except Exception as e:
        print(f"Error in FO_priopen: {e}")
    
    delete_j()
