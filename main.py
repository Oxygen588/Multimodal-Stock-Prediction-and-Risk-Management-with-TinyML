import yfinance as yf
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

db = sqlite3.connect("fintime.db", check_same_thread=False)
cursor = db.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS symbolsDone (symbol TEXT PRIMARY KEY)")
cursor.execute("CREATE TABLE IF NOT EXISTS stockData (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, date TEXT, dataJSON TEXT)")
db.commit()

def convert_to_number(value):
    multipliers = {'B': 1_000_000_000, 'M': 1_000_000, 'T': 1_000_000_000_000, 'K': 1_000}
    if value == '--':
        return None
    try:
        if '%' in value:
            return float(value.replace(',', '').replace('%', '')) / 100
        if value[-1] in multipliers:
            return float(value[:-1].replace(',', '')) * multipliers[value[-1]]
        return float(value.replace(',', ''))
    except ValueError:
        return value

def print_statistics_nicely(headers, rows):
    statistics = {}
    dates = headers[1:]
    for date in dates:
        statistics[date] = {}
    for row in rows:
        label = row[0]
        for date, value in zip(dates, row[1:]):
            statistics[date][label] = convert_to_number(value)
    return statistics

def get_ibm_key_statistics(stock_ticker):
    url = f"https://finance.yahoo.com/quote/{stock_ticker}/key-statistics"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve page: {response.status_code}")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    stat_section = soup.find("section", {"data-testid": "qsp-statistics"})
    if not stat_section:
        print("No statistics section found.")
        return None
    table = stat_section.find("table", class_="table yf-kbx2lo")
    if not table:
        print("No table found.")
        return None
    header_cells = table.find("thead").find_all("th")
    headers = [cell.get_text(strip=True) for cell in header_cells]
    all_row_data = []
    body_rows = table.find("tbody").find_all("tr")
    for row in body_rows:
        cols = row.find_all("td")
        row_data = [col.get_text(strip=True) for col in cols]
        all_row_data.append(row_data)
    main_stats = print_statistics_nicely(headers, all_row_data)
    return main_stats

def get_comp_desc(stock_ticker):
    url = f"https://finance.yahoo.com/quote/{stock_ticker}/profile"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve page: {response.status_code}")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    stat_section = soup.find("section", {"data-testid": "description"})
    if not stat_section:
        print("No description section found.")
        return None
    desc_tag = stat_section.find("p")
    if desc_tag:
        return desc_tag.get_text(strip=True)
    else:
        print("No description found.")
        return None

def get_historical_stock_details(ticker, date):
    stock = yf.Ticker(ticker)
    hist = stock.history(start=(datetime.strptime(date, "%m/%d/%Y") - timedelta(days=85)),
                         end=(datetime.strptime(date, "%m/%d/%Y") + timedelta(days=85)))
    if hist.empty:
        return None
    hist.reset_index(inplace=True)
    hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
    return hist

def get_price(ticker, date):
    stock = yf.Ticker(ticker)
    hist = stock.history(start=datetime.strptime(date, "%m/%d/%Y"),
                         end=(datetime.strptime(date, "%m/%d/%Y") + timedelta(days=3)))
    if hist.empty:
        print(f"No data available for {date}")
        return None
    return (hist['High'].max() + hist['Low'].min()) / 2

def process_stock(stock_ticker):
    company_desc = get_comp_desc(stock_ticker)
    stock_datas = get_ibm_key_statistics(stock_ticker)
    if not stock_datas:
        print(f"Skipping {stock_ticker} due to missing statistics.")
        return
    for date, stats in stock_datas.items():
        if date in ["extra_stats", "Current"]:
            continue
        hist = get_historical_stock_details(stock_ticker, date)
        if hist is None:
            print(f"No historical data for {stock_ticker} on {date}")
            return
        stock_data = {
            "hist": hist.to_dict(orient="records"),
            "current_price": get_price(stock_ticker, date),
            "stats": stats,
            "company_description": company_desc
        }
        cursor.execute("INSERT INTO stockData (ticker, date, dataJSON) VALUES (?, ?, ?)",
                       (stock_ticker, date, json.dumps(stock_data)))
        db.commit()
    cursor.execute("INSERT OR IGNORE INTO symbolsDone (symbol) VALUES (?)", (stock_ticker,))
    db.commit()


stock_tickers = ["IBM", "MAIN"]
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(process_stock, stock_tickers)

