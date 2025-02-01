import yfinance as yf
from datetime import datetime, timedelta
from groq import Groq
import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import json
import redis
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import threading


db = sqlite3.connect("fintime.db",check_same_thread=False)
cursor = db.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS symbolsDone (
        symbol TEXT PRIMARY KEY
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stockData (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        date TEXT,
        dataJSON TEXT
    )
""")
db.commit()

def load_symbols_done():
    cursor.execute("SELECT symbol FROM symbolsDone")
    rows = cursor.fetchall()
    return [r[0] for r in rows]

def load_data():
    cursor.execute("SELECT dataJSON FROM stockData")
    rows = cursor.fetchall()
    return [json.loads(r[0]) for r in rows]

symbolsDone = load_symbols_done()
data_lock = threading.Lock()
skipStocks = []

client = Groq(
    api_key="",
)

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
print("Driver created")

def convert_to_number(value):
    multipliers = {'B': 1_000_000_000, 'M': 1_000_000, 'T': 1_000_000_000_000, 'K': 1_000, 'k': 1_000}
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

def get_key_statistics(stock_ticker):
    url = "https://finance.yahoo.com/quote/" + stock_ticker + "/key-statistics"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve page: {response.status_code}")
        raise Exception("Failed to retrieve page")
    soup = BeautifulSoup(response.text, 'html.parser')
    stat_section = soup.find("section", {"data-testid": "qsp-statistics"})
    if not stat_section:
        raise Exception("Failed to retrieve page")
        return
    table = stat_section.find("table", class_="table yf-kbx2lo")
    if not table:
        raise Exception("Failed to retrieve page")
        return None
    header_cells = table.find("thead").find_all("th")
    headers = [cell.get_text(strip=True) for cell in header_cells]
    print("Headers:", headers)
    all_row_data = []
    body_rows = table.find("tbody").find_all("tr")
    for row in body_rows:
        cols = row.find_all("td")
        row_data = [col.get_text(strip=True) for col in cols]
        all_row_data.append(row_data)
        print("Row data:", row_data)
    main_stats = print_statistics_nicely(headers, all_row_data)

    extra_stats = {}
    for card_section in soup.find_all("section", class_="card small tw-p-0 yf-6zl6fb sticky noBackGround"):
        header_tag = card_section.find("h3", class_="title")
        section_title = header_tag.get_text(strip=True) if header_tag else "Unknown Section"
        extra_stats[section_title] = {}
        table_tag = card_section.find("table", class_="table yf-vaowmx")
        if not table_tag:
            continue
        for row in table_tag.find_all("tr"):
            cols = row.find_all("td", class_="value yf-vaowmx")
            labels = row.find_all("td", class_="label yf-vaowmx")
            if len(cols) == 1 and len(labels) == 1:
                label_text = labels[0].get_text(strip=True)
                value_text = cols[0].get_text(strip=True)
                extra_stats[section_title][label_text] = value_text

    main_stats["extra_stats"] = extra_stats
    return main_stats

def get_comp_desc(c):
    url = "https://finance.yahoo.com/quote/"+c+"/profile"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve page: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    stat_section = soup.find("section", {"data-testid": "description"})
    if not stat_section:
        print("No statistics section found.")
        return
    desc_tag = stat_section.find("p")
    #return desc_tag.get_text(strip=True) this is for getting the original description
    if desc_tag:
        completion = client.chat.completions.create(
            messages=[
                {"role": "system","content": """Respond in the following JSON format:{"description":"company description"}"""},
                {"role": "user","content": "Based on the description of this company re-write it, standardise it and remove all identifieable fields:\n"+desc_tag.get_text(strip=True)}
            ],
            model="llama3-70b-8192",
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        completion_content = completion.choices[0].message.content
        try:
            parsed_json = json.loads(completion_content)
            return parsed_json['description']
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
    else:
        print("No description found.")

def get_historical_stock_details(ticker, date):
    stock = yf.Ticker(ticker)
    hist = stock.history(
        start=(datetime.strptime(date, "%m/%d/%Y") - timedelta(days=85)),
        end=(datetime.strptime(date, "%m/%d/%Y") + timedelta(days=85))
    )
    if hist.empty:
        return None
    hist.reset_index(inplace=True)
    hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
    return hist

def get_price(ticker, date):
    stock = yf.Ticker(ticker)
    hist = stock.history(
        start=datetime.strptime(date, "%m/%d/%Y"),
        end=(datetime.strptime(date, "%m/%d/%Y") + timedelta(days=3))
    )
    if hist.empty:
        print(f"No data available for {date}")
        return
    highest_price = hist['High'].max()
    lowest_price = hist['Low'].min()
    return (highest_price+lowest_price)/2

with open('./symbols.txt') as f:
    stock_tickers = [line.strip() for line in f if line.strip()]

symbols_done_lock = threading.Lock()

def process_stock(stock_ticker):
    for attempt in range(3):
        try:
            if stock_ticker in symbolsDone or stock_ticker in skipStocks:
                return

            company_desc = get_comp_desc(stock_ticker)
            stock_datas = get_key_statistics(stock_ticker)
            if not stock_datas:
                skipStocks.append(stock_ticker)
                return
            for date, stats in stock_datas.items():
                if date == "extra_stats":
                    continue
                if date == "Current":
                    continue
                hist = get_historical_stock_details(stock_ticker, date)
                if hist is None:
                    skipStocks.append(stock_ticker)
                    return
                print(f"Statistics for {date}:")
                stock_data = {}
                stock_data["hist"] = hist.to_dict(orient="records")
                stock_data["current_price"] = get_price(stock_ticker, date)
                for label, value in stats.items():
                    stock_data[label] = value
                stock_data["company_description"] = company_desc
                with data_lock:
                    for section in stock_datas["extra_stats"]:
                        for lbl, val in stock_datas["extra_stats"][section].items():
                            if val is None:
                                stock_datas["extra_stats"][section][lbl] = -1
                                continue
                            if isinstance(val, (float, int)):
                                continue
                            stock_datas["extra_stats"][section][lbl] = convert_to_number(val)
                    stock_data["extra"] = stock_datas["extra_stats"]
                    cursor.execute(
                        "INSERT INTO stockData (ticker, date, dataJSON) VALUES (?, ?, ?)",
                        (stock_ticker, date, json.dumps(stock_data))
                    )
                    db.commit()
            with symbols_done_lock:
                # Store in DB instead of symbols.json
                cursor.execute("INSERT OR IGNORE INTO symbolsDone(symbol) VALUES (?)", (stock_ticker,))
                db.commit()
                symbolsDone.append(stock_ticker)
            break
        except Exception as e:
            if attempt < 2:
                print(f"Error processing {stock_ticker}: {e}. Retrying in 15 seconds...")
                time.sleep(320)
            else:
                print(f"Failed to process {stock_ticker} after 3 attempts.")
                raise e

lock = threading.Lock()
with open("company_descriptions.json") as f:
    company_descriptions = json.load(f)

def process_stock_desc(stock_ticker):
    if stock_ticker in company_descriptions:
        return
    for attempt in range(3):
        try:
            desc = get_comp_desc(stock_ticker)
            print(desc)
            if desc is None:
                exception = Exception("Failed to retrieve description")
                raise exception
            with lock:
                company_descriptions[stock_ticker] = desc
                with open("company_descriptions.json", "w") as f:
                    json.dump(company_descriptions, f)
            return
        except Exception as e:
            time.sleep(15)
            print(f"Error processing {stock_ticker}: {e}")

with ThreadPoolExecutor(max_workers=6) as executor:
    executor.map(process_stock_desc, stock_tickers)


all_data = load_data()
print(all_data)

driver.quit()