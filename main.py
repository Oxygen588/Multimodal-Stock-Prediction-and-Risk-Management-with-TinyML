import yfinance as yf
import requests
from bs4 import BeautifulSoup
import sqlite3

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


stock_ticker = "MAIN"

stats = get_ibm_key_statistics("MAIN")
print(stats)
desc = get_comp_desc("MAIN")
print(desc)