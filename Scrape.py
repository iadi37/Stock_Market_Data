import requests
from bs4 import BeautifulSoup
import re
from collections import OrderedDict
import pandas as pd
import multiprocessing
import concurrent.futures
import time
import datetime




def scrape_stock_info(stock_url, data):

    resp = requests.get(stock_url)
    html = resp.content
    soup = BeautifulSoup(html, 'html.parser')

    fundamentals = soup.find_all('div', {'class': 'column col-2 col-md-4 col-sm-6'})
    price = fundamentals[0].get_text().strip().strip('Price (delayed)').strip()
    mkt_cap = fundamentals[1].get_text().strip().split('Market cap')[1].strip()
    pe_ratio = fundamentals[2].get_text().strip().strip('P/E Ratio').strip()

    if ',' in pe_ratio:
        pe_ratio = float(pe_ratio.replace(',',''))

    if pe_ratio != 'N/A':
        pe_ratio = float(pe_ratio)



    div = fundamentals[3].get_text().strip().strip('Dividend/share').strip()
    roic_all = soup.find_all('div', {'class': 'card-body p-1'})
    
    for r in roic_all:
        if 'Return on invested capital' in r.get_text():
            roic = r.find('div', {'class': 'float-right key-stat'}).get_text().strip()
            break

    for r in roic_all:
        if 'Debt to equity' in r.get_text():
            debt = r.find('div', {'class': 'float-right key-stat'}).get_text().strip()

            if debt != 'N/A':
                debt = float(debt)

            break

    data['Price'] = price
    data['Market Cap'] = mkt_cap
    data['PE Ratio'] = pe_ratio
    data['Dividend'] = div
    data['ROIC'] = roic
    data['Debt to Equity'] = debt

    return data

def get_sector_links(sectors_data, sector_pages):

    for sector, sector_link in sectors_data.items():
        print(f'Getting all Pages for Sector - {sector_link}')
        resp = requests.get(sector_link)
        html = resp.content
        soup = BeautifulSoup(html, 'html.parser')
        time.sleep(30)
        uls = soup.find('ul', {"class": "pagination"})
        if uls:
            num_pages = int(uls.find_all('li')[-2].get_text())
        else:
            # No pages
            num_pages = 1
        
        stock_pages = []
        for n in range(1, num_pages + 1):
            l = sector_link + f"?page={n}"
            stock_pages.append(l)

        sector_pages[sector] =  stock_pages

    return sector_pages



def get_all_data(sector_pages):

    final_data = []

    for sector, pages in sector_pages.items():
        print(f'Processing Sector - {sector}')
        for page in pages:
            print(f'\tProcessing Sector Page - {page}')
            resp = requests.get(page)
            html = resp.content
            soup = BeautifulSoup(html, 'html.parser')
            stock_table = soup.find('table')
            stock_body  = stock_table.find('tbody')
            stocks = stock_body.find_all('tr')
            
            for stock in stocks:
                infos = stock.find_all('td')
                ticker = infos[0].find('span').text
                ticker_el = infos[0].find('span')
                company_name = ticker_el.next_sibling .strip()
                print(f'\t\tProcessing Stock - {company_name}')
                exchange = infos[1].get_text().strip()
                stock_info = {'Company': company_name,
                            'Sector': sector, 'Ticker': ticker,
                            'Exchange': exchange}

                stock_url = "https://fullratio.com/" + infos[0].find('a')['href']
                data = scrape_stock_info(stock_url, stock_info)

                final_data.append(data)

    return final_data




if __name__ == "__main__":

    # scrape_stock_info('https://fullratio.com/stocks/nasdaq-gevo/gevo', {})

    # quit()

    main_url = "https://fullratio.com/stocks"

    resp = requests.get(main_url)
    html = resp.content
    soup = BeautifulSoup(html, 'html.parser')

    sectors = soup.find_all('div', {"class": "card-body"})


    sectors_data = {}

    for sector in sectors:
        sector_name = sector.get_text().strip()
        sector_link = "https://fullratio.com/" + sector.find('a')['href']
        sectors_data[sector_name] = sector_link

    print(sectors_data)

    # Get sector and its pages -> {sector1: [page1, page2], sector: [page1, page2, page3]}
    sector_pages = get_sector_links(sectors_data, {})

    final_data = get_all_data(sector_pages)

    df = pd.DataFrame(final_data)

    df.to_csv('stock_data.csv', index = False)



