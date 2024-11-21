import requests
import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timedelta
import urllib.parse
from time import sleep
import os
import logging
# Configure logging
logging.basicConfig(
    filename="sector_scanner.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# Utility function to fetch data from NSE API
def fetch_with_retry(url, cookies=None, retries=5, delay=1):
    homepage_url = "https://www.nseindia.com/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0",
        "Referer": homepage_url,
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    if not cookies:
        homepage_response = requests.get(homepage_url, headers=headers)
        if homepage_response.status_code == 200:
            cookies = homepage_response.cookies
        else:
            logging.error("Failed to fetch cookies from homepage.")
            raise Exception("Error receiving cookies from homepage.")

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, cookies=cookies)
            if response.status_code == 200:
                return response.json()
            else:
                logging.warning(f"Attempt {attempt + 1}: HTTP {response.status_code}")
                sleep(delay * (2 ** attempt))
        except requests.exceptions.JSONDecodeError:
            with open("raw_response.html", "wb") as f:
                f.write(response.content)
            logging.error(f"JSONDecodeError: Saved raw response to 'raw_response.html'. URL: {url}")
        except Exception as e:
            logging.error(f"Error fetching data: {e}. URL: {url}")
        sleep(delay * (2 ** attempt))
    raise Exception("All retries failed.")

 
# Fetch sector names
def get_sector_names():
    index_res = fetch_with_retry("https://www.nseindia.com/api/equity-master")
    if index_res is None:
        print("Failed to fetch sector data.")
        return []
    
    sector_names = []
    for _, sector in index_res.items():
        sector_names.extend(sector)
    return sector_names[:-2]  # Assuming last two entries need to be excluded


# Fetch stock data for each sector and create a DataFrame
def fetch_sector_data(sector_names):
    url_list = {sector: f'https://www.nseindia.com/api/equity-stockIndices?index={urllib.parse.quote(sector)}' for sector in sector_names}
    sector_df_list = []
    
    for sector, url in url_list.items():
        try:
            sector_data = fetch_with_retry(url)
            if sector_data is None:
                print(f"Failed to fetch data for sector: {sector}")
                continue

            sector_df = pd.json_normalize(sector_data['data'])
            sector_df = sector_df[sector_df.priority != 1]  # Filter priority stocks
            sector_df = sector_df[["symbol", "series", "lastPrice", "meta.industry", "meta.isin"]]
            sector_df.columns = ["symbol", "series", "lastPrice", "industry", "isin"]
            sector_df['indexSector'] = sector
            sector_df_list.append(sector_df)
        except Exception as e:
            print(f"Error fetching data for {sector}: {e}")
    
    return pd.concat(sector_df_list, ignore_index=True)


# Fetch master stock data from Upstox
def fetch_master_stock_data():
    file_url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    master_df = pd.read_csv(file_url)
    return master_df[(master_df.exchange == 'NSE_EQ') & (master_df.lot_size == 1) & (master_df.last_price > 0)]


# Filter stocks based on sector symbols
def filter_stocks_by_sector(master_df, sector_df):
    filtered_stocks = master_df[master_df.tradingsymbol.isin(sector_df.symbol.tolist())]
    return filtered_stocks.drop_duplicates(subset='tradingsymbol', keep=False).reset_index(drop=True)


# Async function to fetch historical stock data
async def get_historical_data(symbol, instrument_key):
    for _ in range(2):
        try:
            instrument = urllib.parse.quote(instrument_key)
            today = datetime.now()
            to_date = datetime.strftime(today, '%Y-%m-%d')
            from_date = datetime.strftime(today - timedelta(days=100), '%Y-%m-%d')
            url = f'https://api.upstox.com/v2/historical-candle/{instrument}/day/{to_date}/{from_date}'

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={'accept': 'application/json'}) as response:
                    candle_res = await response.json()
                    candle_data = pd.DataFrame(candle_res['data']['candles'])
                    candle_data.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
                    candle_data = candle_data[['date', 'open', 'high', 'low', 'close']]
                    candle_data['date'] = pd.to_datetime(candle_data['date']).dt.tz_convert('Asia/Kolkata').dt.date
                    candle_data.sort_values(by='date', inplace=True)
                    print(f"Done fetching data for {symbol}, {len(candle_data)} records")
                    return symbol, candle_data
        except Exception as e:
            print(f"Error fetching historical data for {symbol}: {e}")
            sleep(2)
    return None, None


# Calculate relative strength index (RSI)
def calculate_relative_strength(base_close, comparative_close, length):
    base_relative = base_close / base_close.shift(length)
    comparative_relative = comparative_close / comparative_close.shift(length)
    relative_strength = base_relative / comparative_relative - 1
    return relative_strength.round(2)


# Fetch relative strength data for each stock
def fetch_relative_strength_data(sym_ohlc_map, index_data):
    sym_rs_map = {}
    for sym, ohlc_data in sym_ohlc_map.items():
        try:
            stock_data = ohlc_data[['date', 'close']]
            merged_data = stock_data.merge(index_data[['date', 'close']], on='date', how='left', suffixes=("", "_index"))
            merged_data['RS_INDEX'] = calculate_relative_strength(merged_data['close'], merged_data['close_index'], length=50)
            recent_data = merged_data.iloc[-1]
            recent_data['sym'] = sym
            sym_rs_map[sym] = recent_data.to_dict()
        except Exception as e:
            print(f"Error calculating RSI for {sym}: {e}")
    
    return sym_rs_map


async def main():
    sector_names = get_sector_names()
    sector_df = fetch_sector_data(sector_names)
    master_df = fetch_master_stock_data()
    filtered_stocks = filter_stocks_by_sector(master_df, sector_df)

    # Create batches for asynchronous fetching of historical data
    result = [filtered_stocks.index[i:i + 100] for i in range(0, len(filtered_stocks), 100)]
    sym_ohlc_list = []

    for sublist in result:
        coros = []
        for i in sublist:
            stock_info = filtered_stocks.loc[i]
            coros.append(get_historical_data(stock_info['tradingsymbol'], stock_info['instrument_key']))
        sym_ohlc_list.extend(await asyncio.gather(*coros))
        print("------- ")
        sleep(2)

    # Process historical data
    sym_ohlc_map = {key: value for key, value in sym_ohlc_list if value is not None}

    # Fetch index data for Nifty 500
    index_name, index_data = await get_historical_data('Nifty50', 'NSE_INDEX|Nifty 50')

    # Calculate relative strength for each stock
    sym_rs_map = fetch_relative_strength_data(sym_ohlc_map, index_data)

    # Initialize INDEX_STOCK_MAP
    INDEX_STOCK_MAP = {}

    # Rank sectors based on relative strength
    index_grouped = sector_df.groupby('indexSector')
    index_rank_list = []

    for index_name, index_stock in index_grouped:
        print(f"Processing sector: {index_name} with {len(index_stock)} stocks.")
        stock_data = []
        positive_rs_data = []
        negative_rs_data = []
        
        for sym in index_stock.symbol:
            if sym in sym_rs_map:
                stock_data.append(sym_rs_map[sym])
                
                # Separate positive and negative RS data
                if sym_rs_map[sym]['RS_INDEX'] > 0:
                    positive_rs_data.append(sym_rs_map[sym]['RS_INDEX'])
                else:
                    negative_rs_data.append(sym_rs_map[sym]['RS_INDEX'])
        
        # If there is any stock data, create the DataFrame and add to INDEX_STOCK_MAP
        if stock_data:
            stock_data_df = pd.DataFrame(stock_data).round(2)
            INDEX_STOCK_MAP[index_name] = stock_data_df
            
            # Calculate average RS of positive and negative RS stocks
            avg_positive_rs = (sum(positive_rs_data) / len(positive_rs_data)) if positive_rs_data else 0
            avg_negative_rs = (sum(negative_rs_data) / len(negative_rs_data)) if negative_rs_data else 0
            
            positive_rs = len(positive_rs_data)
            negative_rs = len(negative_rs_data)
            ratio = positive_rs / len(stock_data_df)
            avg_rs = stock_data_df['RS_INDEX'].mean()
            
            index_rank_list.append({
                'indexName': index_name, 
                'positiveRS': positive_rs, 
                'negativeRS': negative_rs, 
                'avgPositiveRS': avg_positive_rs,  # avg RS of positive RS stocks
                'avgNegativeRS': avg_negative_rs,  # avg RS of negative RS stocks
                'ratio': ratio, 
                'avgRS': avg_rs
            })

    # Rank sectors by relative strength
    index_rank_df = pd.DataFrame(index_rank_list).round(2)
    index_rank_df['rank'] = (index_rank_df['ratio'] * index_rank_df['avgPositiveRS'])+((1-index_rank_df['ratio']) * index_rank_df['avgNegativeRS'])
    index_rank_df = index_rank_df.sort_values(by='rank', ascending=False).reset_index(drop=True)

    # Save sector rankings to a CSV file
    index_rank_df.to_csv("sector_rankings.csv", index=False)

    # Save stock-level relative strength data
    pd.concat([df.assign(sector=index_name) for index_name, df in INDEX_STOCK_MAP.items()], ignore_index=True).to_csv("stockRS.csv", index=False)


# Run the script
if __name__ == "__main__":
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)
    
    asyncio.run(main())

