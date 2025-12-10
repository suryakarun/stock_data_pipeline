import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import time
import logging
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataFetcher:
    """
    A class to fetch stock data from Alpha Vantage API and store it in PostgreSQL.
    """
    
    def __init__(self):
        """Initialize the StockDataFetcher with environment variables."""
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'stock_data'),
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
        }
        self.base_url = "https://www.alphavantage.co/query"
        
        # Validate configuration
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set")
    
    def get_db_connection(self):
        """
        Establish a connection to PostgreSQL database.
        
        Returns:
            psycopg2.connection: Database connection object
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            logger.info("Successfully connected to PostgreSQL database")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def fetch_stock_data(self, symbol: str) -> Optional[Dict]:
        """
        Fetch stock data for a given symbol from Alpha Vantage API.
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL')
        
        Returns:
            Optional[Dict]: Parsed stock data or None if request fails
        """
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': '60min',
            'apikey': self.api_key,
            'outputsize': 'compact'
        }
        
        try:
            logger.info(f"Fetching data for {symbol}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                logger.error(f"API Error for {symbol}: {data['Error Message']}")
                return None
            
            if "Note" in data:
                logger.warning(f"API Rate Limit for {symbol}: {data['Note']}")
                return None
            
            if "Time Series (60min)" not in data:
                logger.warning(f"No time series data found for {symbol}")
                return None
            
            logger.info(f"Successfully fetched data for {symbol}")
            return {
                'symbol': symbol,
                'data': data['Time Series (60min)']
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Error parsing JSON response for {symbol}: {e}")
            return None
    
    def parse_stock_data(self, stock_data: Dict) -> List[tuple]:
        """
        Parse stock data into a list of tuples for database insertion.
        
        Args:
            stock_data (Dict): Raw stock data from API
        
        Returns:
            List[tuple]: List of tuples containing parsed stock data
        """
        parsed_data = []
        symbol = stock_data['symbol']
        time_series = stock_data['data']
        
        try:
            for timestamp, values in time_series.items():
                # Parse timestamp
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                
                # Extract values with error handling
                open_price = float(values.get('1. open', 0))
                high_price = float(values.get('2. high', 0))
                low_price = float(values.get('3. low', 0))
                close_price = float(values.get('4. close', 0))
                volume = int(values.get('5. volume', 0))
                
                parsed_data.append((
                    symbol,
                    dt,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume
                ))
            
            logger.info(f"Parsed {len(parsed_data)} records for {symbol}")
            return parsed_data
            
        except (ValueError, KeyError) as e:
            logger.error(f"Error parsing data for {symbol}: {e}")
            return []
    
    def store_stock_data(self, parsed_data: List[tuple]) -> int:
        """
        Store parsed stock data in PostgreSQL database.
        
        Args:
            parsed_data (List[tuple]): List of tuples containing stock data
        
        Returns:
            int: Number of rows inserted/updated
        """
        if not parsed_data:
            logger.warning("No data to store")
            return 0
        
        conn = None
        cursor = None
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Insert or update data using ON CONFLICT
            insert_query = """
                INSERT INTO stock_prices 
                (symbol, timestamp, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, timestamp) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            execute_values(cursor, insert_query, parsed_data)
            conn.commit()
            
            rows_affected = cursor.rowcount
            logger.info(f"Successfully inserted/updated {rows_affected} rows")
            
            return rows_affected
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def process_symbol(self, symbol: str) -> bool:
        """
        Process a single stock symbol: fetch, parse, and store data.
        
        Args:
            symbol (str): Stock symbol to process
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Fetch data
            stock_data = self.fetch_stock_data(symbol)
            if not stock_data:
                logger.warning(f"No data fetched for {symbol}")
                return False
            
            # Parse data
            parsed_data = self.parse_stock_data(stock_data)
            if not parsed_data:
                logger.warning(f"No data parsed for {symbol}")
                return False
            
            # Store data
            rows_affected = self.store_stock_data(parsed_data)
            
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return False
    
    def process_multiple_symbols(self, symbols: List[str]) -> Dict[str, bool]:
        """
        Process multiple stock symbols with rate limiting.
        
        Args:
            symbols (List[str]): List of stock symbols
        
        Returns:
            Dict[str, bool]: Dictionary mapping symbols to success status
        """
        results = {}
        
        for i, symbol in enumerate(symbols):
            logger.info(f"Processing symbol {i+1}/{len(symbols)}: {symbol}")
            results[symbol] = self.process_symbol(symbol)
            
            # Rate limiting: Alpha Vantage free tier allows 5 API calls per minute
            if i < len(symbols) - 1:
                logger.info("Waiting 12 seconds before next API call (rate limiting)")
                time.sleep(12)
        
        return results


def main():
    """
    Main function to fetch and store stock data for configured symbols.
    """
    # Get stock symbols from environment variable
    symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT')
    symbols = [s.strip() for s in symbols_str.split(',')]
    
    logger.info(f"Starting stock data pipeline for symbols: {symbols}")
    
    try:
        fetcher = StockDataFetcher()
        results = fetcher.process_multiple_symbols(symbols)
        
        # Log results
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Pipeline completed: {success_count}/{len(symbols)} symbols processed successfully")
        
        for symbol, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"{symbol}: {status}")
        
        # Exit with error code if any symbol failed
        if success_count < len(symbols):
            exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in pipeline: {e}")
        exit(1)


if __name__ == "__main__":
    main()