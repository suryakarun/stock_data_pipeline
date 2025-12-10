from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from fetch_stock_data import StockDataFetcher

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch and store stock market data from Alpha Vantage API',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['stock', 'data-pipeline', 'etl'],
)


def check_database_connection():
    """Task to verify database connection."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        fetcher = StockDataFetcher()
        conn = fetcher.get_db_connection()
        conn.close()
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def fetch_and_store_stock_data():
    """Task to fetch and store stock data."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Get stock symbols from environment
    symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT')
    symbols = [s.strip() for s in symbols_str.split(',')]
    
    logger.info(f"Processing symbols: {symbols}")
    
    try:
        fetcher = StockDataFetcher()
        results = fetcher.process_multiple_symbols(symbols)
        
        # Check results
        success_count = sum(1 for v in results.values() if v)
        total_count = len(symbols)
        
        logger.info(f"Processed {success_count}/{total_count} symbols successfully")
        
        # Log individual results
        for symbol, success in results.items():
            status = "✓" if success else "✗"
            logger.info(f"{status} {symbol}")
        
        # Fail the task if no symbols were processed successfully
        if success_count == 0:
            raise Exception("All symbols failed to process")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in fetch_and_store_stock_data: {e}")
        raise


def generate_summary(**context):
    """Task to generate pipeline execution summary."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Get results from previous task
    ti = context['ti']
    results = ti.xcom_pull(task_ids='fetch_stock_data')
    
    if results:
        success_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        success_rate = (success_count / total_count) * 100 if total_count > 0 else 0
        
        summary = f"""
        ===== Stock Data Pipeline Summary =====
        Execution Date: {context['ds']}
        Total Symbols: {total_count}
        Successful: {success_count}
        Failed: {total_count - success_count}
        Success Rate: {success_rate:.2f}%
        ======================================
        """
        
        logger.info(summary)
        return summary
    else:
        logger.warning("No results available from previous task")
        return "No summary available"


# Task 1: Check database connection
check_db = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    dag=dag,
)

# Task 2: Fetch and store stock data
fetch_data = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_and_store_stock_data,
    dag=dag,
)

# Task 3: Generate summary
summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_db >> fetch_data >> summary