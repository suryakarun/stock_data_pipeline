# Dockerized Stock Data Pipeline with Apache Airflow

A production-ready data pipeline that automatically fetches, processes, and stores stock market data using Apache Airflow and PostgreSQL, all containerized with Docker.

## 🎯 Features

- **Automated Data Collection**: Hourly fetching of stock data from Alpha Vantage API
- **Robust Error Handling**: Comprehensive error management and data validation
- **Scalable Architecture**: Docker-based deployment for easy scaling
- **Data Persistence**: PostgreSQL database with optimized schema
- **Production-Ready**: Includes logging, monitoring, and retry mechanisms

## 📋 Prerequisites

- Docker (version 20.10+)
- Docker Compose (version 2.0+)
- Alpha Vantage API Key ([Get free key](https://www.alphavantage.co/support/#api-key))

## 🚀 Quick Start

### 1. Clone and Setup
```bash
# Create project directory
mkdir stock-data-pipeline
cd stock-data-pipeline

# Create necessary directories
mkdir -p dags scripts sql logs
```

### 2. Configure Environment

Create a `.env` file in the project root:
```env
# API Configuration
ALPHA_VANTAGE_API_KEY=YOUR_API_KEY_HERE
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA

# PostgreSQL Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=stock_data
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/stock_data
AIRFLOW__CORE__FERNET_KEY=
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

**Generate Fernet Key:**
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Build and Run
```bash
# Build and start all services
docker-compose up -d

# Initialize Airflow (first time only)
docker-compose run airflow-init

# Check service status
docker-compose ps
```

### 4. Access Airflow UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 5. Enable the DAG

1. Navigate to the Airflow UI
2. Find `stock_data_pipeline` in the DAGs list
3. Toggle the DAG to "On"
4. Click "Trigger DAG" to run immediately

## 📊 Database Access

Access PostgreSQL directly:
```bash
docker-compose exec postgres psql -U airflow -d stock_data
```

Query stock data:
```sql
-- View latest stock prices
SELECT symbol, timestamp, close, volume 
FROM stock_prices 
ORDER BY timestamp DESC 
LIMIT 10;

-- Check data for specific symbol
SELECT * FROM stock_prices 
WHERE symbol = 'AAPL' 
ORDER BY timestamp DESC;
```

## 🔧 Configuration

### Adding More Symbols

Edit `.env` file:
```env
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA,FB,NFLX
```

Restart services:
```bash
docker-compose down
docker-compose up -d
```

### Changing Schedule

Edit `dags/stock_data_pipeline.py`:
```python
schedule_interval='@daily',  # Options: @hourly, @daily, @weekly, or cron
```

### API Rate Limiting

Alpha Vantage free tier: **5 API calls per minute**

The pipeline includes automatic rate limiting (12-second delay between calls).

## 📁 Project Structure
```
stock-data-pipeline/
├── docker-compose.yml      # Docker services configuration
├── Dockerfile              # Airflow container image
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (not in git)
├── README.md              # This file
├── dags/
│   └── stock_data_pipeline.py  # Airflow DAG definition
├── scripts/
│   ├── __init__.py
│   └── fetch_stock_data.py     # Data fetching logic
├── sql/
│   └── init.sql               # Database initialization
└── logs/                      # Airflow logs
```

## 🐛 Troubleshooting

### Services Won't Start
```bash
# Check logs
docker-compose logs airflow-webserver
docker-compose logs postgres

# Restart services
docker-compose restart
```

### Database Connection Issues
```bash
# Verify database is ready
docker-compose exec postgres pg_isready -U airflow

# Check environment variables
docker-compose exec airflow-webserver env | grep POSTGRES
```

### API Errors

- Verify API key is correct in `.env`
- Check rate limiting (max 5 calls/minute for free tier)
- View logs: `docker-compose logs airflow-scheduler`

### DAG Not Appearing
```bash
# Check DAG syntax
docker-compose exec airflow-webserver python /opt/airflow/dags/stock_data_pipeline.py

# Refresh DAGs
docker-compose exec airflow-webserver airflow dags list
```

## 🛠️ Maintenance

### View Logs
```bash
# All logs
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f postgres
```

### Backup Database
```bash
docker-compose exec postgres pg_dump -U airflow stock_data > backup.sql
```

### Restore Database
```bash
docker-compose exec -T postgres psql -U airflow stock_data < backup.sql
```

### Stop Services
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v
```

## 📈 Monitoring

### Check Pipeline Status

1. Airflow UI: http://localhost:8080
2. View DAG runs and task logs
3. Monitor success/failure rates

### Database Statistics
```sql
-- Count records per symbol
SELECT symbol, COUNT(*) as record_count
FROM stock_prices
GROUP BY symbol
ORDER BY record_count DESC;

-- Latest update time
SELECT MAX(updated_at) as last_update
FROM stock_prices;
```

## 🔐 Security Best Practices

1. **Never commit `.env` file** - Add to `.gitignore`
2. **Use strong passwords** in production
3. **Rotate API keys** regularly
4. **Limit network exposure** - Use firewall rules
5. **Keep dependencies updated** - Run `pip list --outdated`

## 🚦 Production Deployment

For production environments:

1. Use managed PostgreSQL (AWS RDS, Cloud SQL)
2. Implement proper logging and monitoring
3. Set up alerts for pipeline failures
4. Use secrets management (HashiCorp Vault, AWS Secrets Manager)
5. Scale with CeleryExecutor for multiple workers
6. Implement data quality checks

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Alpha Vantage API Documentation](https://www.alphavantage.co/documentation/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 📝 License

This project is open-source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please submit pull requests or open issues for bugs and feature requests.

---

**Author**: Your Name  
**Last Updated**: December 2025