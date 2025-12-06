🛒 POS2ETL: E-Commerce Data Pipeline

POS2ETL is a fully automated Extract–Transform–Load (ETL) system for processing E-commerce Point-of-Sale (POS) data from CSV files into a PostgreSQL Data Warehouse, with real-time insights displayed through a Streamlit dashboard.

All components run in isolated containers orchestrated via Docker Compose, ensuring a consistent development and production environment.

🚀 Architecture & Data Flow

The project is composed of four main containerized services:

1. db (PostgreSQL)

The central data warehouse where cleaned and transformed sales data is stored.

2. etl (Python)

Runs etl.py, which:

Watches the local ./data folder

Reads all incoming CSV files

Cleans, deduplicates, and transforms the records

Loads unique transactions into the data warehouse

3. dashboard (Streamlit)

A web interface (dashboard.py) that:

Connects directly to the PostgreSQL warehouse

Displays metrics including revenue, profit, and top-selling products

Provides interactive charts and filters

4. scheduler (Alpine/Shell)

A background loop that:

Triggers the ETL service every 60 seconds

Ensures new CSVs are processed automatically

Keeps the warehouse continuously up to date

Data Pipeline Overview
New CSV → scheduler triggers ETL → ETL loads into PostgreSQL → dashboard visualizes


Place a CSV file into ./data/ and it is processed automatically.

📦 Prerequisites

Install the following before running the project:

Docker

Docker Compose

These tools handle all dependency management and service orchestration.

🛠️ Project Setup
1. Clone the Repository
git clone https://github.com/sholex111/pos2etl.git
cd pos2etl

2. Configure Environment Variables

Create or edit the .env file located in the project root.

# Database Credentials
POSTGRES_USER=ecomm_user
POSTGRES_PASSWORD=strong_password
POSTGRES_DB=datetimeline_dw

# Database Host (used by ETL & Dashboard)
DB_HOST=db
DB_PORT=5432


Feel free to change the username, password, and database name.

3. Input Data Files

Place all your .csv files in the local:

./data/


The ETL engine automatically detects and processes them within 60 seconds, thanks to the scheduler.

▶️ Running the Project

Run these commands from the pos2etl/ directory.

1. Build and Start All Services
docker-compose up --build -d


This launches:

PostgreSQL

ETL service

Streamlit dashboard

Scheduler service

2. Access the Analytics Dashboard

After startup, open:

http://localhost:8501


You will see your sales metrics, charts, and live analytics.

3. Monitor the Scheduler (Important)

To confirm that the ETL runs every 60 seconds:

docker-compose logs -f scheduler


You should see logs like:

--- running etl.py ---
ETL completed successfully.


This confirms that automation is active.

4. Refresh Dashboard Data

After new data is processed:

Open the Streamlit dashboard

Click “Force Data Refresh”

New metrics appear instantly

🛑 Stopping the Project

To stop and remove all containers:

docker-compose down