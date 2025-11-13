# app/utils.py

import logging
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# --- Logging Setup ---

def setup_logging() -> logging.Logger:
    """Configures and returns a root logger."""
    
    # Configure logger to output to console and a file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
        handlers=[
            logging.FileHandler("app.log"), # Log to a file
            logging.StreamHandler(sys.stdout) # Log to the console (for Docker logs)
        ]
    )
    
    logger = logging.getLogger(__name__)
    return logger

# --- Database Connection ---

def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine using environment variables.
    Handles connection errors.
    """
    logger = setup_logging()
    try:
        # Load database credentials from environment variables
        db_user = os.getenv('POSTGRES_USER')
        db_pass = os.getenv('POSTGRES_PASSWORD')
        db_host = os.getenv('POSTGRES_HOST')
        db_port = os.getenv('POSTGRES_PORT')
        db_name = os.getenv('POSTGRES_DB')

        # Check if all required variables are set
        if not all([db_user, db_pass, db_host, db_port, db_name]):
            logger.error("Database environment variables are not fully set.")
            raise ValueError("Missing database configuration in .env file.")
            
        # Create the connection string
        connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        
        # Create and return the engine
        engine = create_engine(connection_string)
        
        # Test the connection
        with engine.connect() as conn:
            logger.info(f"Successfully connected to database '{db_name}' at {db_host}.")
        
        return engine
    
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        sys.exit(1) # Exit the script if DB connection fails