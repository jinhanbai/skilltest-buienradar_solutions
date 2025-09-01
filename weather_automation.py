#!/usr/bin/env python3
"""
Weather Data Automation Script
Automatically fetches weather data from Buienradar every 20 minutes and stores it in SQLite database.
"""

import requests
import pandas as pd
import sqlite3
import time
import logging
from datetime import datetime
import os
from pathlib import Path

# Set up loggs
script_dir = Path(__file__).parent
log_file = script_dir / "weather_automation.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

'''
The Buienradar Client is a wrapper that performs the automation pipeline with logging and error handling
, following the steps as described in part 1.
That is:
1) Fetch the data from API
2) Filters and processes the data
3) Stores/updates the tables in the continuous database (weather_cont.db)

'''
class BuienradarClient:
    def __init__(self, db_path):
        self.db_path = db_path
        self.endpoint = 'https://data.buienradar.nl/2.0/feed/json'
        #sqlite datatypes
        self.stations_dtype = {
            'stationid': 'TEXT',
            'stationname': 'TEXT',
            'lat': 'REAL',
            'lon': 'REAL',
            'regio': 'TEXT'
        }
        self.measurements_dtype = {
            'measurementid': 'TEXT',
            'stationid': 'TEXT',
            'timestamp': 'TEXT',
            'temperature': 'REAL',
            'groundtemperature': 'REAL',
            'feeltemperature': 'REAL',
            'windgusts': 'REAL',
            'windspeedBft': 'INTEGER',
            'humidity': 'REAL',
            'precipitation': 'REAL',
            'sunpower': 'REAL'
        }
        
    def fetch_weather_data(self):
        """Fetch weather data from Buienradar API"""
        try:
            logger.info("Fetching weather data from Buienradar API...")
            response = requests.get(url=self.endpoint, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            station_measurements = data['actual']['stationmeasurements']
            
            logger.info(f"Successfully fetched data for {len(station_measurements)} stations")
            return station_measurements
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data: {e}")
            return None
        except KeyError as e:
            logger.error(f"Unexpected data structure: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def process_data(self, raw_data):
        """Process raw weather data into DataFrames"""
        try:
            if not raw_data:
                return None, None
                
            # Convert to DataFrame
            data = pd.DataFrame(raw_data)
            
            # Create measurements dataset
            measurements = data[['stationid', 'timestamp', 'temperature', 'groundtemperature', 
                               'feeltemperature', 'windgusts', 'windspeedBft', 'humidity', 
                               'precipitation', 'sunpower']].drop_duplicates().copy()
            
            # Create measurement ID
            measurements['measurementid'] = (
                measurements['stationid'].astype(str) + '_' + measurements['timestamp'].astype(str)
            )

            # Placeholder for temperature imputation logic based on regional distance :^)

            # Create stations dataset
            stations = data[['stationid', 'stationname', 'lat', 'lon', 'regio']].drop_duplicates().copy()
            
            logger.info(f"Processed {len(measurements)} measurements and {len(stations)} stations")
            
            
            return measurements, stations
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return None, None
    
    def setup_database(self):
        """Set up database tables if they don't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA foreign_keys = ON")
            
            with conn:
                # Stations table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stations (
                        stationid TEXT PRIMARY KEY,
                        stationname TEXT,
                        lat REAL,
                        lon REAL,
                        regio TEXT
                    )
                """)
                
                # Measurements table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS measurements (
                        measurementid TEXT PRIMARY KEY,
                        stationid TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        temperature REAL,
                        groundtemperature REAL,
                        feeltemperature REAL,
                        windgusts REAL,
                        windspeedBft INTEGER,
                        humidity REAL,
                        precipitation REAL,
                        sunpower REAL,
                        FOREIGN KEY (stationid) REFERENCES stations(stationid) ON DELETE CASCADE
                    )
                """)
                
                # Useful indexes and constraints
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_measurements_stationid_timestamp ON measurements(stationid, timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_measurements_stationid ON measurements(stationid)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_measurements_timestamp ON measurements(timestamp)")
            
            conn.close()
            logger.info("Database setup completed")
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
    
    def store_data(self, measurements, stations):
        """Store data in database with proper handling of duplicates"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Store stations (replace to handle any updates)
            if stations is not None and not stations.empty:
                stations.to_sql("stations", conn, if_exists="replace", index=False, dtype=self.stations_dtype)
                logger.info(f"Stored {len(stations)} stations")
            
            # Store measurements (append new ones, ignore duplicates)
            if measurements is not None and not measurements.empty:
                # Use 'append' mode and let SQLite handle duplicates via unique constraint
                measurements.to_sql("measurements", conn, if_exists="append", index=False, dtype=self.measurements_dtype)
                logger.info(f"Stored {len(measurements)} measurements")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error storing data: {e}")
    
    def run_collection_cycle(self):
        """Run one complete collection cycle"""
        try:
            logger.info("Starting weather data collection cycle...")
            
            # Fetch data
            raw_data = self.fetch_weather_data()
            if not raw_data:
                logger.warning("No data fetched, skipping this cycle")
                return False
            
            # Process data
            measurements, stations = self.process_data(raw_data)
            if measurements is None or stations is None:
                logger.warning("Data processing failed, skipping this cycle")
                return False
            
            # Store data
            self.store_data(measurements, stations)
            
            logger.info("Weather data collection cycle completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in collection cycle: {e}")
            return False

def main():
    """Main function to run the weather data collection"""
    # Get the script directory and set up database path
    script_dir = Path(__file__).parent
    db_path = script_dir / "database" / "weather_stream.db"
    
    # Ensure database directory exists
    db_path.parent.mkdir(exist_ok=True)
    
    # Initialize client
    collector = BuienradarClient(db_path=str(db_path))
    
    # Set up database tables
    collector.setup_database()
    
    # Data gets updated every 20 minutes
    interval_minutes = 20
    interval_seconds = interval_minutes * 60
    
    logger.info(f"Starting weather data automation. Running every {interval_minutes} minutes.")
    logger.info(f"Database location: {db_path}")
    
    try:
        while True:
            start_time = time.time()
            
            # Run collection cycle
            success = collector.run_collection_cycle()
            
            # Calculate sleep time (account for execution time)
            execution_time = time.time() - start_time
            sleep_time = max(0, interval_seconds - execution_time)
            
            if success:
                logger.info(f"Next collection in {sleep_time/60:.1f} minutes")
            else:
                logger.warning(f"Collection failed, retrying in {sleep_time/60:.1f} minutes")
            
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Weather data automation stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")

if __name__ == "__main__":
    main()
