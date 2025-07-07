import sqlite3
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import time
from sqlalchemy import create_engine, text
import warnings
import os
warnings.filterwarnings('ignore')

#logging info storage
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_etl.log'),
        logging.StreamHandler()
    ]
)

class WeatherETL:
    def __init__(self, db_path: str = "weather.db"):
        '''
        initializing pipeline
        '''
        self.db_path = db_path
        #self.nasa_api_key = nasa_api_key or "DEMO_KEY"
        self.engine = None
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect_to_database(self) -> bool:
        
        try:
            # Create connection string for SQLite
            conn_str = f"sqlite:///{self.db_path}"
            
            self.engine = create_engine(conn_str)
            self.connection = self.engine.connect()
            self.connection.execute(text("SELECT 1"))
            self.logger.info(f"connected to SQLite database: {self.db_path}")
            return True

        except Exception as e:
            self.logger.error(f"failed to connect to database: {e}")
            return False
    
    def create_tables(self):

        try:
            # Earth Weather Data Table
            earth_weather_table = """
            CREATE TABLE IF NOT EXISTS earth_weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location TEXT,
                latitude REAL,
                longitude REAL,
                date_time DATETIME,
                temperature_mean REAL,
                temperature_max REAL,
                temperature_min REAL,
                humidity_mean REAL,
                wind_speed_max REAL,
                wind_gust_max REAL,
                wind_direction_dominant REAL,
                precipitation_sum REAL,
                data_quality_score REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            #index 
            create_index = """
            CREATE INDEX IF NOT EXISTS idx_location_date 
            ON earth_weather_data(location, date_time)
            """
            
            self.connection.execute(text(earth_weather_table))
            self.connection.execute(text(create_index))
            self.connection.commit()
            self.logger.info("Database tables created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            raise
    
    
    def fetch_earth_weather_data(self, locations: List[Dict], days_back: int = 7) -> Optional[pd.DataFrame]:
        
        try:
            all_data = []
            end_date = (datetime.utcnow() - timedelta(days=1)).date()
            start_date = (end_date - timedelta(days=days_back))
            
            for location in locations:
                try:
                    self.logger.info(f"Fetching data for {location['name']}...")
                    
                    url = "https://archive-api.open-meteo.com/v1/archive"
                    params = {
                        "latitude": location["lat"],
                        "longitude": location["lon"],
                        "start_date": str(start_date),
                        "end_date": str(end_date),
                        "daily": [
                        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                        "precipitation_sum", "windspeed_10m_max", "windgusts_10m_max",
                        "winddirection_10m_dominant", "relative_humidity_2m_mean"
                        ],
                        "timezone": "UTC"
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    daily_data = data.get("daily", {})
                    dates = daily_data.get("time", [])
                        
                    for i, date_str in enumerate(dates):
                        record = {
                            "location": location["name"],
                            "latitude": location["lat"],
                            "longitude": location["lon"],
                            "date_time": date_str,
                            "temperature_mean": daily_data.get("temperature_2m_mean", [None])[i],
                            "temperature_max": daily_data.get("temperature_2m_max", [None])[i],
                            "temperature_min": daily_data.get("temperature_2m_min", [None])[i],
                            "humidity_mean": daily_data.get("relative_humidity_2m_mean", [None])[i],
                            "wind_speed_max": daily_data.get("windspeed_10m_max", [None])[i],
                            "wind_gust_max": daily_data.get("windgusts_10m_max", [None])[i],
                            "wind_direction_dominant": daily_data.get("winddirection_10m_dominant", [None])[i],
                            "precipitation_sum": daily_data.get("precipitation_sum", [None])[i]
                        }
                        all_data.append(record)
            
                    time.sleep(0.3)
                                    
                except Exception as e:
                    self.logger.warning(f"Error fetching data for {location['name']}: {e}")
                    continue
        
            if all_data:
                df = pd.DataFrame(all_data)
                self.logger.info(f"Fetched {len(df)} records for {len(locations)} locations")
                return df
            else:
                self.logger.warning("No Earth weather data fetched")
                return None
        except Exception as e:
            self.logger.error(f"fetch error: {e}")
            return None
                    
    def clean_and_transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        
        try:
            if df is None or df.empty:
                return df
            
            df['date_time'] = pd.to_datetime(df['date_time'])

            #a copy to avoid modifying original data
            df = df.copy()
            
            #cleaning numerical columns
            numerical_cols = [
                'temperature_mean', 'temperature_max', 'temperature_min',
                'humidity_mean',
                'wind_speed_max', 'wind_gust_max', 'wind_direction_dominant',
                'precipitation_sum'
            ]

            
            for col in numerical_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    #handling extreme outliers based on column type
                    if col in ['temperature_mean', 'temperature_max', 'temperature_min']:
                        #remove temperatures outside reasonable range (-60 to 60 C)
                        df.loc[df[col] < -60, col] = np.nan
                        df.loc[df[col] > 60, col] = np.nan
                    elif col == 'humidity_mean':
                        #humidity should be 0-100%
                        df.loc[df[col] < 0, col] = 0
                        df.loc[df[col] > 100, col] = 100
                    elif col == 'wind_direction_dominant':
                        #wind direction should be 0-360 degrees
                        df.loc[df[col] < 0, col] = np.nan
                        df.loc[df[col] > 360, col] = np.nan
                    elif col == 'wind_speed_max':
                        #wind speed should be non-negative and reasonable (<200 m/s)
                        df.loc[df[col] < 0, col] = np.nan
                        df.loc[df[col] > 200, col] = np.nan
                    elif col == 'precipitation_sum':
                        #precipitation should be non-negative
                        df.loc[df[col] < 0, col] = 0
            
            #removes rows with all null weather data
            key_cols = ['temperature_mean', 'humidity_mean']
            df = df.dropna(subset=key_cols, how='all')
            
            #remove duplicate records
            df = df.drop_duplicates(subset=['location', 'date_time'])
            
            #add data quality score
            df['data_quality_score'] = self._calculate_data_quality_score(df)
            
            #date_time to string for SQLite compatibility
            df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            
            self.logger.info(f"Cleaned Earth weather data: {len(df)} records remaining")
            return df
            
        except Exception as e:
            self.logger.error(f"Error cleaning Earth weather data: {e}")
            return df
    
    def _calculate_data_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate data quality score based on completeness"""
        try:
            key_columns = ['temperature_mean', 'humidity_mean', 'wind_speed_max']
            
            #calculate completeness score (0-1)
            completeness = df[key_columns].notna().sum(axis=1) / len(key_columns)
            
            #converting to 0-10 scale
            quality_score = (completeness * 10).round(1)
            
            return quality_score
            
        except Exception as e:
            self.logger.error(f"Error calculating data quality score: {e}")
            return pd.Series([5.0] * len(df)) if df is not None else pd.Series(dtype='float64')
    
    def save_to_database(self, df: pd.DataFrame, if_exists: str = 'append'):

        try:
            if df is None or df.empty:
                self.logger.warning("No data to save to database")
                return
            
            #save to database
            df.to_sql('earth_weather_data', self.engine, if_exists=if_exists, index=False, method='multi', chunksize=1000)
            
            self.logger.info(f"Successfully saved {len(df)} records to earth_weather_data table")
            
        except Exception as e:
            self.logger.error(f"Error saving data to database: {e}")
            raise
    
    def run_etl_pipeline(self, locations: Optional[List[Dict]] = None, days_back: int = 7):
        try:
            self.logger.info("Starting Earth Weather ETL Pipeline")
            
            #default locations if none provided
            if locations is None:
                locations = [
                    {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
                    {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
                    {'name': 'Tokyo', 'lat': 35.6762, 'lon': 139.6503},
                    {'name': 'Sydney', 'lat': -33.8688, 'lon': 151.2093},
                    {'name': 'Mumbai', 'lat': 19.0760, 'lon': 72.8777},
                    {'name': 'Los Angeles', 'lat': 34.0522, 'lon': -118.2437}
                ]
            
            #connect to database
            if not self.connect_to_database():
                raise Exception("Failed to connect to database")
            
            #create tables
            self.create_tables()
            
            #fetch and process Earth weather data
            self.logger.info(f"Fetching Earth weather data for {len(locations)} locations...")
            earth_data = self.fetch_earth_weather_data(locations, days_back)
            
            if earth_data is not None and not earth_data.empty:
                #clean and transform data
                earth_data_cleaned = self.clean_and_transform_data(earth_data)
                
                #save to database
                self.save_to_database(earth_data_cleaned)
                
                #show summary
                self.print_summary(earth_data_cleaned)
            else:
                self.logger.warning("No Earth weather data to process")
            
            self.logger.info("ETL Pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {e}")
            raise
        finally:
            #close database connection
            if self.connection:
                self.connection.close()
            self.logger.info("Database connection closed")
    
    def print_summary(self, df: pd.DataFrame):
        """Print a summary of the processed data"""
        try:
            if df is None or df.empty:
                return
                
            summary = f"""
            
Earth Weather Data Summary:
========================================
Total Records: {len(df)}
Date Range: {df['date_time'].min()} to {df['date_time'].max()}
Locations: {', '.join(df['location'].unique())}

Data Quality:
- Avg Score: {df['data_quality_score'].mean():.1f}/10
- Records with Score ≥ 8: {len(df[df['data_quality_score'] >= 8])}

Temperature:
- Min: {df['temperature_min'].min():.1f}°C
- Max: {df['temperature_max'].max():.1f}°C
- Avg: {df['temperature_mean'].mean():.1f}°C

Other Stats:
- Humidity Avg: {df['humidity_mean'].mean():.1f}%
- Wind Speed Avg: {df['wind_speed_max'].max():.1f} m/s

Database saved as: {self.db_path}
            """
            
            print(summary)
            self.logger.info("Data summary printed")
            
        except Exception as e:
            self.logger.error(f"Error printing summary: {e}")
    

def main():
    db_path = "weather.db"  
    
    #run ETL pipeline
    etl = WeatherETL(db_path=db_path)
    etl.run_etl_pipeline()

if __name__ == "__main__":
    main()
    