
# Weather ETL Pipeline

This project is a weather data ETL (Extract, Transform, Load) pipeline that fetches historical daily weather data from the [Open-Meteo API](https://open-meteo.com/), cleans it, performs basic validation, and stores it in a local SQLite database.

---

## Features

- Fetches weather data for multiple global cities  
- Cleans and transforms data for consistency  
- Calculates a data quality score  
- Stores the results in `weather.db` (SQLite)  
- Prints summary statistics to console

---

## Project Structure

```
weather-etl/
│
├── nasa_weather_etl.py           # Main ETL pipeline
├── delete_table_script.py        # Script to reset DB table (optional)
├── requirements.txt              # Python package dependencies
├── weather.db                    # SQLite database (generated after running)
├── nasa_weather_env/             # Virtual environment folder
└── README.md                     # Project documentation
```

---

## Setup

### 1. Create a virtual environment (recommended)

```bash
python -m venv nasa_weather_env
```

### 2. Activate it

```bash
# Windows
nasa_weather_env\Scripts\activate

# macOS/Linux
source nasa_weather_env/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Run the Pipeline

```bash
python nasa_weather_etl.py
```

This will:
- Connect to/create `weather.db`
- Fetch 7 days of weather data for 6 cities
- Clean and validate it
- Store it in the database
- Print a summary report

---

## Query the Data

Use your SQLite browser of choice or tools like `sqlite3` in terminal:

```sql
SELECT * FROM earth_weather_data LIMIT 10;
```

---

## Customize

To add more cities, modify this block in `run_etl_pipeline()`:

```python
locations = [
    {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
    {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
    ...
]
```

---

## License

MIT License – free to use, modify, and distribute.

---

## Contributions

Feel free to fork this repo, open issues, or submit pull requests.  
Let’s make open-source weather data pipelines better together!
