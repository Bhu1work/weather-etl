import sqlite3

# Connect to your local SQLite database file
conn = sqlite3.connect("weather.db")
cursor = conn.cursor()

# Drop the existing table
cursor.execute("DROP TABLE IF EXISTS earth_weather_data;")

# Commit and close
conn.commit()
conn.close()
print("Table dropped successfully.")
