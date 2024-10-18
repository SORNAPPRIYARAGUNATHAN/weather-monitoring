import requests
import time
import sqlite3
from datetime import datetime
from statistics import mean

# OpenWeatherMap API configuration
API_KEY = 'e2df5c5712a77f625b59b8a35ccfccc4'  # Replace this with your actual API key
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
API_URL = "http://api.openweathermap.org/data/2.5/weather"

# Database setup
def init_db():
    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
                        city TEXT, 
                        date TEXT, 
                        temp REAL, 
                        max_temp REAL, 
                        min_temp REAL, 
                        dominant_condition TEXT
                      )''')
    conn.commit()
    return conn

# Fetch weather data from OpenWeatherMap API
def fetch_weather(city):
    params = {'q': city, 'appid': API_KEY}
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        print(f"Fetched data for {city}: {data}")  # Debug print to check response
        temp_kelvin = data['main']['temp']
        temp_celsius = kelvin_to_celsius(temp_kelvin)
        condition = data['weather'][0]['main']
        print(f"Weather in {city}: {condition}, Temperature: {temp_celsius:.2f}°C")  # Print weather condition
        return {
            'city': city,
            'temp': temp_celsius,
            'condition': condition,
            'timestamp': data['dt']
        }
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {city}: {e}")
        return None

# Convert temperature from Kelvin to Celsius
def kelvin_to_celsius(temp_kelvin):
    return temp_kelvin - 273.15

# Store weather data in the database
def store_weather_data(conn, city, date, temp, max_temp, min_temp, dominant_condition):
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO weather (city, date, temp, max_temp, min_temp, dominant_condition) 
                      VALUES (?, ?, ?, ?, ?, ?)''', (city, date, temp, max_temp, min_temp, dominant_condition))
    conn.commit()

# Process daily weather summaries and rollups
def daily_weather_summary(conn, city, data):
    date = datetime.now().strftime("%Y-%m-%d")
    temps = [item['temp'] for item in data]
    max_temp = max(temps)
    min_temp = min(temps)
    avg_temp = mean(temps)
    dominant_condition = max(set([item['condition'] for item in data]), key=[item['condition'] for item in data].count)

    store_weather_data(conn, city, date, avg_temp, max_temp, min_temp, dominant_condition)
    print(f"Weather summary for {city} on {date} -> Avg: {avg_temp:.2f}C, Max: {max_temp:.2f}C, Min: {min_temp:.2f}C, Condition: {dominant_condition}")

# Define alert system based on temperature thresholds
def check_alerts(data, threshold=35):
    alerts = []
    for item in data:
        if item['temp'] > threshold:
            alerts.append(f"Alert: {item['city']} temperature exceeds {threshold}C")
    return alerts

# Main loop to fetch data and calculate rollups
def run_weather_monitoring():
    conn = init_db()
    weather_data = {city: [] for city in CITIES}

    try:
        while True:
            for city in CITIES:
                data = fetch_weather(city)
                if data:
                    weather_data[city].append(data)
                
                    # Daily summaries
                    if len(weather_data[city]) > 24:  # Assuming we get 24 updates per day (once every hour)
                        daily_weather_summary(conn, city, weather_data[city])
                        weather_data[city] = []  # Reset for the next day

                    # Check temperature alerts
                    alerts = check_alerts(weather_data[city])
                    for alert in alerts:
                        print(alert)

            # Allow user input to fetch weather immediately
            user_input = input("Press Enter to fetch weather again or type 'quit' to exit: ")
            if user_input.lower() == 'quit':
                break
            
            time.sleep(10)  # Sleep for 10 seconds before the next round

    except KeyboardInterrupt:
        print("Weather monitoring stopped.")
    finally:
        conn.close()

# Entry point
if __name__ == "__main__":
    run_weather_monitoring()
