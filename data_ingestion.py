import requests
import boto3
import json
import time
import base64
from datetime import datetime, timedelta, timezone
import os

# AWS Kinesis configuration
STREAM_NAME = "weather-stream"
REGION = "us-east-1"

# OpenWeatherMap API configuration
API_KEY = "9ba8eed24aeb3474e121dfbd8584a5d3"  # Replace with your actual API key
LAT = "37.5665"  # Latitude for Seoul
LON = "126.9780"  # Longitude for Seoul

# Initialize the Kinesis client
try:
    kinesis_client = boto3.client("kinesis", region_name=REGION)
    print("Kinesis client initialized successfully.")
except Exception as e:
    print(f"Error initializing Kinesis client: {e}")

# Check if today is a holiday in Korea
def is_korean_holiday():
    try:
        year = datetime.today().year
        url = f"https://date.nager.at/Api/v2/PublicHolidays/{year}/KR"
        response = requests.get(url)
        if response.status_code == 200:
            holidays = response.json()
            today = datetime.today().strftime("%Y-%m-%d")
            for holiday in holidays:
                if holiday['date'] == today:
                    print(f"Today is a Korean holiday: {holiday['localName']}")
                    return 1
            return 0
        else:
            print(f"Failed to fetch holiday data. Status code: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error fetching holiday data: {e}")
        return 0

def fetch_weather():
    """Fetch weather data from OpenWeatherMap API using latitude and longitude."""
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        data = response.json()

        # Log full API response for debugging
        print("API Response:", data)

        # Check for valid response
        if response.status_code != 200 or "main" not in data:
            print(f"Invalid API response: {data.get('message', 'No data')}")
            return None

        # Calculate Seoul local time
        utc_timestamp = datetime.fromtimestamp(data['dt'], tz=timezone.utc)
        local_timestamp = utc_timestamp + timedelta(seconds=data['timezone'])
        hour = local_timestamp.hour  # Local hour of Seoul

        # Check if it's a weekend
        is_weekend = 1 if datetime.today().weekday() >= 5 else 0
        is_holiday = is_korean_holiday()

        # Prepare weather data
        weather = {
            "hour": hour,
            "temperature": data["main"].get("temp", 0.0),
            "humidity": data["main"].get("humidity", 0),
            "wind_speed": data["wind"].get("speed", 0.0),
            "visibility": data.get("visibility", 1000),  # Visibility in meters
            "solar_radiation": data.get("solarRadiation", 0.1),  # Placeholder
            "rainfall": data.get("rain", {}).get("1h", 0),  # mm
            "snowfall": data.get("snow", {}).get("1h", 0) / 10,  # Convert mm to cm
            "weekend": is_weekend,
            "holiday_status": is_holiday
        }
        
        # Validate data
        if all(key in weather for key in ["hour", "temperature", "humidity", "wind_speed", "visibility", "solar_radiation", "rainfall", "snowfall", "weekend", "holiday_status"]):
            print(f"Weather data fetched: {weather}")
            return weather
        else:
            print("Incomplete weather data, skipping.")
            return None

    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None

def send_to_kinesis(data):
    """Send weather data to AWS Kinesis."""
    try:
        print("Sending data to Kinesis...")
        payload = json.dumps(data)  # Ensure the data is a JSON string
        
        # Print JSON before encoding
        print(f"Payload before encoding: {payload}")
        print(f"Payload type: {type(payload)}")
        
        # Send to Kinesis without base64 encoding for simplicity
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=payload.encode("utf-8"),  # Just UTF-8 encode, not base64
            PartitionKey="weather-data"
        )
        
        # Verify the response
        print(f"Kinesis response: {response}")
        if "SequenceNumber" in response:
            print(f"SUCCESS: Data sent to Kinesis with sequence number: {response['SequenceNumber']}")
            
            # Save sequence number to a file for debugging
            with open("last_ingestion_sequence.txt", "w") as f:
                f.write(response['SequenceNumber'])
                f.write("\n")
                f.write(datetime.now().isoformat())
        else:
            print("WARNING: No sequence number in response")
            
    except Exception as e:
        print(f"Error sending data to Kinesis: {e}")
        import traceback
        print(traceback.format_exc())

# For testing: Send a single data point and exit
def send_test_data():
    """Send a single test data point to Kinesis."""
    print("Sending test data point...")
    test_data = {
        "hour": datetime.now().hour,
        "temperature": 22.5,
        "humidity": 65,
        "wind_speed": 3.0,
        "visibility": 10000,
        "solar_radiation": 0.8,
        "rainfall": 0,
        "snowfall": 0,
        "weekend": 1 if datetime.today().weekday() >= 5 else 0,
        "holiday_status": 0
    }
    send_to_kinesis(test_data)
    print("Test data sent.")

def run_data_ingestion():
    """Continuously fetch and send data to Kinesis."""
    while True:
        try:
            weather_data = fetch_weather()
            if weather_data:
                send_to_kinesis(weather_data)
            else:
                print("No data fetched, retrying in 15 minutes.")
        except Exception as e:
            print(f"Error during data ingestion: {e}")
            import traceback
            print(traceback.format_exc())
        
        # Sleep for 15 minutes
        print(f"Sleeping for 15 minutes. Next update at {(datetime.now() + timedelta(minutes=15)).strftime('%H:%M:%S')}")
        time.sleep(900)

if __name__ == "__main__":
    # Check for test mode
    if os.environ.get("TEST_MODE", "").lower() == "true":
        print("Running in TEST MODE - sending a single data point")
        send_test_data()
    else:
        print("Starting continuous data ingestion...")
        run_data_ingestion()