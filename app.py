from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import pandas as pd
from typing import Optional
from azure.storage.blob import BlobServiceClient
import json
import os
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler  # Import the scaler

# FastAPI app initialization
app = FastAPI()

# Load .env file
load_dotenv()

# Azure Blob Storage connection setup
blob_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "weather-data"
blob_name = "075e16cd-f153-48cd-84c6-0701f92e238d"

# Load your trained model
with open('bike_rental_counts.pkl', 'rb') as f:
    model = pickle.load(f)

# Load the scaler (if it was saved during training)
with open('scaler.pkl', 'rb') as f:
    scaler = pickle.load(f)

# Function to fetch the weather data from Azure Blob Storage
def fetch_weather_data_from_blob():
    """
    Fetch the JSON weather data from the Azure Blob Storage container.
    """
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    # Download blob and decode it to JSON format
    blob_data = blob_client.download_blob()
    json_data = json.loads(blob_data.readall().decode('utf-8'))  # Decode bytes to JSON
    return json_data

# Pydantic model for input data (User will provide these inputs)
class WeatherData(BaseModel):
    weekend: int  # Binary input (1 if weekend, 0 otherwise)
    holiday_status: int  # Binary input (1 if holiday, 0 otherwise)
    solar_radiation: Optional[float] = 0.0  # Solar radiation in MJ/m² (optional, can be zero)
    snowfall: Optional[float] = 0.0  # Snowfall in cm (optional, can be zero)

# Feature extraction function
def extract_features_from_weather(weather_data):
    """
    Extract the relevant features from the weather data.
    """
    # Extract hour from localtime (assuming the format 'YYYY-MM-DD HH:MM')
    localtime = weather_data['current']['last_updated']  # Extract from API response
    hour = int(localtime.split(" ")[1].split(":")[0])
    
    # Extract weather data
    temp_c = weather_data['current']['temp_c']
    humidity = weather_data['current']['humidity']
    wind_kph = weather_data['current']['wind_kph']
    precip_mm = weather_data['current']['precip_mm']
    vis_km = weather_data['current']['vis_km']

    # Convert wind speed from km/h to m/s
    wind_speed_mps = wind_kph / 3.6

    # Create a feature dictionary
    features = {
        "Hour": hour,
        "Temperature(°C)": temp_c,
        "Humidity(%)": humidity,
        "Wind speed (m/s)": wind_speed_mps,
        "Visibility (10m)": vis_km * 1000,  # Convert km to meters
        "Solar Radiation (MJ/m2)": 0.0,  # Default value or fetched from user input
        "Rainfall(mm)": precip_mm,
        "Snowfall (cm)": 0.0,  # Default value or fetched from user input
        "Weekend": 0,  # Default value or fetched from user input
        "Holiday_status": 0  # Default value or fetched from user input
    }
    
    # Convert the features into a pandas DataFrame
    features_df = pd.DataFrame([features])
    return features_df

@app.get("/")
async def read_root():
    """
    Root endpoint to verify the app is running.
    """
    return {"message": "Welcome to the FastAPI app!"}

@app.post("/predict")
async def predict(weather_data: WeatherData):
    """
    Endpoint for making predictions based on weather data and user inputs.
    """
    try:
        # Fetch the weather data from the Azure Blob Storage
        weather_data_blob = fetch_weather_data_from_blob()
        
        # Extract features from the weather data
        features_df = extract_features_from_weather(weather_data_blob)
        
        # Add user inputs to the extracted features
        features_df["Solar Radiation (MJ/m2)"] = weather_data.solar_radiation
        features_df["Snowfall (cm)"] = weather_data.snowfall
        features_df["Weekend"] = weather_data.weekend
        features_df["Holiday_status"] = weather_data.holiday_status
        
        # Scale the features using the loaded scaler
        features_scaled = scaler.transform(features_df)  # Use the scaler to transform the features
        
        # Make a prediction
        prediction = model.predict(features_scaled)  # Use the scaled features for prediction
        
        # Return the prediction as a JSON response
        return {"prediction": prediction.tolist()}
    
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

@app.get("/fetch_weather")
async def fetch_weather():
    """
    Fetch weather data from Azure Blob Storage.
    """
    try:
        # Fetch weather data from Azure Blob Storage
        weather_data = fetch_weather_data_from_blob()
        
        # Return weather data as JSON response
        return {"weather_data": weather_data}
    
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
