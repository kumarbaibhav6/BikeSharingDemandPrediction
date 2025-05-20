import json
import boto3
import base64
import pandas as pd
import joblib  # Fixed import
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Load the model and scaler
try:
    model = joblib.load('/Users/kumarbaibhav/Downloads/Bike Sharing Demand Prediction/bike_rental_lambda/package/bike_rental_counts.pkl')
    scaler = joblib.load('/Users/kumarbaibhav/Downloads/Bike Sharing Demand Prediction/bike_rental_lambda/package/bike_rental_counts.pkl')
    logger.info("Model and scaler loaded successfully.")
except Exception as e:
    logger.error(f"Error loading model or scaler: {str(e)}")

def lambda_handler(event, context):
    try:
        # Iterate through each record from Kinesis
        for record in event['Records']:
            # Decode the data from base64
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            data = json.loads(payload)

            # Extract the features from the record
            features = [
                data["hour"], data["temperature"], data["humidity"], 
                data["wind_speed"], data["visibility"], data["solar_radiation"], 
                data["rainfall"], data["snowfall"], data["weekend"], data["holiday_status"]
            ]

            # Convert to DataFrame and scale the input features
            df = pd.DataFrame([features], columns=[
                "hour", "temperature", "humidity", "wind_speed", 
                "visibility", "solar_radiation", "rainfall", 
                "snowfall", "weekend", "holiday_status"
            ])
            scaled_features = scaler.transform(df)

            # Predict bike rental count
            prediction = model.predict(scaled_features)[0]
            logger.info(f"Predicted bike count: {int(prediction)}")

            # Log and return the prediction
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'predicted_bike_count': int(prediction),
                    'features': data
                })
            }

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
