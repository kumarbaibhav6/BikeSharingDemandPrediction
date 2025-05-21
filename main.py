from fastapi import FastAPI, BackgroundTasks
import pandas as pd
import boto3
import json
import base64
import joblib
import logging
import os
import datetime
import time
from pydantic import BaseModel

# Set up logging
logging.basicConfig(
    filename="logs/fastapi.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# AWS Kinesis configuration
STREAM_NAME = "weather-stream"
REGION = "us-east-1"
SEQUENCE_NUMBER_FILE = "logs/last_sequence_number.txt"

# Initialize Kinesis client
kinesis_client = boto3.client("kinesis", region_name=REGION)

# Load the model and scaler
try:
    current_dir = os.getcwd()
    model_path = os.path.join(current_dir, "bike_rental_counts.pkl")
    scaler_path = os.path.join(current_dir, "scaler.pkl")
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    logger.info("Model and scaler loaded successfully.")
except Exception as e:
    logger.error("Error loading model or scaler: %s", str(e))

# Define the correct column mapping for the retrained model
COLUMN_MAPPING = {
    "hour": "Hour",
    "temperature": "Temperature",
    "humidity": "Humidity",
    "wind_speed": "WindSpeed",
    "visibility": "Visibility",
    "solar_radiation": "SolarRadiation",
    "rainfall": "Rainfall",
    "snowfall": "Snowfall",
    "weekend": "Weekend",
    "holiday_status": "Holiday_status"
}

# FastAPI app initialization
app = FastAPI()

# Fix base64 padding
def fix_base64_padding(data):
    try:
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return data
    except Exception as e:
        logger.error("Error fixing padding: %s", str(e))
        return data

# Get the last processed sequence number
def get_last_sequence_number():
    try:
        with open(SEQUENCE_NUMBER_FILE, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

# Save the last processed sequence number
def save_last_sequence_number(sequence_number):
    try:
        with open(SEQUENCE_NUMBER_FILE, "w") as file:
            file.write(sequence_number)
    except Exception as e:
        logger.error("Error saving sequence number: %s", str(e))

# Get a new shard iterator
def get_new_shard_iterator(shard_id, iterator_type="TRIM_HORIZON"):
    try:
        response = kinesis_client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType=iterator_type
        )
        return response['ShardIterator']
    except Exception as e:
        logger.error("Error getting new shard iterator: %s", str(e))
        return None

# Force reset the sequence number file (for debugging)
def reset_sequence_number():
    try:
        if os.path.exists(SEQUENCE_NUMBER_FILE):
            os.remove(SEQUENCE_NUMBER_FILE)
            logger.info("Sequence number file reset")
        return {"status": "Sequence number reset successfully"}
    except Exception as e:
        logger.error(f"Error resetting sequence number: {str(e)}")
        return {"error": str(e)}

# Define the input schema
class PredictionInput(BaseModel):
    hour: int
    temperature: float
    humidity: float
    wind_speed: float
    visibility: float
    solar_radiation: float
    rainfall: float
    snowfall: float
    weekend: int
    holiday_status: int

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "FastAPI server is running"}

# Reset sequence number endpoint (for debugging)
@app.get("/reset-sequence")
async def reset_sequence():
    return reset_sequence_number()

# Test endpoint to send data directly to Kinesis
@app.get("/test-kinesis")
async def test_kinesis(background_tasks: BackgroundTasks, temp: float = 20.5):
    try:
        # Create test data similar to what data_ingestion.py would send
        test_data = {
            "hour": datetime.datetime.now().hour,
            "temperature": temp,  # Use parameter if provided, otherwise default
            "humidity": 65,
            "wind_speed": 3.2,
            "visibility": 10000,
            "solar_radiation": 0.8,
            "rainfall": 0,
            "snowfall": 0,
            "weekend": 0,
            "holiday_status": 0,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "test_marker": f"test-{datetime.datetime.now().strftime('%H%M%S')}"  # Add a unique marker
        }
        
        # Send to Kinesis in the background
        background_tasks.add_task(send_test_data_to_kinesis, test_data)
        
        return {"status": "Test data sent to Kinesis", "data": test_data}
    except Exception as e:
        logger.error(f"Error sending test data: {str(e)}")
        return {"error": str(e)}

# Function to send test data to Kinesis
def send_test_data_to_kinesis(data):
    try:
        # Convert to JSON
        payload = json.dumps(data)
        logger.info(f"Test JSON payload: {payload}")
        
        # Don't base64 encode for simplicity
        encoded_payload = payload.encode("utf-8")
        logger.info(f"Test UTF-8 encoded payload: {encoded_payload}")
        
        # Send to Kinesis
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=encoded_payload,
            PartitionKey="test-data"
        )
        logger.info(f"Test data sent to Kinesis. Response: {response}")
    except Exception as e:
        logger.error(f"Error in background task: {str(e)}")

# Direct prediction endpoint (bypassing Kinesis)
@app.get("/direct-prediction")
async def direct_prediction():
    """Make a prediction with hard-coded test data, bypassing Kinesis."""
    try:
        # Create test data similar to what we'd expect from Kinesis
        test_data = {
            "hour": datetime.datetime.now().hour,
            "temperature": 20.5,
            "humidity": 65,
            "wind_speed": 3.2,
            "visibility": 10000,
            "solar_radiation": 0.8,
            "rainfall": 0,
            "snowfall": 0,
            "weekend": 0,
            "holiday_status": 0,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.info("Making direct prediction with test data")
        logger.info(f"Test data: {test_data}")
        
        # Create a copy of data without timestamp for prediction
        prediction_data = test_data.copy()
        if "timestamp" in prediction_data:
            del prediction_data["timestamp"]  # Remove timestamp before creating DataFrame
        
        # Prepare DataFrame with the CORRECT column names (using mapping)
        df = pd.DataFrame([prediction_data])
        
        # Rename columns to match what the model expects
        df = df.rename(columns=COLUMN_MAPPING)
        logger.info(f"Renamed columns DataFrame: {df.columns.tolist()}")
        
        # Scale features
        try:
            scaled_features = scaler.transform(df)
            logger.info(f"Scaled features successfully")
            
            # Make prediction
            try:
                prediction = model.predict(scaled_features)[0]
                logger.info(f"Prediction successful: {int(prediction)}")
                
                return {
                    "success": True,
                    "predicted_bike_count": int(prediction),
                    "features": test_data
                }
            except Exception as pred_error:
                logger.error(f"Error during prediction: {str(pred_error)}")
                import traceback
                logger.error(traceback.format_exc())
                
                return {
                    "success": False,
                    "error": "Prediction failed",
                    "details": str(pred_error),
                    "stage": "prediction"
                }
                
        except Exception as scale_error:
            logger.error(f"Error during feature scaling: {str(scale_error)}")
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "success": False,
                "error": "Feature scaling failed",
                "details": str(scale_error),
                "stage": "scaling"
            }
            
    except Exception as e:
        logger.error(f"Unexpected error in direct prediction: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            "success": False,
            "error": "Unexpected error",
            "details": str(e)
        }

# Check Kinesis endpoint for debugging
@app.get("/check-kinesis")
async def check_kinesis():
    """Debug endpoint to check Kinesis stream configuration."""
    try:
        # Get stream info
        response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
        stream_desc = response["StreamDescription"]
        
        # Get all shards
        shards = stream_desc["Shards"]
        
        results = {
            "stream_name": STREAM_NAME,
            "stream_status": stream_desc["StreamStatus"],
            "shard_count": len(shards),
            "shards": [],
            "latest_records": []
        }
        
        # Check each shard
        for shard in shards:
            shard_id = shard["ShardId"]
            shard_info = {
                "shard_id": shard_id,
                "starting_sequence_number": shard.get("SequenceNumberRange", {}).get("StartingSequenceNumber", "unknown"),
                "ending_sequence_number": shard.get("SequenceNumberRange", {}).get("EndingSequenceNumber", "open")
            }
            results["shards"].append(shard_info)
            
            # Get latest record from this shard using TRIM_HORIZON
            try:
                # Get records from TRIM_HORIZON
                shard_iterator = kinesis_client.get_shard_iterator(
                    StreamName=STREAM_NAME,
                    ShardId=shard_id,
                    ShardIteratorType="TRIM_HORIZON"
                )["ShardIterator"]
                
                # Get records
                records_response = kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                
                # If records exist, get the most recent one
                if records_response["Records"]:
                    # Sort by arrival timestamp
                    sorted_records = sorted(
                        records_response["Records"], 
                        key=lambda r: r["ApproximateArrivalTimestamp"],
                        reverse=True  # Most recent first
                    )
                    
                    # Get the most recent record
                    latest_record = sorted_records[0]
                    record_info = {
                        "shard_id": shard_id,
                        "sequence_number": latest_record["SequenceNumber"],
                        "arrival_time": latest_record["ApproximateArrivalTimestamp"].isoformat(),
                        "partition_key": latest_record["PartitionKey"]
                    }
                    
                    # Try to decode the data
                    try:
                        raw_data = latest_record["Data"]
                        decoded_data = raw_data.decode("utf-8")
                        data_json = json.loads(decoded_data)
                        record_info["temperature"] = data_json.get("temperature", "unknown")
                        record_info["timestamp"] = data_json.get("timestamp", "unknown")
                    except Exception as decode_error:
                        record_info["decode_error"] = str(decode_error)
                    
                    results["latest_records"].append(record_info)
                else:
                    results["latest_records"].append({
                        "shard_id": shard_id,
                        "error": "No records found in this shard"
                    })
            except Exception as shard_error:
                results["latest_records"].append({
                    "shard_id": shard_id,
                    "error": str(shard_error)
                })
        
        return results
    except Exception as e:
        logger.error(f"Error checking Kinesis: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": str(e)}

# Fetch the latest prediction from Kinesis - FIXED to always get the most recent data
@app.get("/fetch-prediction")
async def fetch_latest_prediction():
    try:
        logger.info("--------------------------------")
        logger.info("Starting fetch-prediction process")
        logger.info("--------------------------------")
        
        # Get the shard ID
        response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        logger.info(f"Found shard ID: {shard_id}")

        # Use TRIM_HORIZON to get ALL records
        logger.info("Using TRIM_HORIZON to get all records")
        shard_iterator = get_new_shard_iterator(shard_id, "TRIM_HORIZON")
        
        # Fetch records from the stream (up to 100)
        records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
        logger.info(f"Retrieved {len(records_response['Records'])} records from stream")
        
        if not records_response['Records']:
            logger.warning("No records found in Kinesis stream")
            return {"message": "No records found in Kinesis stream"}
            
        # Sort records by arrival timestamp to find the most recent one
        sorted_records = sorted(
            records_response['Records'], 
            key=lambda r: r['ApproximateArrivalTimestamp'], 
            reverse=True  # Most recent first
        )
        
        # Get the most recent record
        record = sorted_records[0]
        arrival_time = record['ApproximateArrivalTimestamp'].isoformat()
        logger.info(f"Using most recent record with arrival time: {arrival_time}")
        
        # Get the sequence number
        sequence_number = record['SequenceNumber']
        logger.info(f"Processing record with sequence number: {sequence_number}")

        # Decode the data
        raw_data = record['Data']
        logger.info(f"Raw data type: {type(raw_data)}")
        
        try:
            # First try direct decoding as UTF-8
            if isinstance(raw_data, bytes):
                try:
                    direct_decode = raw_data.decode('utf-8')
                    logger.info("Successfully decoded data directly as UTF-8")
                    payload = direct_decode
                except UnicodeDecodeError:
                    # If direct decoding fails, try base64 decoding
                    logger.warning("Direct UTF-8 decoding failed, trying base64")
                    try:
                        # Fix padding if needed
                        if len(raw_data) % 4 != 0:
                            missing_padding = 4 - (len(raw_data) % 4)
                            fixed_data = raw_data + (b"=" * missing_padding)
                        else:
                            fixed_data = raw_data
                            
                        base64_decode = base64.b64decode(fixed_data)
                        payload = base64_decode.decode('utf-8')
                        logger.info("Successfully decoded data with base64")
                    except Exception as b64_error:
                        logger.error(f"Base64 decoding failed: {str(b64_error)}")
                        return {"message": "Error decoding data from Kinesis"}
            else:
                # Not bytes, just use as is
                payload = raw_data
                logger.info("Data wasn't bytes, using as is")
                
            # Parse the JSON data
            data = json.loads(payload)
            logger.info(f"Successfully parsed JSON data: {data}")
            
            # Store timestamp or create one if it doesn't exist
            now = datetime.datetime.now()
            if "timestamp" not in data:
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                data["timestamp"] = timestamp
                logger.info("Added timestamp to data")
            else:
                timestamp = data["timestamp"]
            
            # Create a copy of data without timestamp for prediction
            prediction_data = data.copy()
            if "timestamp" in prediction_data:
                del prediction_data["timestamp"]  # Remove timestamp before creating DataFrame
            if "timezone" in prediction_data:
                del prediction_data["timezone"]  # Remove timezone before creating DataFrame
            if "test_marker" in prediction_data:
                del prediction_data["test_marker"]  # Remove test marker if present
            
            # Create DataFrame with the proper column names
            df = pd.DataFrame([prediction_data])
            
            # Rename columns to match what the retrained model expects
            df = df.rename(columns=COLUMN_MAPPING)
            logger.info(f"Renamed columns DataFrame: {df.columns.tolist()}")
            
            # Scale features
            scaled_features = scaler.transform(df)
            logger.info("Features scaled successfully")

            # Predict bike rental count
            prediction = model.predict(scaled_features)[0]
            logger.info(f"Predicted bike count: {int(prediction)}")

            # Return the full response with additional metadata
            return {
                "predicted_bike_count": int(prediction),
                "features": data,  # Include original data with timestamp
                "sequence_number": sequence_number,
                "timestamp": timestamp,
                "arrival_time": arrival_time
            }
        
        except Exception as process_error:
            logger.error(f"Error processing record data: {str(process_error)}")
            import traceback
            logger.error(traceback.format_exc())
            return {"message": f"Error processing data: {str(process_error)}"}
            
    except Exception as e:
        logger.error(f"Error in fetch-prediction: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": str(e)}