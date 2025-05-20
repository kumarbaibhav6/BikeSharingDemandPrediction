from fastapi import FastAPI, BackgroundTasks
import pandas as pd
import boto3
import json
import base64
import joblib
import logging
import os
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

# Define the correct column mapping - UPDATED with corrected names
COLUMN_MAPPING = {
    "hour": "Hour",
    "temperature": "Temperature(°C)",
    "humidity": "Humidity(%)",
    "wind_speed": "Wind speed (m/s)",  # Note the space instead of underscore
    "visibility": "Visibility (10m)",   # Note the space
    "solar_radiation": "Solar Radiation (MJ/m2)",  # Note the space
    "rainfall": "Rainfall(mm)",
    "snowfall": "Snowfall (cm)",
    "weekend": "Weekend",  # Not Weekend_Status
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
def get_new_shard_iterator(shard_id, iterator_type="LATEST"):
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

# Inspect model endpoint
@app.get("/inspect-model")
async def inspect_model():
    """Endpoint to inspect the feature names expected by the model and scaler."""
    try:
        # Get the feature names from the scaler
        feature_names = None
        try:
            # Different ways to extract feature names depending on scaler type
            if hasattr(scaler, 'feature_names_in_'):
                feature_names = scaler.feature_names_in_.tolist()
            elif hasattr(scaler, 'get_feature_names_out'):
                feature_names = scaler.get_feature_names_out().tolist()
            elif hasattr(scaler, 'get_feature_names'):
                feature_names = scaler.get_feature_names().tolist()
            
            logger.info(f"Feature names from scaler: {feature_names}")
        except Exception as e:
            logger.error(f"Error getting feature names from scaler: {str(e)}")
        
        # Try to load data frame with empty values to see what columns it expects
        try:
            # Create an empty DataFrame with expected columns
            if feature_names:
                empty_df = pd.DataFrame(columns=feature_names)
                empty_row = {col: 0 for col in feature_names}
                empty_df = pd.DataFrame([empty_row])
                
                # Try to transform it
                scaled = scaler.transform(empty_df)
                logger.info("Empty dataframe successfully transformed")
                
                return {
                    "success": True,
                    "feature_names": feature_names,
                    "num_features": len(feature_names)
                }
            else:
                return {
                    "success": False,
                    "error": "Could not determine feature names from scaler"
                }
        except Exception as df_error:
            logger.error(f"Error testing empty DataFrame: {str(df_error)}")
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "success": False,
                "error": str(df_error),
                "traceback": traceback.format_exc()
            }
            
    except Exception as e:
        logger.error(f"General error in inspect-model: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            "success": False,
            "error": str(e)
        }

# Test endpoint to send data directly to Kinesis
@app.get("/test-kinesis")
async def test_kinesis(background_tasks: BackgroundTasks):
    try:
        # Create test data similar to what data_ingestion.py would send
        test_data = {
            "hour": 12,
            "temperature": 20.5,
            "humidity": 65,
            "wind_speed": 3.2,
            "visibility": 10000,
            "solar_radiation": 0.8,
            "rainfall": 0,
            "snowfall": 0,
            "weekend": 0,
            "holiday_status": 0
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
            "hour": 12,
            "temperature": 20.5,
            "humidity": 65,
            "wind_speed": 3.2,
            "visibility": 10000,
            "solar_radiation": 0.8,
            "rainfall": 0,
            "snowfall": 0,
            "weekend": 0,
            "holiday_status": 0
        }
        
        logger.info("Making direct prediction with test data")
        logger.info(f"Test data: {test_data}")
        
        # Prepare DataFrame with the CORRECT column names (using mapping)
        df = pd.DataFrame([test_data])
        
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

# Fetch the latest prediction from Kinesis
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

        # Log all shards for verification
        all_shards = response['StreamDescription']['Shards']
        logger.info(f"All shards in stream: {[s['ShardId'] for s in all_shards]}")
        
        # Get the last processed sequence number
        last_sequence_number = get_last_sequence_number()
        logger.info(f"Last sequence number: {last_sequence_number}")
        
        # Get the shard iterator, handle expired iterator
        try:
            if last_sequence_number:
                logger.info("Using AFTER_SEQUENCE_NUMBER iterator for continuity")
                shard_iterator = kinesis_client.get_shard_iterator(
                    StreamName=STREAM_NAME,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=last_sequence_number
                )['ShardIterator']
            else:
                logger.info("Using TRIM_HORIZON iterator to get oldest records")
                shard_iterator = get_new_shard_iterator(shard_id, "TRIM_HORIZON")
        except kinesis_client.exceptions.ExpiredIteratorException:
            logger.warning("Shard iterator expired. Regenerating with TRIM_HORIZON.")
            shard_iterator = get_new_shard_iterator(shard_id, "TRIM_HORIZON")

        # Fetch records from the stream
        records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
        logger.info(f"Retrieved {len(records_response['Records'])} records from Kinesis")

        # Handle no records scenario
        if not records_response['Records']:
            logger.info("No records found with initial iterator, trying TRIM_HORIZON")
            shard_iterator = get_new_shard_iterator(shard_id, "TRIM_HORIZON")
            records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
            logger.info(f"Retrieved {len(records_response['Records'])} records with TRIM_HORIZON")
            
            if not records_response['Records']:
                logger.warning("No records found in Kinesis stream")
                return {"message": "No records found in Kinesis stream"}

        for record in records_response['Records']:
            try:
                # Save the sequence number for next iteration
                sequence_number = record['SequenceNumber']
                save_last_sequence_number(sequence_number)
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
                                continue
                    else:
                        # Not bytes, just use as is
                        payload = raw_data
                        logger.info("Data wasn't bytes, using as is")
                        
                    # Parse the JSON data
                    data = json.loads(payload)
                    logger.info(f"Successfully parsed JSON data: {data}")
                    
                    # Create DataFrame with the proper column names
                    df = pd.DataFrame([data])
                    
                    # Rename columns to match what the model expects
                    df = df.rename(columns=COLUMN_MAPPING)
                    logger.info(f"Renamed columns DataFrame: {df.columns.tolist()}")
                    
                    # Scale features
                    scaled_features = scaler.transform(df)
                    logger.info("Features scaled successfully")

                    # Predict bike rental count
                    prediction = model.predict(scaled_features)[0]
                    logger.info(f"Predicted bike count: {int(prediction)}")

                    return {
                        "predicted_bike_count": int(prediction),
                        "features": data,
                        "sequence_number": sequence_number
                    }
                
                except Exception as process_error:
                    logger.error(f"Error processing record data: {str(process_error)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue

            except Exception as record_error:
                logger.error(f"Error processing record: {str(record_error)}")
                import traceback
                logger.error(traceback.format_exc())
                continue

        logger.info("No valid prediction could be made from available records")
        return {"message": "Processed all available records but couldn't make a valid prediction"}
        
    except Exception as e:
        logger.error(f"Error in fetch-prediction: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": str(e)}