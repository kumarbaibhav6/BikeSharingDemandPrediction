import dashboard as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import datetime
import json
import os

# Set page configuration
st.set_page_config(
    page_title="Bike Sharing Demand Dashboard",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dashboard title and description
st.title("🚲 Real-time Bike Sharing Demand Dashboard")
st.markdown("""
This dashboard shows real-time predictions of bike rental counts based on current weather conditions in Seoul.
The data is collected every 15 minutes and processed through a machine learning model.
""")

# Constants
API_URL = "http://localhost:8000"  # Change this to your API's URL in production
REFRESH_INTERVAL = 60  # Refresh data every 60 seconds by default

# Create or load history file
HISTORY_FILE = "prediction_history.json"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []

def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f)

# Initialize session state
if 'prediction_history' not in st.session_state:
    st.session_state.prediction_history = load_history()
    
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = None

if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True

# Function to fetch prediction from API
def fetch_prediction():
    try:
        response = requests.get(f"{API_URL}/fetch-prediction")
        if response.status_code == 200:
            data = response.json()
            if "predicted_bike_count" in data:
                # Add timestamp to the data
                data["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return data
            else:
                st.error(f"No prediction data available: {data.get('message', 'Unknown error')}")
        else:
            st.error(f"Error fetching prediction: {response.status_code}")
    except Exception as e:
        st.error(f"Connection error: {e}")
    return None

# Sidebar controls
with st.sidebar:
    st.header("Dashboard Controls")
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("Auto-refresh data", value=st.session_state.auto_refresh)
    st.session_state.auto_refresh = auto_refresh
    
    # Refresh interval slider
    if auto_refresh:
        refresh_seconds = st.slider("Refresh interval (seconds)", 
                                   min_value=10, 
                                   max_value=300, 
                                   value=REFRESH_INTERVAL,
                                   step=10)
    else:
        refresh_seconds = REFRESH_INTERVAL
    
    # Manual refresh button
    if st.button("Refresh Now"):
        with st.spinner("Fetching latest prediction..."):
            new_prediction = fetch_prediction()
            if new_prediction:
                st.session_state.prediction_history.append(new_prediction)
                save_history(st.session_state.prediction_history)
                st.session_state.last_update_time = datetime.datetime.now()
                st.success("Data refreshed!")
    
    # History controls
    st.header("History Settings")
    max_history = st.slider("Maximum history points", 
                           min_value=10, 
                           max_value=500, 
                           value=100)
    
    # Trim history if needed
    if len(st.session_state.prediction_history) > max_history:
        st.session_state.prediction_history = st.session_state.prediction_history[-max_history:]
        save_history(st.session_state.prediction_history)
    
    # Clear history button
    if st.button("Clear History"):
        st.session_state.prediction_history = []
        save_history([])
        st.success("History cleared!")
    
    # Show API connection details
    st.header("API Connection")
    api_url = st.text_input("API URL", value=API_URL)
    API_URL = api_url

# Auto-refresh logic
if st.session_state.auto_refresh:
    # Check if it's time to refresh
    current_time = datetime.datetime.now()
    if (st.session_state.last_update_time is None or 
        (current_time - st.session_state.last_update_time).seconds >= refresh_seconds):
        
        with st.spinner("Auto-refreshing data..."):
            new_prediction = fetch_prediction()
            if new_prediction:
                st.session_state.prediction_history.append(new_prediction)
                save_history(st.session_state.prediction_history)
                st.session_state.last_update_time = current_time

# Last update info
if st.session_state.last_update_time:
    st.info(f"Last updated: {st.session_state.last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh countdown
    if st.session_state.auto_refresh:
        seconds_since_update = (datetime.datetime.now() - st.session_state.last_update_time).seconds
        seconds_until_refresh = max(0, refresh_seconds - seconds_since_update)
        st.caption(f"Next refresh in {seconds_until_refresh} seconds")

# Main content
col1, col2 = st.columns([2, 1])

# Current prediction display
with col2:
    st.subheader("Current Prediction")
    
    if st.session_state.prediction_history:
        current_data = st.session_state.prediction_history[-1]
        
        # Create metrics for current prediction
        pred_count = current_data.get("predicted_bike_count", 0)
        
        # Create a gauge chart for the prediction
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pred_count,
            title={'text': "Predicted Bike Count"},
            gauge={'axis': {'range': [0, 5000]},
                   'bar': {'color': "royalblue"},
                   'steps': [
                       {'range': [0, 1000], 'color': "lightgray"},
                       {'range': [1000, 2500], 'color': "gray"},
                       {'range': [2500, 5000], 'color': "darkgray"}],
                   'threshold': {
                       'line': {'color': "red", 'width': 4},
                       'thickness': 0.75,
                       'value': pred_count}}))
        
        fig.update_layout(height=300, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, use_container_width=True)
        
        # Weather metrics
        st.subheader("Current Weather Conditions")
        weather_cols = st.columns(2)
        
        with weather_cols[0]:
            st.metric("Temperature (°C)", f"{current_data.get('features', {}).get('temperature', 'N/A')}")
            st.metric("Humidity (%)", f"{current_data.get('features', {}).get('humidity', 'N/A')}")
            st.metric("Wind Speed (m/s)", f"{current_data.get('features', {}).get('wind_speed', 'N/A')}")
            st.metric("Visibility (m)", f"{current_data.get('features', {}).get('visibility', 'N/A')}")
        
        with weather_cols[1]:
            st.metric("Solar Radiation", f"{current_data.get('features', {}).get('solar_radiation', 'N/A')}")
            st.metric("Rainfall (mm)", f"{current_data.get('features', {}).get('rainfall', 'N/A')}")
            st.metric("Snowfall (cm)", f"{current_data.get('features', {}).get('snowfall', 'N/A')}")
            
            # Weekend/Holiday status
            weekend = current_data.get('features', {}).get('weekend', 0)
            holiday = current_data.get('features', {}).get('holiday_status', 0)
            
            weekend_text = "Yes" if weekend == 1 else "No"
            holiday_text = "Yes" if holiday == 1 else "No"
            
            st.metric("Weekend", weekend_text)
            st.metric("Holiday", holiday_text)
            
    else:
        st.warning("No prediction data available. Click 'Refresh Now' to fetch data.")

# Historical data and charts
with col1:
    st.subheader("Prediction History")
    
    if st.session_state.prediction_history:
        # Convert history to DataFrame
        history_data = []
        for item in st.session_state.prediction_history:
            if "timestamp" in item and "predicted_bike_count" in item:
                entry = {
                    "timestamp": item["timestamp"],
                    "predicted_bike_count": item["predicted_bike_count"]
                }
                
                # Add feature data if available
                if "features" in item:
                    for key, value in item["features"].items():
                        entry[key] = value
                
                history_data.append(entry)
        
        df = pd.DataFrame(history_data)
        
        # Convert timestamp to datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Line chart for prediction history
        fig = px.line(df, x="timestamp", y="predicted_bike_count", 
                     title="Bike Sharing Demand Prediction History",
                     labels={"timestamp": "Time", "predicted_bike_count": "Predicted Bike Count"})
        
        fig.update_layout(height=400, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
        
        # Weather condition correlation charts
        st.subheader("Weather Conditions vs. Predictions")
        
        # Feature selection
        weather_features = ["temperature", "humidity", "wind_speed", "visibility", 
                           "solar_radiation", "rainfall", "snowfall"]
        
        weather_feature = st.selectbox("Select weather feature to compare with predictions", 
                                     options=weather_features)
        
        if weather_feature in df.columns:
            # Create scatter plot
            fig = px.scatter(df, x=weather_feature, y="predicted_bike_count",
                           title=f"Relationship between {weather_feature} and Bike Demand",
                           trendline="ols",  # Add trend line
                           labels={weather_feature: weather_feature.capitalize(), 
                                  "predicted_bike_count": "Predicted Bike Count"})
            
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)
        
        # Display raw data table if requested
        if st.checkbox("Show raw prediction data"):
            st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True)
    else:
        st.info("No historical data available yet. Predictions will appear here as they are collected.")

# Footer
st.markdown("---")
st.caption("Bike Sharing Demand Prediction Dashboard | Powered by Streamlit and FastAPI")