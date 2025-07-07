# 🚴‍♂️ Bike Rental Demand Prediction

> 🎯 **Smart inventory optimization through predictive analytics** - Forecasting bike rental demand using machine learning and real-time weather data integration.

## 📋 Table of Contents
- [🎯 Problem Statement](#-problem-statement)
- [🔧 Solution Overview](#-solution-overview)
- [📊 Dataset](#-dataset)
- [🚀 Quick Start](#-quick-start)
- [🔍 Methodology](#-methodology)
- [📈 Model Performance](#-model-performance)
- [🏗️ Architecture](#️-architecture)
- [💡 Key Features](#-key-features)
- [📱 Dashboard](#-dashboard)
- [🔮 Future Enhancements](#-future-enhancements)
- [👨‍💻 Author](#-author)

## 🎯 Problem Statement

A bike rental company faces significant challenges in inventory management:
- **Stockouts** during peak demand periods leading to lost revenue
- **Excess inventory** during off-seasons increasing storage costs
- **Poor resource planning** affecting operational efficiency
- **Customer dissatisfaction** due to unavailable bikes

**Solution**: Develop an intelligent forecasting system that predicts bike rental demand based on weather conditions and temporal patterns.

## 🔧 Solution Overview

This project delivers a comprehensive machine learning solution that:
- 🔍 Analyzes historical rental patterns and weather correlations
- 🤖 Trains multiple ML models to predict demand with 90% accuracy
- 🌤️ Integrates real-time weather data via OpenWeatherAPI
- 📊 Provides interactive dashboards for business insights
- ⚡ Serves predictions through a scalable FastAPI deployment

## 📊 Dataset

**Source**: Kaggle Bike Rental Dataset
- 📈 **Records**: 8,760 observations
- 🏷️ **Features**: 14 variables including:
  - `temperature` - Ambient temperature (°C)
  - `windspeed` - Wind speed (m/s)
  - `hour` - Hour of the day (0-23)
  - `date` - Date of observation
  - `solar_radiation` - Solar radiation levels
  - `bike rental count` - Rental bike count

## 🚀 Quick Start

### Prerequisites
```bash
Python 3.8+
pip install -r requirements.txt
```

### Installation
```bash
# Clone the repository
git clone https://github.com/kumarbaibhav6/BikeSharingDemandPrediction
cd bike-rental-prediction

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Add your OpenWeatherAPI key to .env
```

### Running the Application
```bash
# Start the FastAPI server
uvicorn main:app --reload

# Launch Streamlit dashboard
streamlit run dashboard.py

```

## 🔍 Methodology

### 1. Data Preprocessing & Feature Engineering
- ✅ **Data Cleaning**: Handled missing values and outliers
- 🔧 **Feature Creation**: 
  - Extracted `month`, `day`, `season` from datetime
  - Created cyclical features for temporal patterns
  - Generated interaction features between weather variables

### 2. Exploratory Data Analysis
- 📊 **Temporal Patterns**: Identified peak hours (6-8 AM, 4-6 PM)
- 🌦️ **Weather Impact**: Strong correlation between temperature and rentals
- 📅 **Seasonal Trends**: Higher demand in spring/summer months
- 🔍 **Hypothesis Testing**: Validated weekend vs weekday patterns

### 3. Model Development
Implemented and compared multiple algorithms:
- **Linear Regression**: Baseline model
- **Lasso/Ridge**: Regularized linear models
- **Decision Tree**: Non-linear relationships
- **Random Forest**: Ensemble method
- **Gradient Boosting**: Advanced ensemble
- **KNN Regressor**: Instance-based learning

### 4. Model Evaluation
- **Cross-validation**: 5-fold CV for robust evaluation
- **Metrics**: R² Score, RMSE, MAE
- **Feature Importance**: Identified key predictors
- **Hyperparameter Tuning**: Grid search optimization

## 📈 Model Performance

| Model | R² Score | RMSE | Status |
|-------|----------|------|--------|
| **Random Forest** | **90%** | **193.98** | ✅ **Selected** |
| Gradient Boosting | 85% | 243.12 | ⚪ |
| KNN Regressor | 82% | 265.36 | ⚪ |
| Decision Tree | 81% | 270.96 | ⚪ |
| Linear Regression | 53% | 427.54 | ⚪ |

### 🎯 Key Insights
- **Top Predictors**: Hour of day, temperature, humidity
- **Peak Demand**: 6-8 AM and 4-6 PM (commute hours)
- **Weather Impact**: Temperature is the top predictor with a variable importance score of 0.38.
- **Seasonal Effect**: Summer is the peak season with around 70% more demand compared to all other seasons combined.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  OpenWeatherAPI │───▶│   AWS Kinesis    │───▶│  ML Pipeline    │
│                 │    │  (Data Stream)   │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                          │
                                                          ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Streamlit       │◀───│    FastAPI       │◀───│ Random Forest   │
│ Dashboard       │    │   (REST API)     │    │     Model       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 💡 Key Features

### 🌤️ Real-time Weather Integration
- Live weather data ingestion every 15 minutes
- Automated data validation and preprocessing
- Fault-tolerant streaming pipeline

### 📊 Interactive Dashboard
- Real-time demand predictions
- Weather condition visualization
- Historical trend analysis
- Business KPI monitoring

## 📱 Dashboard

The Streamlit dashboard provides:
- 🌡️ **Weather Metrics**: Temperature, wind speed, humidity
- 📈 **Demand Forecast**: Hourly predictions for next 24 hours
- 📊 **Historical Trends**: Monthly and seasonal patterns
- 🎯 **Business Insights**: Inventory recommendations

## 🔮 Future Enhancements

- [ ] **Deep Learning Models**: LSTM/GRU for time series forecasting
- [ ] **Multi-city Support**: Expand to multiple locations
- [ ] **Event Integration**: Include local events and holidays

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Kumar Baibhav**
- 🔗 [LinkedIn](https://www.linkedin.com/in/kumarbaibhav66/)
- 📧 [Email](mailto:baibhav06june6@gmail.com)
- 🐙 [GitHub](https://github.com/kumarbaibhav6)

---

⭐ **If you found this project helpful, please consider giving it a star!** ⭐

*Built with ❤️ for smarter bike rental management*