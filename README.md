# Bike Rental Demand Prediction
Predict rental bike counts based on historical data using machine learning algorithms.

---

## Problem Statement

A bike rental company aims to optimize its inventory by accurately forecasting bike rental demand based on weather conditions. By predicting the number of bikes needed in advance, the company can minimize stockouts during peak periods and reduce excess inventory during off-seasons, leading to better resource planning, improved customer satisfaction, and cost efficiency.

---

## Approach

- Cleaned and explored bike rental dataset.
- Created features such as month, day, time of day and season to understand demand patterns and trends.
- Conducted hypothesis testing to understand rental trends based on time of day, seasons, weekday and weekends.
- Trained Linear Regression, Lasso, Ridge, Decision Tree and Random Forest models.
- Evaluated models using R2 score and Mean Squared Error (MSE).
- Performed hyperparameter tuning on the best performing model.
- Developed a real-time prediction pipeline that ingests live weather data at regular intervals from the OpenWeatherAPI. AWS Kinesis was used to stream the weather data, which was then processed by a machine learning model deployed via FastAPI to serve on-demand rental demand predictions.
- Developed a streamlit dashboard to display weather data such as temperature, wind speed, solar radiation and rental bike count needed.

---

## Dataset

- Source: Kaggle Loan Default Dataset
- 8,760 records, 14 features including temperature, windspeed, date, hour (time of day).

---

## Results

| Model             | R2 Score | RMSE
|-------------------|----------|----------|
| KNN Regressor     | 82%      | 265.36   |
| Random Forest     | 90%      | 193.98   |
| GB Regressor.     | 85%.     | 243.12.  |

---

## Conclusion

The Random Forest model was selected for hyperparameter tuning and deployment due to its superior performance in terms of R² score and RMSE, as well as its interpretability, which provided valuable insights into the key factors influencing bike rental demand.

Features such as hour (time of day) and temperature were the ones having most impact on rental bike counts as observed from the feature importance plot.

---

## Author

Kumar Baibhav — [LinkedIn](https://linkedin.com/in/yourprofile)
