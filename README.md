# F1 Podium Prediction

A Python machine learning project that estimates each Formula 1 driver's probability of finishing on the podium. It combines historical race results with qualifying data, builds features from driver, team, and circuit performance, and generates predictions using a model registered in MLflow. FastF1 supplies race data, PostgreSQL stores results and predictions, and Prefect orchestrates the workflows.

## Goals

- Estimate drivers' chances of a podium finish ahead of upcoming races.
- Automate race data ingestion, feature preparation, and prediction scheduling.
- Keep historical data and predictions available for analysis by driver, team, circuit, and season.
- Maintain modular, testable code so data sources, features, and models can evolve independently.

## In Development

- Update dashboard to display results and allow users to explore results and data
- Data drift detection
- Automatic model retraining and logging
- Dataset logging
- Add new models for finishing place prediction, qualifying prediction, and live overtake prediction
