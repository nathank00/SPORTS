import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import VarianceThreshold
import xgboost as xgb
import joblib


# Load the dataset
df = pd.read_csv('../model/masterdata.csv')

#Remove first 250 rows (because many custom stat values are empty)
df = df.tail(len(df)-255)

# Convert 'game_date' column to datetime objects
df['game_date'] = pd.to_datetime(df['game_date'])

# Get today's date
today = datetime.today().strftime('%Y-%m-%d')

# Filter out rows with today's date
df = df[df['game_date'] != today]

# Drop the columns containing 'Name', 'ID', or '_P_'
columns_to_drop = [col for col in df.columns if 'Name' in col or 'ID' in col or '_P_' in col or 'bbrefID' in col]
df = df.drop(columns=columns_to_drop)

# Drop rows with missing values in the target variable
df = df.dropna(subset=['over_under_runline'])

# Define features and target variable
X = df.drop(columns=['over_under_target', 'runs_total', 'game_date', 'runs_home', 'runs_away', 'game_id', 'home_name', 'away_name'])
y = df['over_under_target']

# You can now proceed to train your model using X and y
# Example: Train a RandomForestClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)



# Define a pipeline with imputer, scaler, and model
pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='mean')),
    ('var_thresh', VarianceThreshold(threshold=0.1)),  # Remove low-variance features
    ('scaler', StandardScaler()),
    ('model', xgb.XGBClassifier(random_state=42, use_label_encoder=False, eval_metric='logloss'))
])

# Define a parameter grid for XGBoost
param_grid1 = {
    'model__n_estimators': [50, 100, 200],
    'model__learning_rate': [0.01, 0.1],
    'model__max_depth': [3, 5, 9],
    'model__subsample': [0.8, 1.0],
    'model__colsample_bytree': [0.8, 1.0],
    'model__gamma': [0, 0.1],
    'model__min_child_weight': [1, 3, 7]
}

param_grid2 = {
    'model__n_estimators': [25, 75, 150, 250],
    'model__learning_rate': [0.05, 0.15],
    'model__max_depth': [6, 12, 15],
    'model__subsample': [0.25, 0.5, 0.9],
    'model__colsample_bytree': [0.2, 0.4, 0.6],
    'model__gamma': [0.05, 0.2, 0.3],
    'model__min_child_weight': [4, 8, 12]
}

# Grid search for hyperparameter tuning
grid_search = GridSearchCV(pipeline, param_grid1, cv=StratifiedKFold(n_splits=5), scoring='accuracy', n_jobs=-1, verbose=1)

# Fit the model on the training data
grid_search.fit(X_train, y_train)

# Best model from grid search
best_model = grid_search.best_estimator_

# Make predictions on the test set
y_pred = best_model.predict(X_test)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
conf_matrix = confusion_matrix(y_test, y_pred)
class_report = classification_report(y_test, y_pred)

print(f'Accuracy: {accuracy}')
print('Confusion Matrix:\n', conf_matrix)
print('Classification Report:\n', class_report)

# Save the best model
joblib.dump(best_model, 'model/xgb_model.pkl')