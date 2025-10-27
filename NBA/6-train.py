import pandas as pd
import numpy as np
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SelectFromModel
import joblib
import logging
import os

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(level=logging.INFO, filename='logs/xgboost_training.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

# Load data
try:
    df = pd.read_parquet('customgamelogs.parquet')
    logging.info(f"Loaded {len(df)} games from customgamelogs.parquet")
except FileNotFoundError:
    logging.error("customgamelogs.parquet not found")
    raise FileNotFoundError("customgamelogs.parquet not found")

# Ensure chronological order
if 'game_date' in df.columns:
    df = df.sort_values('game_date')
else:
    logging.warning("game_date column missing, assuming data is sorted")

# Select features: all home/away rolling stats (_10 and _50)
features = [col for col in df.columns if (col.startswith('home_') or col.startswith('away_')) and (col.endswith('_10') or col.endswith('_50'))]
if not features:
    logging.error("No rolling stat features found in customgamelogs.parquet")
    raise ValueError("No rolling stat features found in customgamelogs.parquet")

# Target: winner
if 'winner' not in df.columns:
    logging.error("winner column missing in customgamelogs.parquet")
    raise ValueError("winner column missing in customgamelogs.parquet")

# Select features and target
X = df[features]
y = df['winner']

# Drop rows with any NaN in features or target
original_len = len(X)
X_y = pd.concat([X, y], axis=1)
X_y = X_y.dropna(subset=features + ['winner'])
if len(X_y) < original_len:
    dropped_proportion = (original_len - len(X_y)) / original_len
    logging.warning(f"Dropped {original_len - len(X_y)} rows ({dropped_proportion:.2%}) with NaN values in features or target")
    print(f"Warning: Dropped {original_len - len(X_y)} rows ({dropped_proportion:.2%}) with NaN values in features or target")
    if dropped_proportion > 0.5:
        logging.warning("High proportion of rows dropped due to NaN values, consider reviewing data quality")
if len(X_y) == 0:
    logging.error("No complete data rows after dropping NaNs")
    raise ValueError("No complete data rows after dropping NaNs")

X = X_y[features]
y = X_y['winner']

# Log class distribution
logging.info(f"Class distribution: {y.value_counts(normalize=True).to_dict()}")

# Split data: 60% train, 20% validation, 20% test
X_train_val, X_test, y_train_val, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)
X_train, X_val, y_train, y_val = train_test_split(X_train_val, y_train_val, test_size=0.25, shuffle=False, random_state=42)  # 0.25 * 0.8 = 0.2
logging.info(f"Training set: {len(X_train)} samples, Validation set: {len(X_val)} samples, Test set: {len(X_test)} samples")

# Standardize features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_val = scaler.transform(X_val)
X_test = scaler.transform(X_test)

# XGBoost classifier with balanced class weights
scale_pos_weight = sum(y_train == 0) / sum(y_train == 1) if sum(y_train == 1) > 0 else 1
xgb = XGBClassifier(
    objective='binary:logistic',
    eval_metric='auc',
    random_state=42,
    scale_pos_weight=scale_pos_weight,
    early_stopping_rounds=10
)

# Optimized parameter grid
param_grid = {
    'n_estimators': [100, 300, 500, 700],
    'max_depth': [3, 5, 7],
    'learning_rate': [0.005, 0.01, 0.05, 0.1],
    'subsample': [0.6, 0.8, 1.0],
    'colsample_bytree': [0.6, 0.8, 1.0],
    'reg_lambda': [0.1, 1.0, 10.0],
    'reg_alpha': [0.1, 1.0, 5.0],
    'min_child_weight': [1, 3, 5, 10],
    'gamma': [0.0, 0.1, 0.5, 1.0],
    'max_delta_step': [0, 1, 5]
}

# Randomized search with 5-fold CV, optimizing ROC-AUC
grid = RandomizedSearchCV(
    estimator=xgb,
    param_distributions=param_grid,
    n_iter=50,
    cv=5,
    scoring='roc_auc',
    n_jobs=-1,
    random_state=42
)
grid.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

# Best model
best_model = grid.best_estimator_
logging.info(f"Best parameters: {grid.best_params_}")
print(f"Best Parameters: {grid.best_params_}")
print(f"Best CV ROC-AUC: {grid.best_score_:.4f}")

# Feature selection with stricter threshold
selector = SelectFromModel(best_model, threshold='1.5*mean', prefit=True)
X_train_selected = selector.transform(X_train)
X_val_selected = selector.transform(X_val)
X_test_selected = selector.transform(X_test)
selected_features = [features[i] for i in selector.get_support(indices=True)]
logging.info(f"Selected {len(selected_features)} features: {selected_features}")

# Retrain on selected features
xgb_selected = XGBClassifier(
    objective='binary:logistic',
    eval_metric='auc',
    random_state=42,
    scale_pos_weight=scale_pos_weight,
    early_stopping_rounds=10,
    **grid.best_params_
)
xgb_selected.fit(X_train_selected, y_train, eval_set=[(X_val_selected, y_val)], verbose=False)

# Evaluate on test set
y_pred = xgb_selected.predict(X_test_selected)
accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)
roc_auc = roc_auc_score(y_test, xgb_selected.predict_proba(X_test_selected)[:, 1])

# Print and log results
print("\nTest Set Performance (Selected Features):")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"F1-Score: {f1:.4f}")
print(f"ROC-AUC: {roc_auc:.4f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Away Win', 'Home Win']))
logging.info(f"Test Accuracy: {accuracy:.4f}")
logging.info(f"Test Precision: {precision:.4f}")
logging.info(f"Test Recall: {recall:.4f}")
logging.info(f"Test F1-Score: {f1:.4f}")
logging.info(f"Test ROC-AUC: {roc_auc:.4f}")

# Feature importance for selected features
feature_importance = pd.DataFrame({
    'Feature': selected_features,
    'Importance': xgb_selected.feature_importances_
}).sort_values('Importance', ascending=False)
print("\nTop 10 Feature Importances (Selected Features):")
print(feature_importance.head(10))
logging.info("Top 10 Feature Importances (Selected Features):\n" + feature_importance.head(10).to_string())

# Save model, scaler, and feature selector
xgb_selected.save_model('xgboost_nba_winner_model.json')
logging.info("Saved model to xgboost_nba_winner_model.json")
joblib.dump(scaler, 'scaler.pkl')
logging.info("Saved scaler to scaler.pkl")
joblib.dump(selector, 'feature_selector.pkl')
logging.info("Saved feature selector to feature_selector.pkl")
