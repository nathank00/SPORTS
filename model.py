import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, StratifiedKFold, RandomizedSearchCV
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import xgboost as xgb
import joblib
import time
import matplotlib.pyplot as plt

from model.data_cleaner import clean_input_data

# === Load and clean data ===
df = pd.read_parquet('model/masterdata.parquet')
X, y = clean_input_data(df)

# === Train/Test Split ===
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# === Pipeline ===
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('model', xgb.XGBClassifier(
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42
    ))
])

# === Hyperparameter Grid ===
param_dist = {
    'model__n_estimators': [100, 200, 300, 500],
    'model__learning_rate': [0.01, 0.05, 0.1],
    'model__max_depth': [3, 5, 7],
    'model__subsample': [0.6, 0.8, 1.0],
    'model__colsample_bytree': [0.6, 0.8, 1.0],
    'model__gamma': [0, 0.01, 0.1, 0.2]
}

# === Cross-validation ===
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

# === Randomized Search ===
search = RandomizedSearchCV(
    pipeline,
    param_distributions=param_dist,
    n_iter=55,
    scoring='roc_auc',  # better for probabilistic binary classification
    cv=cv,
    verbose=1,
    n_jobs=-1,
    random_state=42
)

start = time.time()
search.fit(X_train, y_train)
print(f"Training time: {time.time() - start:.2f} seconds")

# === Evaluation ===
best_model = search.best_estimator_
y_pred = best_model.predict(X_test)
y_proba = best_model.predict_proba(X_test)[:, 1]

print(f"\nTotal training samples: {len(X)}")
print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
print(f"ROC AUC: {roc_auc_score(y_test, y_proba):.4f}")
print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))
print("\nClassification Report:\n", classification_report(y_test, y_pred))

# === Save Model ===
model_out = 'model/xgb_model.pkl'
joblib.dump(best_model, model_out)
print(f"Model saved to {model_out}")

# === Save Feature Names ===
final_features = X.columns.tolist()
with open("model/final_features.txt", "w") as f:
    for feat in final_features:
        f.write(f"{feat}\n")

# === Save Evaluation Summary ===
summary_out = model_out.replace(".pkl", "_summary.txt")
with open(summary_out, "w") as f:
    f.write(f"Model: {model_out}\n")
    f.write(f"Training samples: {len(X)}\n")
    f.write(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}\n")
    f.write(f"ROC AUC: {roc_auc_score(y_test, y_proba):.4f}\n\n")
    f.write("Confusion Matrix:\n")
    f.write(np.array2string(confusion_matrix(y_test, y_pred)))
    f.write("\n\nClassification Report:\n")
    f.write(classification_report(y_test, y_pred))
print(f"Evaluation summary saved to {summary_out}")
