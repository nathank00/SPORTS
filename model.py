import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split, RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import VarianceThreshold
from sklearn.base import BaseEstimator, TransformerMixin
import xgboost as xgb
import joblib
from tqdm import tqdm

df = pd.read_parquet('model/masterdata.parquet')
df = df.tail(len(df)-255)
df['game_date'] = pd.to_datetime(df['game_date'])
df = df[~(df['game_date'].dt.date == datetime.today().date())]

drop_cols = [col for col in df.columns if 'Name' in col or 'ID' in col or 'bbrefID' in col or '_P_' in col]
df.drop(columns=drop_cols, inplace=True)
df.dropna(subset=['over_under_runline', 'over_under_target'], inplace=True)

bullpen_stats = [
    'IP_real_20', 'ERA', 'H_20', 'BF_20', 'HR_20', 'R_20', 'ER_20', 'BB_20', 'SO_20', 'XB_against_20',
    'TB_against_20', 'ERA_20', 'WHIP_20', 'IP_real_10', 'H_10', 'BF_10', 'HR_10', 'R_10', 'ER_10', 'BB_10',
    'SO_10', 'XB_against_10', 'TB_against_10', 'ERA_10', 'WHIP_10', 'IP_real_5', 'H_5', 'BF_5', 'HR_5', 'R_5',
    'ER_5', 'BB_5', 'SO_5', 'XB_against_5', 'TB_against_5', 'ERA_5', 'WHIP_5', 'IP_real_3', 'H_3', 'BF_3',
    'HR_3', 'R_3', 'ER_3', 'BB_3', 'SO_3', 'XB_against_3', 'TB_against_3', 'ERA_3', 'WHIP_3', 'BB', 'BF', 'ER', 
    'H', 'HR', 'IP_real', 'R', 'SO', 'TB_against', 'WHIP', 'XB_against'
]

for team in ['Home', 'Away']:
    for stat in bullpen_stats:
        drop_cols = [f'{team}_bullpen_{i}_{stat}' for i in range(1, 16) if f'{team}_bullpen_{i}_{stat}' in df.columns]
        df.drop(columns=drop_cols, inplace=True)

X = df.drop(columns=['over_under_target', 'runs_total', 'game_date', 'runs_home', 'runs_away', 'game_id', 'home_name', 'away_name'])
y = df['over_under_target']
X.replace([np.inf, -np.inf], np.nan, inplace=True)

print(f"\nRows after preprocessing: {len(X)}")
print("Target class distribution:")
print(y.value_counts(dropna=False))

if len(X) == 0:
    raise ValueError("No rows available for training after preprocessing. Check filters or data availability.")

class CorrelationFilter(BaseEstimator, TransformerMixin):
    def __init__(self, threshold=0.95):
        self.threshold = threshold
        self.to_drop = []

    def fit(self, X, y=None):
        corr_matrix = pd.DataFrame(X).corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        self.to_drop = [column for column in upper.columns if any(upper[column] > self.threshold)]
        return self

    def transform(self, X):
        return pd.DataFrame(X).drop(columns=self.to_drop, errors='ignore')

pos_weight = (y == 0).sum() / (y == 1).sum()

pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='mean')),
    ('var_thresh', VarianceThreshold(threshold=0.01)),
    ('corr_filter', CorrelationFilter(threshold=0.95)),
    ('scaler', StandardScaler()),
    ('model', xgb.XGBClassifier(
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss',
        scale_pos_weight=pos_weight,
        n_jobs=-1
    ))
])
'''
param_dist = {
    'model__n_estimators': [50, 100, 150, 200],
    'model__learning_rate': [0.01, 0.05, 0.1, 0.15],
    'model__max_depth': [3, 5, 7, 9],
    'model__subsample': [0.6, 0.8, 1.0],
    'model__colsample_bytree': [0.6, 0.8, 1.0],
    'model__gamma': [0, 0.1, 0.2],
    'model__min_child_weight': [1, 3, 5]
}
'''
param_dist = {
    'model__n_estimators': [50, 100, 150],
    'model__learning_rate': [0.01, 0.05],
    'model__max_depth': [3, 5],
    'model__subsample': [0.6, 0.8],
    'model__colsample_bytree': [0.6, 0.8],
    'model__gamma': [0, 0.1],
    'model__min_child_weight': [1, 5]
}


X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.2, random_state=42)

search = RandomizedSearchCV(
    pipeline,
    param_distributions=param_dist,
    n_iter=10,
    #n_iter=50,
    scoring='accuracy',
    cv=StratifiedKFold(n_splits=5),
    verbose=1,
    n_jobs=-1,
    random_state=42
)

print("\nTraining model with hyperparameter tuning...\n")
for _ in tqdm([search.fit(X_train, y_train)], desc="Grid search progress"):
    pass

best_model = search.best_estimator_
y_pred = best_model.predict(X_test)

print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
print("Classification Report:\n", classification_report(y_test, y_pred))

joblib.dump(best_model, 'model/xgb_model.pkl')

# Save the feature set actually used after all transforms
X_transformed = search.best_estimator_.named_steps['corr_filter'].transform(
    search.best_estimator_.named_steps['var_thresh'].transform(
        search.best_estimator_.named_steps['imputer'].transform(X_train)
    )
)
final_features = X_transformed.columns if isinstance(X_transformed, pd.DataFrame) else [f'feature_{i}' for i in range(X_transformed.shape[1])]

with open("model/final_features.txt", "w") as f:
    for feat in final_features:
        f.write(f"{feat}\n")
