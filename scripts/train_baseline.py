import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.metrics import accuracy_score, mean_absolute_error, classification_report

class BaselineGeneExpressionModel:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.df = None
        self.X = None
        self.X_scaled = None
        self.y_gender = None
        self.y_age = None
        self.le = LabelEncoder()
        self.scaler = StandardScaler()

    def load_and_preprocess(self):
        # Load and preprocess the dataset
        self.df = pd.read_csv(self.csv_path)
        self.df['age'].hist()
        print(self.df.head())
        self.df = self.df.dropna(subset=['sex', 'age'])

        columns_to_drop = []
        for col in ['tissue', 'sample-id', 'sample_id', 'Unnamed: 0']:
            if col in self.df.columns:
                columns_to_drop.append(col)
        if columns_to_drop:
            self.df = self.df.drop(columns=columns_to_drop)

        # Encode categorical (sex)
        self.df['sex_enc'] = self.le.fit_transform(self.df['sex'])

        # Determine features
        drop_cols = [c for c in ['sex', 'sex_enc', 'age'] if c in self.df.columns]
        feature_cols = [c for c in self.df.columns if c not in drop_cols]
        self.X = self.df[feature_cols]
        self.y_gender = self.df['sex_enc']
        self.y_age = self.df['age']

        # Scaling
        self.X_scaled = self.scaler.fit_transform(self.X)

    def split_data(self, test_size=0.2, random_state=42):
        return train_test_split(
            self.X_scaled, self.y_gender, self.y_age, 
            test_size=test_size, random_state=random_state
        )

    def predict_gender(self, X_train, X_test, y_train, y_test):
        print("Predicting gender...")

        # Logistic Regression
        lr = LogisticRegression(max_iter=1000)
        lr.fit(X_train, y_train)
        y_pred_lr = lr.predict(X_test)
        print("Logistic Regression Accuracy (gender):", accuracy_score(y_test, y_pred_lr))

        # Random Forest
        rf = RandomForestClassifier(n_estimators=100, random_state=42)
        rf.fit(X_train, y_train)
        y_pred_rf = rf.predict(X_test)
        print("Random Forest Accuracy (gender):", accuracy_score(y_test, y_pred_rf))

        # Gradient Boosting
        gbc = GradientBoostingClassifier(n_estimators=100, random_state=42)
        gbc.fit(X_train, y_train)
        y_pred_gbc = gbc.predict(X_test)
        print("Gradient Boosting Accuracy (gender):", accuracy_score(y_test, y_pred_gbc))

        print("\nClassification report (best model may differ):\n", classification_report(y_test, y_pred_gbc, target_names=self.le.classes_))

    def predict_age(self, X_train, X_test, y_train, y_test):
        print("\nPredicting age...")

        # Random Forest Regressor
        rf_reg = RandomForestRegressor(n_estimators=100, random_state=42)
        rf_reg.fit(X_train, y_train)
        age_pred_rf = rf_reg.predict(X_test)
        print("Random Forest MAE (age):", mean_absolute_error(y_test, age_pred_rf))

        # Gradient Boosting Regressor
        gbr = GradientBoostingRegressor(n_estimators=100, random_state=42)
        gbr.fit(X_train, y_train)
        age_pred_gb = gbr.predict(X_test)
        print("Gradient Boosting MAE (age):", mean_absolute_error(y_test, age_pred_gb))

    def run(self):
        self.load_and_preprocess()
        X_train, X_test, y_gender_train, y_gender_test, y_age_train, y_age_test = self.split_data()
        self.predict_gender(X_train, X_test, y_gender_train, y_gender_test)
        self.predict_age(X_train, X_test, y_age_train, y_age_test)

if __name__ == "__main__":
    model = BaselineGeneExpressionModel('/Users/philippevannson/Desktop/AMARCHs3/merged.csv')
    model.run()

