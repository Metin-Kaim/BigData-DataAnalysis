# train.py

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd
import joblib

# Iris veri setini yükle
iris_data = pd.read_csv('train.csv')  # Veri setinizi uygun bir şekilde değiştirin

# Özellikler ve etiketleri ayır
X = iris_data.drop('species', axis=1)
y = iris_data['species']

# Eğitim ve test setlerini oluştur
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Modeli eğit
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Modeli değerlendir
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f'Model Accuracy: {accuracy}')

# Modeli kaydet
joblib.dump(model, 'iris_model.joblib')  # Modeli uygun bir şekilde kaydedin
