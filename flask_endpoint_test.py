import requests

# Testear endpoint principal
response = requests.get("http://127.0.0.1:5000/")
print("Endpoint principal:", response.json())

# Testear predicción por ID
inspection_id = 1
response = requests.get(f"http://127.0.0.1:5000/prediction_id/{inspection_id}")
if response.status_code == 200:
    print(f"Predicción para ID {inspection_id}:", response.json())
else:
    print(f"Error al obtener predicción para ID {inspection_id}:", response.json())

# Testear predicciones por fecha
pred_date = "2025-01-01"
response = requests.get(f"http://127.0.0.1:5000/predictions_date/{pred_date}")
if response.status_code == 200:
    print(f"Predicciones para la fecha {pred_date}:", response.json())
else:
    print(f"Error al obtener predicciones para la fecha {pred_date}:", response.json())
