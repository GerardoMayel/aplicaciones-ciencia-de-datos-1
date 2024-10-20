import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import yaml
import pickle
import boto3

from datetime import date
from sodapy import Socrata

# Cargamos la configuración del archivo YAML
# Esto nos permite mantener las credenciales fuera del código
with open('credentials.yaml', 'r') as f:
    config = yaml.safe_load(f)

token = config['api_chicago']['app_token']
username = config['api_chicago']['username']
password = config['api_chicago']['password']
matricula = config['iexe']['matricula']

# Identificador del conjunto de datos de Chicago
# Lo usaremos para todas las llamadas a la API
chicago_dataset = "4ijn-s7e5"

def get_client():
    # Esta función crea un cliente para conectarnos a la API de Chicago
    # Usamos las credenciales del archivo YAML para autenticarnos
    client = Socrata("data.cityofchicago.org", 
                     token,
                     username=username,
                     password=password)
    
    return client

def ingesta_inicial(chicago_dataset, client, limit):
    # Aquí hacemos la ingesta inicial de los datos
    # Pedimos hasta el límite especificado de registros
    # Los ordenamos por fecha de inspección para tener un orden consistente
    datasets = client.get(chicago_dataset, limit=limit, offset=0, order='inspection_date')
    
    return datasets

def get_s3_resource():
    # Creamos una conexión a S3 usando las credenciales del archivo YAML
    # Esto nos permite interactuar con nuestro bucket en S3
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        aws_session_token=config['s3']['aws_session_token']
    )
    
    return session.resource('s3')

def guardar_ingesta(bucket, bucket_path, dataset):
    # Esta función guarda los datos ingestados en S3
    # Usamos pickle para serializar los datos antes de subirlos
    s3 = get_s3_resource()
    
    # Serializamos el dataset
    pickled_data = pickle.dumps(dataset)
    
    # Obtenemos la fecha de hoy para el nombre del archivo
    today = date.today()
    
    # Creamos la ruta completa con la fecha
    full_path = f"{bucket_path}-{today}.pkl"
    
    # Subimos los datos serializados a S3
    s3.Object(bucket, full_path).put(Body=pickled_data)

def ingesta_consecutiva(chicago_dataset, client, fecha, limit):
    # Esta función realiza ingestas consecutivas
    # Pedimos datos a partir de la fecha especificada
    new_dataset = client.get(chicago_dataset, limit=limit, where=f"inspection_date>='{fecha}'")
    
    return new_dataset

# Ejecución principal del script
if __name__ == "__main__":
    # Primero hacemos la ingesta inicial
    print("Iniciando ingesta inicial...")
    client = get_client()
    datasets = ingesta_inicial(chicago_dataset, client, 300000)
    
    # Guardamos la ingesta inicial en S3
    print("Guardando ingesta inicial en S3...")
    bucket = f"aplicaciones-cd-1-{matricula}"
    initial_key = "ingesta/inicial/inspecciones-historicas"
    guardar_ingesta(bucket, initial_key, datasets)
    
    # Ahora hacemos una ingesta consecutiva
    # Usamos una fecha de ejemplo, pero en la práctica
    # usaríamos la fecha de la última ingesta
    print("Iniciando ingesta consecutiva...")
    new_datasets = ingesta_consecutiva(chicago_dataset, client, '2020-11-03', 1000)
    
    # Guardamos la ingesta consecutiva en S3
    print("Guardando ingesta consecutiva en S3...")
    consecutive_key = "ingesta/consecutiva/inspecciones-consecutivas"
    guardar_ingesta(bucket, consecutive_key, new_datasets)

    print("¡Ingesta y almacenamiento completados con éxito!")

# Nota: Este script realiza tanto la ingesta inicial como una consecutiva
# En un escenario real, probablemente separaríamos estas operaciones
# y las ejecutaríamos en momentos diferentes

# También sería buena idea agregar manejo de errores y logging
# para tener un mejor seguimiento de lo que está pasando durante la ejecución