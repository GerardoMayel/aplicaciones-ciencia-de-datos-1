import pandas as pd
import yaml
import pickle
import boto3
from datetime import date
from sodapy import Socrata

# Cargamos la configuración del YAML. Esto nos ahorra tener que escribir las credenciales directamente en el código.
with open('credentials.yaml', 'r') as f:
    config = yaml.safe_load(f)

token = config['api_chicago']['app_token']
username = config['api_chicago']['username']
password = config['api_chicago']['password']
matricula = config['iexe']['matricula']

# Este es el ID del dataset de inspecciones de comida de Chicago
chicago_dataset = "4ijn-s7e5"

def get_client():
    # Creamos un cliente para la API de Chicago. Esto es como pedir permiso para acceder a sus datos.
    return Socrata("data.cityofchicago.org", 
                   token,
                   username=username,
                   password=password)

def ingesta_inicial(chicago_dataset, client, limit):
    # Aquí hacemos la primera carga grande de datos. Es como llenar un camión con toda la información disponible.
    return client.get(chicago_dataset, limit=limit, offset=0, order='inspection_date')

def get_s3_resource():
    # Configuramos la conexión a S3. Es como preparar nuestro almacén virtual en la nube.
    session = boto3.Session(
        aws_access_key_id = config['s3']['aws_access_key_id'],
        aws_secret_access_key = config['s3']['aws_secret_access_key'],
        aws_session_token= config['s3']['aws_session_token']
    )
    return session.resource('s3')

def guardar_ingesta(bucket, bucket_path, dataset):
    # Guardamos los datos en S3. Es como archivar nuestros documentos en el almacén.
    s3 = get_s3_resource()
    s3.Object(bucket, bucket_path).put(Body=pickle.dumps(dataset))

def ingesta_consecutiva(chicago_dataset, client, fecha, limit):
    # Esto es para cargas posteriores. Sólo tomamos los datos nuevos desde la última vez que revisamos.
    return client.get(chicago_dataset, limit=limit, where=f"inspection_date>='{fecha}'")

def crear_bucket_si_no_existe():
    # Nos aseguramos de tener un lugar donde guardar nuestros datos en S3.
    s3 = get_s3_resource()
    bucket_name = f"aplicaciones-cd-1-{matricula}"
    if bucket_name not in [bucket.name for bucket in s3.buckets.all()]:
        s3.create_bucket(Bucket=bucket_name)
    return bucket_name

# Función adicional para verificar la integridad de los datos cargados
def verificar_carga(bucket, bucket_path):
    # Esta función es como hacer un conteo rápido para asegurarnos de que todo lo que cargamos está ahí.
    s3 = get_s3_resource()
    obj = s3.Object(bucket, bucket_path)
    data = pickle.loads(obj.get()['Body'].read())
    return len(data)

if __name__ == "__main__":
    TODAY = date.today()
    client = get_client()
    
    print("Iniciando la carga inicial de datos...")
    datasets = ingesta_inicial(chicago_dataset, client, 300000)
    print(f"Se obtuvieron {len(datasets)} registros en la carga inicial.")
    
    bucket_name = crear_bucket_si_no_existe()
    
    print("Guardando la carga inicial en S3...")
    key_inicial = f"ingesta/inicial/inspecciones-historicas-{TODAY}.pkl"
    guardar_ingesta(bucket_name, key_inicial, datasets)
    
    print("Verificando la integridad de la carga inicial...")
    registros_cargados = verificar_carga(bucket_name, key_inicial)
    print(f"Se verificaron {registros_cargados} registros en S3.")
    
    print("Realizando una carga consecutiva de ejemplo...")
    new_dataset = ingesta_consecutiva(chicago_dataset, client, '2023-01-01', 1000)
    print(f"Se obtuvieron {len(new_dataset)} registros en la carga consecutiva.")
    
    print("Guardando la carga consecutiva en S3...")
    key_consecutiva = f"ingesta/consecutiva/inspecciones-consecutivas-{TODAY}.pkl"
    guardar_ingesta(bucket_name, key_consecutiva, new_dataset)
    
    print("Verificando la integridad de la carga consecutiva...")
    registros_consecutivos = verificar_carga(bucket_name, key_consecutiva)
    print(f"Se verificaron {registros_consecutivos} registros consecutivos en S3.")
    
    print("Proceso de ingesta y almacenamiento completado.")