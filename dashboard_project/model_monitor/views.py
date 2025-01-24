# model_monitor/views.py
from django.shortcuts import render
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import yaml
import boto3
import pickle
import base64

def load_s3_data():
    """Carga datos desde S3"""
    try:
        with open("credentials.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        bucket = "aplicaciones-cd-2-" + config['iexe']['matricula']
        session = boto3.Session(
            aws_access_key_id=config['s3']['aws_access_key_id'],
            aws_secret_access_key=config['s3']['aws_secret_access_key'],
            aws_session_token=config['s3']['aws_session_token']
        )
        s3 = session.resource('s3')
        
        # Cargar predicciones y etiquetas
        scores_obj = s3.Object(bucket, 'results/predictions_score.pkl').get()['Body'].read()
        scores = pickle.loads(scores_obj)
        
        labels_obj = s3.Object(bucket, 'results/predictions_label.pkl').get()['Body'].read()
        labels = pickle.loads(labels_obj)
        
        # Cargar ROC curve
        roc_obj = s3.Object(bucket, 'results/roc.png').get()['Body'].read()
        roc_base64 = base64.b64encode(roc_obj).decode('utf-8')
        
        return scores, labels, roc_base64
    except Exception as e:
        print(f"Error cargando datos de S3: {e}")
        return None, None, None

def create_score_histogram(scores):
    """Crear histograma de scores"""
    fig = go.Figure(data=[go.Histogram(
        x=scores['predictions'],
        nbinsx=30,
        name='Scores'
    )])
    
    fig.update_layout(
        title='Distribución de Scores de Predicción',
        xaxis_title='Score',
        yaxis_title='Frecuencia',
        template='plotly_white'
    )
    return fig.to_html(full_html=False, include_plotlyjs=True)

def create_labels_histogram(labels):
    """Crear histograma de etiquetas"""
    labels_df = pd.DataFrame({'Etiqueta': labels})
    fig = px.histogram(
        labels_df, 
        x='Etiqueta',
        title='Distribución de Etiquetas de Predicción',
        template='plotly_white'
    )
    
    fig.update_layout(
        xaxis_title='Etiqueta (0: No Pasa, 1: Pasa)',
        yaxis_title='Frecuencia'
    )
    return fig.to_html(full_html=False, include_plotlyjs=True)

def dashboard_view(request):
    """Vista principal del dashboard"""
    try:
        scores, labels, roc_base64 = load_s3_data()
        
        if scores is not None and labels is not None:
            score_hist = create_score_histogram(scores)
            label_hist = create_labels_histogram(labels)
            
            context = {
                'score_histogram': score_hist,
                'label_histogram': label_hist,
                'roc_curve': roc_base64,
                'prediction_date': scores['date']
            }
        else:
            context = {
                'error': 'No se pudieron cargar los datos desde S3'
            }
    except Exception as e:
        context = {
            'error': f'Error en el dashboard: {str(e)}'
        }
    
    return render(request, 'model_monitor/dashboard.html', context)