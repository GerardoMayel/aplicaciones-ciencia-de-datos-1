# app.py
from flask import Flask, jsonify
from sqlalchemy import create_engine, Column, Integer, Float, String, Date, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
import boto3
import pickle
import yaml
import os
from datetime import datetime
import pandas as pd

app = Flask(__name__)

class Base(DeclarativeBase):
    pass

# Modelo de datos
class Prediction(Base):
    __tablename__ = 'predictions'
    id = Column(Integer, primary_key=True)
    inspection_id = Column(Integer)
    prediction_score = Column(Float)
    prediction_label = Column(Integer)
    prediction_date = Column(Date)

def get_db_engine():
    """Intenta conectar a PostgreSQL, si falla usa SQLite"""
    try:
        postgres_url = 'postgresql://myuser:mypassword@3.82.248.97:5432/predictions'
        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        print("Usando PostgreSQL")
        return engine
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        print("Usando SQLite como fallback")
        return create_engine('sqlite:///predictions.db')

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
        
        print(f"Intentando cargar datos desde bucket: {bucket}")
        scores = pickle.loads(s3.Object(bucket, 'results/predictions_score.pkl').get()['Body'].read())
        labels = pickle.loads(s3.Object(bucket, 'results/predictions_label.pkl').get()['Body'].read())
        
        return scores, labels, bucket, session
    except Exception as e:
        print(f"Error cargando datos de S3: {e}")
        return None, None, None, None

def init_db():
    """Inicializa la base de datos y carga datos"""
    engine = get_db_engine()
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        if session.query(Prediction).first() is None:
            print("Base de datos vacía, cargando datos...")
            scores, labels, bucket, s3_session = load_s3_data()
            
            if scores is not None and labels is not None:
                predictions_df = pd.DataFrame({
                    'prediction_score': scores['predictions'],
                    'prediction_label': labels,
                    'prediction_date': scores['date']
                })
                predictions_df['inspection_id'] = range(1, len(predictions_df) + 1)
                
                for _, row in predictions_df.iterrows():
                    prediction = Prediction(
                        inspection_id=row['inspection_id'],
                        prediction_score=row['prediction_score'],
                        prediction_label=row['prediction_label'],
                        prediction_date=datetime.strptime(row['prediction_date'], '%Y-%m-%d').date()
                    )
                    session.add(prediction)
                
                session.commit()
                print("Datos cargados exitosamente")
            else:
                print("No se pudieron cargar los datos de S3")
        else:
            print("La base de datos ya contiene datos")
    except Exception as e:
        print(f"Error inicializando la base de datos: {e}")
    finally:
        session.close()
    
    return engine

# Ruta raíz para verificar que la API está funcionando
@app.route('/')
def home():
    return jsonify({
        'status': 'API is running',
        'endpoints': {
            'prediction_by_id': '/prediction_id/<id>',
            'predictions_by_date': '/predictions_date/<YYYY-MM-DD>'
        }
    })

@app.route('/prediction_id/<int:inspection_id>', methods=['GET'])
def get_prediction_by_id(inspection_id):
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        latest_date = session.query(Prediction.prediction_date)\
            .order_by(Prediction.prediction_date.desc())\
            .first()
        
        if not latest_date:
            return jsonify({'error': 'No hay predicciones disponibles'}), 404
        
        prediction = session.query(Prediction)\
            .filter(Prediction.inspection_id == inspection_id)\
            .filter(Prediction.prediction_date == latest_date[0])\
            .first()
        
        if not prediction:
            return jsonify({'error': f'Predicción no encontrada para ID {inspection_id}'}), 404
        
        return jsonify({
            'inspection_id': prediction.inspection_id,
            'prediction_score': prediction.prediction_score,
            'prediction_label': prediction.prediction_label,
            'prediction_date': prediction.prediction_date.strftime('%Y-%m-%d')
        })
    except Exception as e:
        return jsonify({'error': f'Error en la consulta: {str(e)}'}), 500
    finally:
        session.close()

@app.route('/predictions_date/<string:pred_date>', methods=['GET'])
def get_predictions_by_date(pred_date):
    try:
        date_obj = datetime.strptime(pred_date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'Formato de fecha inválido. Use YYYY-MM-DD'}), 400
    
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        predictions = session.query(Prediction)\
            .filter(Prediction.prediction_date == date_obj)\
            .all()
        
        if not predictions:
            return jsonify({'error': f'No hay predicciones para {pred_date}'}), 404
        
        return jsonify([{
            'inspection_id': p.inspection_id,
            'prediction_score': p.prediction_score,
            'prediction_label': p.prediction_label,
            'prediction_date': p.prediction_date.strftime('%Y-%m-%d')
        } for p in predictions])
    except Exception as e:
        return jsonify({'error': f'Error en la consulta: {str(e)}'}), 500
    finally:
        session.close()

if __name__ == '__main__':
    print("Iniciando la aplicación...")
    init_db()
    app.run(debug=True)