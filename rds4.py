from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
from io import StringIO
import psycopg2

# Inicializar cliente S3
s3 = boto3.client('s3')
BUCKET_NAME = 'grupo-9-mlops'

# Conectar a la base de datos
def connect_to_db():
    return psycopg2.connect(
        database="grupo9",
        user="grupo9",
        password="grupo-9-mlops",
        host="grupo-9-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        port='5432'
    )

# Filtrar datos por fecha
def filtrar_datos(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')

    obj = s3.get_object(Bucket=BUCKET_NAME, Key='advertiser_ids.csv')
    advertiser_ids = pd.read_csv(obj['Body'])
    
    obj_ads = s3.get_object(Bucket=BUCKET_NAME, Key='ads_views.csv')
    ads_views = pd.read_csv(obj_ads['Body'])
    
    obj_products = s3.get_object(Bucket=BUCKET_NAME, Key='product_views.csv')
    product_views = pd.read_csv(obj_products['Body'])
    
    # Filtrar por fecha
    ads_views['date'] = pd.to_datetime(ads_views['date'])
    product_views['date'] = pd.to_datetime(product_views['date'])
    ads_views_filtered = ads_views[ads_views['date'] == execution_date]
    product_views_filtered = product_views[product_views['date'] == execution_date]
    
    active_ads = ads_views_filtered[ads_views_filtered['advertiser_id'].isin(advertiser_ids['advertiser_id'])]
    active_products = product_views_filtered[product_views_filtered['advertiser_id'].isin(advertiser_ids['advertiser_id'])]

    buffer_ads = StringIO()
    active_ads.to_csv(buffer_ads, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=f'filtered_ads_views_{execution_date}.csv', Body=buffer_ads.getvalue())

    buffer_products = StringIO()
    active_products.to_csv(buffer_products, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=f'filtered_product_views_{execution_date}.csv', Body=buffer_products.getvalue())

# Calcular top CTR
def calcular_top_ctr(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=f'filtered_ads_views_{execution_date}.csv')
    ads_views = pd.read_csv(obj['Body'])
    
    ctr_data = ads_views.groupby(['advertiser_id', 'product_id']).apply(
        lambda x: len(x[x['type'] == 'click']) / len(x)
    ).reset_index(name='ctr')

    top_ctr = ctr_data.sort_values('ctr', ascending=False).groupby('advertiser_id').head(20)

    buffer_ctr = StringIO()
    top_ctr.to_csv(buffer_ctr, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=f'top_ctr_{execution_date}.csv', Body=buffer_ctr.getvalue())

# Calcular top Products
def calcular_top_product(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=f'filtered_product_views_{execution_date}.csv')
    product_views = pd.read_csv(obj['Body'])
    
    top_product = product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
    top_product = top_product.sort_values('views', ascending=False).groupby('advertiser_id').head(20)

    buffer_product = StringIO()
    top_product.to_csv(buffer_product, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=f'top_product_{execution_date}.csv', Body=buffer_product.getvalue())

# Escribir en base de datos
def escribir_a_db(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    conn = connect_to_db()
    cursor = conn.cursor()

    # Escribir top CTR
    obj_ctr = s3.get_object(Bucket=BUCKET_NAME, Key=f'top_ctr_{execution_date}.csv')
    top_ctr = pd.read_csv(obj_ctr['Body'])
    for _, row in top_ctr.iterrows():
        cursor.execute(
            "INSERT INTO recommendations (advertiser_id, product_id, model, score, date) VALUES (%s, %s, %s, %s, %s)",
            (row['advertiser_id'], row['product_id'], 'CTR', row['ctr'], execution_date)
        )

    # Escribir top Products
    obj_product = s3.get_object(Bucket=BUCKET_NAME, Key=f'top_product_{execution_date}.csv')
    top_product = pd.read_csv(obj_product['Body'])
    for _, row in top_product.iterrows():
        cursor.execute(
            "INSERT INTO recommendations (advertiser_id, product_id, model, score, date) VALUES (%s, %s, %s, %s, %s)",
            (row['advertiser_id'], row['product_id'], 'TopProduct', row['views'], execution_date)
        )

    conn.commit()
    cursor.close()
    conn.close()

# Definir DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('pipeline_recomendaciones',
         default_args=default_args,
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_recommendations_table',
        postgres_conn_id='postgres_rds',
        sql="""
        CREATE TABLE IF NOT EXISTS recommendations (
            advertiser_id VARCHAR(255),
            product_id VARCHAR(255),
            model VARCHAR(50),
            score FLOAT,
            date DATE
        );
        """
    )

    filtrar = PythonOperator(
        task_id='filtrar_datos',
        python_callable=filtrar_datos,
        provide_context=True
    )

    top_ctr = PythonOperator(
        task_id='calcular_top_ctr',
        python_callable=calcular_top_ctr,
        provide_context=True
    )

    top_product = PythonOperator(
        task_id='calcular_top_product',
        python_callable=calcular_top_product,
        provide_context=True
    )

    db_writing = PythonOperator(
        task_id='escribir_a_db',
        python_callable=escribir_a_db,
        provide_context=True
    )

    filtrar >> create_table >> [top_ctr, top_product] >> db_writing
