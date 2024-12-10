from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os 

app = FastAPI()

if _name_ == "_main_":
    import uvicorn

    # Configuración estándar para App Runner
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)

# Conexión a la base de datos
def get_db_connection():
    return psycopg2.connect(
        host="grupo-9-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        port=5432,
        database="grupo9",
        user="grupo9",
        password="grupo-9-mlops",
    )

# Modelo de respuesta para recomendaciones
class Recommendation(BaseModel):
    advertiser: str
    model: str
    recommendations: List[dict]


@app.get("/")
def read_root():
    return {"Intro": "Hola! Las rutas para realizar consultas sobre información de los advertisers son: /recommendations/{adv}/{model}, /stats/ o /history/{adv}/ | Notas: adv = advertiser id ; model = 'ctr' o 'products' ."}


# Ruta: /recommendations/<ADV>/<Modelo>
# Lo que te da es una recomendación en base a si lo que queres mostrar son prods mas populares
# o prods con el mayor ratio de conversion

@app.get("/recommendations/{adv}/{model}", response_model=Recommendation)
async def get_recommendations(adv: str, model: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        if model == "ctr":
            query = """
                SELECT product_id, ctr
                FROM top_ctr
                WHERE advertiser_id = %s
                ORDER BY ctr DESC
                LIMIT 5
            """
        elif model == "products":
            query = """
                SELECT product_id, views
                FROM top_product
                WHERE advertiser_id = %s
                ORDER BY views DESC
                LIMIT 5
            """
        else:
            raise HTTPException(status_code=400, detail="Modelo no válido")

        cur.execute(query, (adv,))
        data = cur.fetchall()
        recommendations = [{"product_id": row[0], "metric": row[1]} for row in data]

        cur.close()
        conn.close()

        return Recommendation(
            advertiser=adv,
            model=model,
            recommendations=recommendations,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Ruta: /stats/
@app.get("/stats/")
async def get_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Cantidad de advertisers
        cur.execute("SELECT COUNT(DISTINCT advertiser_id) FROM top_ctr")
        num_advertisers = cur.fetchone()[0]

        # Advertisers con mayor variación
        cur.execute("""
            SELECT advertiser_id, COUNT(DISTINCT product_id) AS variations
            FROM top_ctr
            GROUP BY advertiser_id
            ORDER BY variations DESC
            LIMIT 5
        """)
        advertisers_with_variation = cur.fetchall()

        # Coincidencias entre ambos modelos
        cur.execute("""
            SELECT COUNT(*)
            FROM top_ctr AS c
            JOIN top_product AS p
            ON c.advertiser_id = p.advertiser_id AND c.product_id = p.product_id
        """)
        model_matches = cur.fetchone()[0]

        cur.close()
        conn.close()

        return {
            "num_advertisers": num_advertisers,
            "advertisers_with_variation": advertisers_with_variation,
            "model_matches": model_matches,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Ruta: /history/<ADV>/

@app.get("/history/{adv}/")
async def get_history(adv: str):
    try:
        # Establecer la conexión a la base de datos
        conn = get_db_connection()
        cur = conn.cursor()

        # Calcular la fecha de hace 7 días
        last_week = datetime.now() - timedelta(days=7)

        # Consulta SQL para obtener los datos de las tablas top_ctr y top_product
        query = """
                SELECT tc.product_id, tc.ctr, tp.views, tc.created_at
                FROM top_ctr tc
                LEFT JOIN top_product tp ON tp.advertiser_id = tc.advertiser_id AND tp.product_id = tc.product_id
                WHERE tc.advertiser_id = %s
                AND tc.created_at >= %s
                ORDER BY tc.created_at DESC
        """
        
        # Ejecutar la consulta con el filtro de los últimos 7 días
        cur.execute(query, (adv, last_week))
        data = cur.fetchall()

        # Preparar los resultados para la respuesta en formato JSON
        history = [
            {
                "product_id": row[0],
                "ctr": row[1],
                "views": row[2],
                "date": row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
            }
            for row in data
        ]

        # Cerrar cursor y conexión
        cur.close()
        conn.close()

        # Retornar los datos en formato JSON
        return {
            "advertiser": adv,
            "history": history,
        }
    except Exception as e:
        # En caso de error, lanzar una excepción HTTP con el mensaje del error
        raise HTTPException(status_code=500, detail=f"Error retrieving history: {str(e)}")