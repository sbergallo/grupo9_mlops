from fastapi import FastAPI, HTTPException
import psycopg2
from pydantic import BaseModel
from typing import List, Optional
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os 

app = FastAPI()


# Conexión a la base de datos
def get_db_connection():
    return psycopg2.connect(
        host="grupo-9-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        port=5432,
        database="grupo9",
        user="grupo9",
        password="grupo-9-mlops",
    )

if _name_ == "_main_":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
    

# Modelo de respuesta para recomendaciones
class Recommendation(BaseModel):
    advertiser: str
    model: str
    recommendations: List[dict]


@app.get("/")
def read_root():
    return {"Intro": "Las rutas para realizar consultas sobre información de los advertisers son: /recommendations/{adv}/{model}, /stats/ o /history/{adv}/ | Notas: adv = advertiser id ; model = 'ctr' o 'products' ."}


# Ruta: /recommendations/<ADV>/<Modelo>
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
            # Manejo de errores
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
        # Manejo de errores
        raise HTTPException(status_code=500, detail=str(e))
        
# Ruta: /stats/
@app.get("/stats/")
async def get_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Cantidad de advs
        cur.execute("SELECT COUNT(DISTINCT advertiser_id) FROM top_ctr")
        num_advertisers = cur.fetchone()[0]

        # Advs mayor variación
        cur.execute("""
            SELECT advertiser_id, COUNT(DISTINCT product_id) AS variations
            FROM top_ctr
            GROUP BY advertiser_id
            ORDER BY variations DESC
            LIMIT 5
        """)
        advertisers_with_variation = cur.fetchall()

        # Coincidencias
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
        # Manejo de errores
        raise HTTPException(status_code=500, detail=str(e))


# Ruta: /history/<ADV>/
@app.get("/history/{adv}/")
async def get_history(adv: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Calculo fecha de la ultima semana
        last_week = datetime.now() - timedelta(days=7)

        # Consulta sobre las tablas top_ctr y top_product
        query = """
                SELECT tc.product_id, tc.ctr, tp.views, tc.created_at
                FROM top_ctr tc
                LEFT JOIN top_product tp ON tp.advertiser_id = tc.advertiser_id AND tp.product_id = tc.product_id
                WHERE tc.advertiser_id = %s
                AND tc.created_at >= %s
                ORDER BY tc.created_at DESC
        """
        
        cur.execute(query, (adv, last_week))
        data = cur.fetchall()
        history = [
            {
                "product_id": row[0],
                "ctr": row[1],
                "views": row[2],
                "date": row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
            }
            for row in data
        ]
        cur.close()
        conn.close()

        # Retornar los datos en formato JSON
        return {
            "advertiser": adv,
            "history": history,
        }
    except Exception as e:
        # Manejo de errores
        raise HTTPException(status_code=500, detail=f"Error retrieving history: {str(e)}")
