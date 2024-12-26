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

if __name__ == "__main__":
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
    # Modelos válidos
    valid_models = ["CTR", "TopProduct"]

    # Validar modelo
    if model not in valid_models:
        raise HTTPException(
            status_code=400,
            detail=f"Modelo inválido: {model}. Modelos válidos son {valid_models}"
        )

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT product_id, score
            FROM recommendations
            WHERE advertiser_id = %s
              AND model = %s
            ORDER BY score DESC
            LIMIT 5
        """

        cur.execute(query, (adv, model))
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

        # Cantidad de anunciantes
        cur.execute("SELECT COUNT(DISTINCT advertiser_id) FROM recommendations")
        num_advertisers = cur.fetchone()[0]

        # Anunciantes con mayor variación de productos
        cur.execute("""
            SELECT advertiser_id, COUNT(DISTINCT product_id) AS variations
            FROM recommendations
            GROUP BY advertiser_id
            ORDER BY variations DESC
            LIMIT 5
        """)
        advertisers_with_variation = cur.fetchall()

        # Coincidencias entre modelos (ajusta según tu lógica específica)
        cur.execute("""
            SELECT COUNT(*)
            FROM recommendations
            WHERE model IN ('CTR', 'TopProduct')
            GROUP BY product_id
            HAVING COUNT(*) > 1
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
        conn = get_db_connection()
        cur = conn.cursor()

        # Calcula la fecha de la última semana
        last_week = datetime.now() - timedelta(days=7)

        # Consulta sobre la tabla recommendations
        query = """
            SELECT product_id, model, score, date
            FROM recommendations
            WHERE advertiser_id = %s
              AND model IN ('CTR', 'TopProduct')
              AND date >= %s
            ORDER BY date DESC
        """

        cur.execute(query, (adv, last_week))
        data = cur.fetchall()
        history = [
            {
                "product_id": row[0],
                "model": row[1],    # Columna model (CTR o TopProduct)
                "score": row[2],       # Columna score
                "date": row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
            }
            for row in data
        ]
        cur.close()
        conn.close()

        return {
            "advertiser": adv,
            "history": history,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving history: {str(e)}")
