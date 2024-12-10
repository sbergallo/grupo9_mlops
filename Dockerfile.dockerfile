# Usar imagen base
FROM python:3.10-slim-bullseye

# Crear directorio de trabajo
WORKDIR /app

# Copiar dependencias
COPY requirements_2.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la API
COPY app /app
COPY . /app


# Exponer el puerto
EXPOSE 8000

# Comando para ejecutar la API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
