# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ---------- PROTOBUF ----------------
# # Copia los archivos .proto
# COPY ./*.proto ./

# # Instala el compilador de Protobuf
# RUN apt-get update && apt-get install -y protobuf-compiler

# # Genera el código de Protobuf
# RUN protoc --python_out=. *.proto
# ------------------------------------

# Copia el resto de los archivos de tu aplicación
COPY . .

# Comando para ejecutar tu aplicación
CMD ["python", "middleware.py"]
