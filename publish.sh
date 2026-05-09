#!/bin/bash
set -e

# ============================================================================
# CONFIGURACIÓN DEL STACK
# ============================================================================
BIGDATA_BASE="$HOME/Documentos/repositoriosGithub/personal/bigdata_stack_platform/volumenes"
BIGDATA_SHARED="$BIGDATA_BASE/shared"
# Landing Zone donde Spark ve los archivos como locales
BIGDATA_MINIO_SHARE="$BIGDATA_SHARED/minioshareddata" 
PROJECT_NAME="fraud_pipeline"

# Colores
GREEN='\033[0;32m'
NC='\033[0m'
print_ok() { echo -e "${GREEN}[✓]${NC} $1"; }

# ============================================================================
# 1. INFRAESTRUCTURA
# ============================================================================
print_ok "⚙️ Preparando directorios en Share..."
# Creamos la carpeta de aterrizaje (Landing)
sudo mkdir -p "$BIGDATA_MINIO_SHARE/landing/payments"

# Permisos generales para el volumen compartido
sudo chmod -R 777 "$BIGDATA_SHARED"
sudo chown -R 1000:1000 "$BIGDATA_SHARED"

# ============================================================================
# 2. GENERACIÓN DE DATOS
# ============================================================================
print_ok "⚡ Ejecutando Generador de Datos..."

# ✅ CORRECCIÓN: Ejecutamos el script desde su ubicación real 'data/'
# Como se ejecuta desde la raíz, el output irá a 'data/input_events' correctamente
python3 data/raw_generator.py

print_ok "📦 Moviendo datos a Landing Zone..."
# Sincronizamos desde la carpeta local data/input_events hacia el Share
if [ -d "data/input_events" ]; then
    rsync -av --delete data/input_events/ "$BIGDATA_MINIO_SHARE/landing/payments/"
fi

# Aseguramos permisos (UID 1000) para que Spark pueda leer
sudo chown -R 1000:1000 "$BIGDATA_MINIO_SHARE/landing"
sudo chmod -R 777 "$BIGDATA_MINIO_SHARE/landing"

# ============================================================================
# 3. PUBLICACIÓN DE CÓDIGO
# ============================================================================
print_ok "📝 Publicando Scripts y DAGs..."

# Scripts de Spark
TARGET_SPARK="$BIGDATA_SHARED/scripts_airflow/$PROJECT_NAME"
mkdir -p "$TARGET_SPARK"
# Tu tree muestra 'spark_jobs' en la raíz, esto es correcto:
rsync -av --delete spark_jobs/ "$TARGET_SPARK/"

# DAGs de Airflow
TARGET_DAGS="$BIGDATA_SHARED/dags_airflow"
mkdir -p "$TARGET_DAGS"
# Tu tree muestra 'dags' en la raíz, esto es correcto:
rsync -av dags/ "$TARGET_DAGS/"

# Permisos de lectura finales
find "$BIGDATA_SHARED" -type f -name "*.py" -exec chmod 666 {} \; 2>/dev/null || true

print_ok "🚀 Pipeline desplegado correctamente."