#!/bin/bash
set -e

# ============================================================================
# CONFIG
# ============================================================================
BIGDATA_BASE="$HOME/Documentos/docker-compose-contenedores/bigdata/volumenes"
BIGDATA_SHARED="$BIGDATA_BASE/shared"
BIGDATA_MINIO="$BIGDATA_SHARED/minio/data"
PROJECT_NAME="fraud_pipeline"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_ok()    { echo -e "${GREEN}[✓]${NC} $1"; }
print_warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
print_err()   { echo -e "${RED}[✗]${NC} $1"; }

# ============================================================================
# INFRA (one-time safety net)
# ============================================================================
prepare_infra() {
    print_ok "Preparing MinIO directories (if missing)..."

    DIRS=(
        "$BIGDATA_MINIO/bronze/payments"
        "$BIGDATA_MINIO/seed_data"
        "$BIGDATA_MINIO/silver"
        "$BIGDATA_MINIO/silver/payments_clean"
    )

    for d in "${DIRS[@]}"; do
        if [ ! -d "$d" ]; then
            print_warn "Creating $d"
            sudo mkdir -p "$d"
        fi
        # ✅ Permisos para que Spark (UID 1000) pueda escribir/borrar
        sudo chmod -R 777 "$d"
        sudo chown -R 1000:1000 "$d"
    done

    # 1. Cambiamos el dueño de TODO el árbol de silver al UID 1000 (Spark)
    #sudo chown -R 1000:1000 $BIGDATA_MINIO/silver

    # 2. Aseguramos que el padre (silver) permita escribir al dueño
   #sudo chmod -R 775 $BIGDATA_MINIO/silver
}

# ============================================================================
# PUBLISH
# ============================================================================
publish_spark() {
    print_ok "Publishing Spark jobs..."
    TARGET="$BIGDATA_SHARED/scripts_airflow/$PROJECT_NAME"
    mkdir -p "$TARGET"
    rsync -av --delete spark_jobs/ "$TARGET/"
    
    
    # Fix permissions so Airflow/Spark containers can read
    print_ok "Setting permissions for Spark jobs..."
    find "$TARGET" -type f -name "*.py" -exec chmod 666 {} \;
}

publish_dags() {
    print_ok "Publishing Airflow DAGs..."
    TARGET="$BIGDATA_SHARED/dags_airflow"
    mkdir -p "$TARGET"

    if [ -d "dags" ] && [ "$(ls -A dags 2>/dev/null)" ]; then
        rsync -av dags/ "$TARGET/"
        
        # Fix permissions so Airflow can read DAGs
        print_ok "Setting permissions for DAGs..."
        find "$TARGET" -type f -name "*.py" -exec chmod 666 {} \;
    else
        print_warn "No DAGs to publish"
    fi
}

publish_data() {
    print_ok "Publishing data to MinIO..."

    if [ -d "data/input_events" ] && [ "$(ls -A data/input_events 2>/dev/null)" ]; then
        rsync -av data/input_events/ "$BIGDATA_MINIO/bronze/payments/"
    fi

    if [ -d "data/seed_data" ] && [ "$(ls -A data/seed_data 2>/dev/null)" ]; then
        rsync -av data/seed_data/ "$BIGDATA_MINIO/seed_data/"
    fi
}

# ============================================================================
# MAIN
# ============================================================================
main() {
    echo "======================================================="
    echo "  FRAUD PIPELINE - PUBLISH"
    echo "======================================================="

    prepare_infra
    publish_spark
    publish_dags
    publish_data

    echo "======================================================="
    print_ok "Publish finished successfully"
    echo "   - DAGs should appear in Airflow within 30-60 seconds"
    echo "   - Check: http://localhost:8090"
    echo "======================================================="
}

main