#!/bin/bash
set -e

SAMPLE=$1
CR_DIR=$2
FINAL_SP_DEST=$3
DB_STATUS=$4
COND_GROUP=$5
CATEGORY=$6

# Helper to manage granular breadcrumbs
set_status() {
    rm -f "$FINAL_SP_DEST/.sp_status_"*
    touch "$FINAL_SP_DEST/.sp_status_$1"
}

WORK_DIR="/scratch/chair_ccb/gahe00001/brain_human/scanpy_results/${SAMPLE}"
mkdir -p "$WORK_DIR"
mkdir -p "$FINAL_SP_DEST"
cd "$WORK_DIR" || { set_status "FAILED_INIT"; exit 1; }

# ==========================================
# MASTER-CONTROLLER INTERVENTION LOGIC
# ==========================================
if [[ "$DB_STATUS" == "PENDING" || "$DB_STATUS" == FAILED* ]]; then
    echo "[STATUS] DB reads $DB_STATUS. Wiping old scratch/checkpoints for a clean run..."
    rm -f "${SAMPLE}_qc.h5ad"
    rm -f "$FINAL_SP_DEST/.sp_status_"*
fi

# ==========================================
# CHECKPOINT 1: SCANPY EXECUTION
# ==========================================
set_status "RUNNING"
echo "[PROGRESS] STAGE=SCANPY_RUNNING"

# Execute the python script inside the docker container
python3 scanpy_qc.py \
    --sample "$SAMPLE" \
    --cr_dir "$CR_DIR" \
    --cond "$COND_GROUP" \
    --cat "$CATEGORY" \
    --out_file "${SAMPLE}_qc.h5ad"

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    if [ $EXIT_CODE -eq 137 ]; then 
        set_status "FAILED_OOM"
    else
        set_status "FAILED_PYTHON"
    fi
    exit 1
fi

# ==========================================
# CHECKPOINT 2: TRANSFER TO HOME
# ==========================================
echo "[PROGRESS] STAGE=MOVING_TO_HOME"

if [ ! -w "$FINAL_SP_DEST" ]; then
    set_status "FAILED_IO"
    exit 1
fi

mv "${SAMPLE}_qc.h5ad" "$FINAL_SP_DEST/" || { set_status "FAILED_IO"; exit 1; }

if [ -f "$FINAL_SP_DEST/${SAMPLE}_qc.h5ad" ]; then
    echo "[STATUS] Transfer verified. Output safely in home directory."
    set_status "COMPLETED"
    
    # Cleanup scratch to save space
    rm -rf "$WORK_DIR"
    exit 0
else
    set_status "FAILED_IO"
    exit 1
fi
