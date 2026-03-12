#!/bin/bash
set -e

FASTQ_DIR=$1
OUTPUT_NAME=$2
FINAL_CR_DEST=$3
DB_STATUS=$4  # Passed from the Orchestrator

# Paths specific to your cluster
WORK_DIR="/scratch/chair_ccb/gahe00001/brain_human/results"
REF_PATH="/scratch/chair_ccb/gahe00001/brain_human/ref_data/refdata-gex-GRCh38-2024-A"

mkdir -p "$WORK_DIR"
cd "$WORK_DIR" || { echo "[PIPELINE_FAILED] STAGE=INITIALIZING"; exit 1; }

# ==========================================
# MASTER-CONTROLLER INTERVENTION LOGIC
# ==========================================
if [ "$DB_STATUS" = "PENDING" ]; then
    echo "[STATUS] DB reads PENDING. Wiping old scratch data for a clean Cell Ranger run..."
    rm -rf "$OUTPUT_NAME"
    rm -f ".chk_cr_done_${OUTPUT_NAME}"
fi

# ==========================================
# CHECKPOINT 1: CELL RANGER EXECUTION
# ==========================================
if [ ! -f ".chk_cr_done_${OUTPUT_NAME}" ]; then
    echo "[PROGRESS] STAGE=CR_RUNNING"
    
    # If the folder exists but the checkpoint doesn't, Martian will natively resume it!
    cellranger count --id="$OUTPUT_NAME" \
                     --transcriptome="$REF_PATH" \
                     --fastqs="$FASTQ_DIR" \
                     --sample="$OUTPUT_NAME" \
                     --create-bam=false \
                     --localcores=16 \
                     --localmem=60 || { echo "[PIPELINE_FAILED] STAGE=CR_RUNNING"; exit 1; }
                     
    touch ".chk_cr_done_${OUTPUT_NAME}"
    echo "[PROGRESS] STAGE=CR_COMPLETED"
else
    echo "[STATUS] Checkpoint found: Cell Ranger already completed. Skipping execution."
fi

# ==========================================
# CHECKPOINT 2: TRANSFER TO HOME
# ==========================================
echo "[PROGRESS] STAGE=CR_MOVING_TO_HOME"

mkdir -p "$FINAL_CR_DEST"

if [ ! -w "$FINAL_CR_DEST" ]; then
    echo "[PIPELINE_FAILED] STAGE=CR_MOVING_TO_HOME (Permission Denied)"
    exit 1
fi

# Move the completed sample folder into the proper category folder
mv "$OUTPUT_NAME" "$FINAL_CR_DEST/" || { echo "[PIPELINE_FAILED] STAGE=CR_MOVING_TO_HOME"; exit 1; }

# Verify that the web summary file arrived safely in the home directory
if [ -f "$FINAL_CR_DEST/$OUTPUT_NAME/outs/web_summary.html" ]; then
    echo "[STATUS] Transfer verified. Output safely in home directory."
    echo "[CR_PIPELINE_SUCCESS] CR_FINAL_DIR=$FINAL_CR_DEST/$OUTPUT_NAME"
    
    # Clean up the checkpoint file so the scratch is fully clean
    rm -f ".chk_cr_done_${OUTPUT_NAME}"
else
    echo "[PIPELINE_FAILED] STAGE=CR_VERIFICATION_FAILED"
    exit 1
fi
