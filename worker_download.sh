#!/bin/sh
set -e

SRR_ID=$1
EXP_NAME=$2
FINAL_DEST_DIR=$3
DB_STATUS=$4  # The status passed from your CSV

SCRATCH_WORK_DIR="/scratch/chair_ccb/gahe00001/brain_human/tmp_${SRR_ID}"

# ==========================================
# MASTER-CONTROLLER INTERVENTION LOGIC
# ==========================================
if [ "$DB_STATUS" = "PENDING" ]; then
    echo "[STATUS] DB reads PENDING. Wiping old scratch data for fresh start..."
    rm -rf "$SCRATCH_WORK_DIR"
fi

mkdir -p "$SCRATCH_WORK_DIR"
cd "$SCRATCH_WORK_DIR" || { echo "[PIPELINE_FAILED] STAGE=INITIALIZING"; exit 1; }

# Rollback Logic: If you manually downgrade a status in the CSV, wipe downstream checkpoints
if [ "$DB_STATUS" = "DOWNLOAD_COMPLETED" ]; then 
    echo "[STATUS] DB rolled back to DOWNLOAD_COMPLETED. Forcing redo of Gzip and Rename..."
    rm -f .chk_gzipped .chk_renamed
fi
if [ "$DB_STATUS" = "GZIP_COMPLETED" ]; then 
    echo "[STATUS] DB rolled back to GZIP_COMPLETED. Forcing redo of Rename..."
    rm -f .chk_renamed
fi

# ==========================================
# 1. DOWNLOAD
# ==========================================
if [ ! -f ".chk_downloaded" ]; then
    echo "[PROGRESS] STAGE=DOWNLOADING"
    fasterq-dump --split-files --include-technical --threads 4 "$SRR_ID" || { echo "[PIPELINE_FAILED] STAGE=DOWNLOADING"; exit 1; }
    touch .chk_downloaded
    echo "[PROGRESS] STAGE=DOWNLOAD_COMPLETED"
fi

# Dynamic Split Count extraction for Orchestrator
FILE_COUNT=$(ls ${SRR_ID}_*.fastq* 2>/dev/null | wc -l || echo 0)
echo "[PROGRESS] SPLIT_COUNT=$FILE_COUNT"

# ==========================================
# 2. COMPRESSION
# ==========================================
if [ ! -f ".chk_gzipped" ]; then
    echo "[PROGRESS] STAGE=GZIPPING"
    if ls *.fastq 1> /dev/null 2>&1; then
        gzip *.fastq || { echo "[PIPELINE_FAILED] STAGE=GZIPPING"; exit 1; }
    fi
    touch .chk_gzipped
    echo "[PROGRESS] STAGE=GZIP_COMPLETED"
fi

# ==========================================
# 3. RENAMING (Dynamic Reverse-Order)
# ==========================================
if [ ! -f ".chk_renamed" ]; then
    echo "[PROGRESS] STAGE=RENAMING"
    if [ "$FILE_COUNT" -ge 2 ]; then
        R2_IDX=$FILE_COUNT
        R1_IDX=$((FILE_COUNT - 1))
        INDEX_COUNT=$((FILE_COUNT - 2))

        mv "${SRR_ID}_${R2_IDX}.fastq.gz" "${EXP_NAME}_S1_L001_R2_001.fastq.gz"
        mv "${SRR_ID}_${R1_IDX}.fastq.gz" "${EXP_NAME}_S1_L001_R1_001.fastq.gz"

        if [ "$INDEX_COUNT" -gt 0 ]; then
            for i in $(seq 1 $INDEX_COUNT); do
                mv "${SRR_ID}_${i}.fastq.gz" "${EXP_NAME}_S1_L001_I${i}_001.fastq.gz"
            done
        fi
        touch .chk_renamed
        echo "[PROGRESS] STAGE=RENAME_COMPLETED"
    else
        echo "[WARNING] Insufficient files for renaming."
    fi
fi

# ==========================================
# 4. TRANSFER TO HOME
# ==========================================
echo "[PROGRESS] STAGE=MOVING_TO_HOME"
mkdir -p "$FINAL_DEST_DIR"

if [ ! -w "$FINAL_DEST_DIR" ]; then
    echo "[PIPELINE_FAILED] STAGE=MOVING_TO_HOME_PERMISSION"
    exit 1
fi

EXPECTED_COUNT=$(ls *.fastq.gz 2>/dev/null | wc -l || echo 0)
if [ "$EXPECTED_COUNT" -gt 0 ]; then
    mv *.fastq.gz "$FINAL_DEST_DIR/" || { echo "[PIPELINE_FAILED] STAGE=MOVING_TO_HOME_TRANSFER"; exit 1; }
fi

ARRIVED_COUNT=$(ls "$FINAL_DEST_DIR"/${EXP_NAME}_S1_L001_*.fastq.gz 2>/dev/null | wc -l || echo 0)

if [ "$ARRIVED_COUNT" -ge 2 ]; then
    echo "[STATUS] Transfer verified. Cleaning scratch..."
    cd /
    rm -rf "$SCRATCH_WORK_DIR"
    echo "[PIPELINE_SUCCESS] FINAL_DIR=$FINAL_DEST_DIR"
else
    echo "[PIPELINE_FAILED] STAGE=VERIFICATION_FAILED"
    exit 1
fi
