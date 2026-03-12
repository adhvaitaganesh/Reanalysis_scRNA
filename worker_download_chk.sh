#!/bin/sh
# Exit immediately if a command exits with a non-zero status
set -e

SRR_ID=$1
EXP_NAME=$2
FINAL_DEST_DIR=$3

SCRATCH_WORK_DIR="/scratch/chair_ccb/gahe00001/brain_human/tmp_${SRR_ID}"

echo "[STATUS] Setting up scratch workspace: $SCRATCH_WORK_DIR"
mkdir -p "$SCRATCH_WORK_DIR"
cd "$SCRATCH_WORK_DIR" || exit 1

# ==========================================
# CHECKPOINT 1: DOWNLOAD
# ==========================================
if [ ! -f ".chk_downloaded" ]; then
    echo "[STATUS] Downloading and splitting $SRR_ID..."
    fasterq-dump --split-files --include-technical --threads 4 "$SRR_ID"
    # Mark as successfully downloaded
    touch .chk_downloaded
else
    echo "[STATUS] Checkpoint found: SRA download already completed. Skipping."
fi

# ==========================================
# CHECKPOINT 2: COMPRESSION
# ==========================================
if [ ! -f ".chk_gzipped" ]; then
    echo "[STATUS] Gzipping files..."
    # Only try to gzip if .fastq files actually exist (prevents errors on restart)
    if ls *.fastq 1> /dev/null 2>&1; then
        gzip *.fastq
    fi
    # Mark as successfully gzipped
    touch .chk_gzipped
else
    echo "[STATUS] Checkpoint found: Files already gzipped. Skipping."
fi

# ==========================================
# CHECKPOINT 3: RENAMING
# ==========================================
if [ ! -f ".chk_renamed" ]; then
    echo "[STATUS] Applying dynamic reverse-order renaming..."
    FILE_COUNT=$(ls ${SRR_ID}_*.fastq.gz 2>/dev/null | wc -l || echo 0)
    
    if [ "$FILE_COUNT" -ge 2 ]; then
        R2_IDX=$FILE_COUNT
        R1_IDX=$((FILE_COUNT - 1))
        INDEX_COUNT=$((FILE_COUNT - 2))

        echo " -> Renaming last file (_${R2_IDX}) to R2..."
        mv "${SRR_ID}_${R2_IDX}.fastq.gz" "${EXP_NAME}_S1_L001_R2_001.fastq.gz"
        
        echo " -> Renaming second-to-last file (_${R1_IDX}) to R1..."
        mv "${SRR_ID}_${R1_IDX}.fastq.gz" "${EXP_NAME}_S1_L001_R1_001.fastq.gz"

        if [ "$INDEX_COUNT" -gt 0 ]; then
            for i in $(seq 1 $INDEX_COUNT); do
                echo " -> Renaming file _${i} to Index I${i}..."
                mv "${SRR_ID}_${i}.fastq.gz" "${EXP_NAME}_S1_L001_I${i}_001.fastq.gz"
            done
        fi
        # Mark as successfully renamed
        touch .chk_renamed
    else
        echo "[WARNING] Less than 2 files found for renaming. Assuming files were already renamed or moved."
    fi
else
    echo "[STATUS] Checkpoint found: Files already renamed. Skipping."
fi

# ==========================================
# CHECKPOINT 4: TRANSFER TO HOME
# ==========================================
echo "[STATUS] Preparing transfer to $FINAL_DEST_DIR"
mkdir -p "$FINAL_DEST_DIR"

if [ ! -w "$FINAL_DEST_DIR" ]; then
    echo "[PIPELINE_FAILED] Cannot write to destination directory: $FINAL_DEST_DIR"
    exit 1
fi

EXPECTED_COUNT=$(ls *.fastq.gz 2>/dev/null | wc -l || echo 0)

if [ "$EXPECTED_COUNT" -gt 0 ]; then
    echo "[STATUS] Moving $EXPECTED_COUNT files to permanent home storage..."
    mv *.fastq.gz "$FINAL_DEST_DIR/"
fi

# Final Verification: Check if the files safely exist in the home directory
ARRIVED_COUNT=$(ls "$FINAL_DEST_DIR"/${EXP_NAME}_S1_L001_*.fastq.gz 2>/dev/null | wc -l || echo 0)

if [ "$ARRIVED_COUNT" -ge 2 ]; then
    echo "[STATUS] Transfer verified ($ARRIVED_COUNT files found in destination). Cleaning scratch..."
    cd /
    rm -rf "$SCRATCH_WORK_DIR"
    echo "[PIPELINE_SUCCESS] FINAL_DIR=$FINAL_DEST_DIR"
else
    echo "[PIPELINE_FAILED] Transfer verification failed! Found only $ARRIVED_COUNT correctly named files in $FINAL_DEST_DIR."
    exit 1
fi
