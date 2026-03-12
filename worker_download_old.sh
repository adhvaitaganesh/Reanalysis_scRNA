#!/bin/sh
set -e

SRR_ID=$1
EXP_NAME=$2
FINAL_DEST_DIR=$3

# Utilizing your specific scratch directory
SCRATCH_WORK_DIR="/scratch/chair_ccb/gahe00001/brain_human/tmp_${SRR_ID}"

echo "[STATUS] Setting up scratch workspace: $SCRATCH_WORK_DIR"
mkdir -p "$SCRATCH_WORK_DIR"
cd "$SCRATCH_WORK_DIR" || exit 1

echo "[STATUS] Downloading and splitting $SRR_ID..."
fasterq-dump --split-files --include-technical --threads 4 "$SRR_ID"

echo "[STATUS] Gzipping and Renaming to CellRanger format..."
gzip *.fastq

[ -f "${SRR_ID}_1.fastq.gz" ] && mv "${SRR_ID}_1.fastq.gz" "${EXP_NAME}_S1_L001_I1_001.fastq.gz"
[ -f "${SRR_ID}_2.fastq.gz" ] && mv "${SRR_ID}_2.fastq.gz" "${EXP_NAME}_S1_L001_R1_001.fastq.gz"
[ -f "${SRR_ID}_3.fastq.gz" ] && mv "${SRR_ID}_3.fastq.gz" "${EXP_NAME}_S1_L001_R2_001.fastq.gz"

echo "[STATUS] Moving files to permanent home storage..."
mkdir -p "$FINAL_DEST_DIR"
mv *.fastq.gz "$FINAL_DEST_DIR/"

cd /
rm -rf "$SCRATCH_WORK_DIR"

echo "[PIPELINE_SUCCESS] FINAL_DIR=$FINAL_DEST_DIR"
