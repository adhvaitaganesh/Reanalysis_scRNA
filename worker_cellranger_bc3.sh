#!/bin/bash
set -e

FASTQ_DIR=$1
OUTPUT_NAME=$2

# Your predefined paths
WORK_DIR="/scratch/chair_ccb/gahe00001/brain_human/results"
REF_PATH="/scratch/chair_ccb/gahe00001/brain_human/ref_data/refdata-gex-GRCh38-2024-A"
HOME_CR_DIR="/home/gahe00001/natpro/data_cellranger"

mkdir -p "$WORK_DIR"
cd "$WORK_DIR" || exit 1

echo "[STATUS] Starting CellRanger count for $OUTPUT_NAME..."

# Because the fastqs are named ExpName_S1_L001_R1_001.fastq.gz, the prefix is the Output Name
cellranger count --id="$OUTPUT_NAME" \
                 --transcriptome="$REF_PATH" \
                 --fastqs="$FASTQ_DIR" \
                 --sample="$OUTPUT_NAME" \
                 --create-bam=false \
                 --localcores=16 \
                 --localmem=60

echo "[STATUS] CellRanger run complete. Moving all output from /scratch to /home..."

mkdir -p "$HOME_CR_DIR"
mv "$OUTPUT_NAME" "$HOME_CR_DIR/"

echo "[CR_PIPELINE_SUCCESS] CR_FINAL_DIR=$HOME_CR_DIR/$OUTPUT_NAME"
