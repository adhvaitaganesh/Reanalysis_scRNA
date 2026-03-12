import pandas as pd
import argparse
import subprocess
import os
import re

# --- System Paths ---
HOME_DIR = "/home/gahe00001/natpro"
SCRATCH_DIR = "/scratch/chair_ccb/gahe00001/brain_human"

# --- Directory Lock ---
os.chdir(HOME_DIR)

DB_FILE = os.path.join(HOME_DIR, "pipeline_tracker.csv")
LOG_DIR_DL = os.path.join(HOME_DIR, "logs/download")
LOG_DIR_CR = os.path.join(HOME_DIR, "logs/cellranger")
HOME_FASTQ_DIR = os.path.join(HOME_DIR, "data_fastqs")
HOME_CR_DIR = os.path.join(HOME_DIR, "data_cellranger")

def auto_heal_db():
    if os.path.exists(DB_FILE):
        df = pd.read_csv(DB_FILE, sep=',', dtype=str).fillna('')
        if 'Category' in df.columns:
            df['Category'] = df['Category'].str.replace('all cells', 'unsorted', case=False)
        cols_to_sanitize = ['Experiment_Name', 'Condition_Group', 'Category', 'Sample_Name']
        for col in cols_to_sanitize:
            if col in df.columns:
                df[col] = df[col].str.strip().str.replace(' ', '_')
        df.to_csv(DB_FILE, index=False, sep=',')

def upgrade_db():
    if not os.path.exists(DB_FILE):
        print(f"No existing {DB_FILE} found to upgrade. Please use --init instead.")
        return
    df = pd.read_csv(DB_FILE, sep=',', dtype=str).fillna('')
    if 'Split_Count' not in df.columns:
        df['Split_Count'] = ''
        print(" -> Added 'Split_Count' column.")
    if 'Is_Duplicate' not in df.columns:
        df['Is_Duplicate'] = ''
        print(" -> Added 'Is_Duplicate' column.")
    df.to_csv(DB_FILE, index=False, sep=',')
    auto_heal_db()
    print(f"Successfully upgraded {DB_FILE} to the full Master-Controller format!")

def init_db(original_csv):
    if os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} already exists. Use --upgrade-db to update it.")
        return
    df = pd.read_csv(original_csv, sep=',', dtype=str).fillna('')
    db = df[['SRR_ID', 'Experiment_Name', 'Condition_Group', 'Category']].copy()
    db['Sample_Name'] = db['Experiment_Name']
    db['DL_Status'] = 'PENDING'
    db['DL_JobID'], db['Fastq_Dir'], db['Split_Count'] = '', '', ''
    db['CR_Status'] = 'PENDING'
    db['CR_JobID'], db['CR_Out_Dir'] = '', ''
    db['Is_Duplicate'] = ''
    db.to_csv(DB_FILE, index=False, sep=',')
    auto_heal_db()
    print(f"Initialized database {DB_FILE}")

def submit_downloads(count, category):
    auto_heal_db()
    df = pd.read_csv(DB_FILE, sep=',', dtype=str).fillna('')
    
    def is_eligible(row):
        # Do not download if flagged as duplicate
        if str(row.get('Is_Duplicate', '')).strip().lower() == 'duplicate': return False
        
        s = str(row.get('DL_Status', '')).strip()
        if s == 'COMPLETED': return False
        if s.startswith('SUBMITTED') or s.startswith('RUNNING'): return False
        return True

    mask = df.apply(is_eligible, axis=1)
    
    if category:
        mask = mask & (df['Category'].str.lower() == category.lower())
    
    pending_jobs = df[mask].head(count)
    if pending_jobs.empty:
        print(f"No eligible jobs found to submit or resume (Category filter: {category if category else 'None'}).")
        return

    os.makedirs(LOG_DIR_DL, exist_ok=True)
    os.makedirs(HOME_FASTQ_DIR, exist_ok=True)

    for index, row in pending_jobs.iterrows():
        srr = str(row['SRR_ID'])
        exp_name = str(row['Experiment_Name'])
        cond = str(row['Condition_Group'])
        cat = str(row['Category'])
        db_status = str(row['DL_Status'])
        
        final_dest = os.path.join(HOME_FASTQ_DIR, cond, cat, exp_name)

        sub_content = f"""universe = docker
docker_image = ncbi/sra-tools:latest
+WantScratchMounted = true
+WantGPUHomeMounted = true
executable = /bin/sh
arguments = worker_download.sh {srr} {exp_name} {final_dest} {db_status}
transfer_input_files = worker_download.sh
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
stream_output = True
stream_error = True
output = {LOG_DIR_DL}/{srr}_$(Cluster).out
error = {LOG_DIR_DL}/{srr}_$(Cluster).err
log = {LOG_DIR_DL}/{srr}_$(Cluster).log
request_cpus = 4
request_memory = 16GB
request_disk = 100GB
queue 1
"""
        sub_filename = f".tmp_sub_dl_{srr}.sub"
        with open(sub_filename, 'w') as f: f.write(sub_content)

        print(f"Submitting {srr} [State: {db_status}] -> Category: {cat}")
        result = subprocess.run(['condor_submit', sub_filename], capture_output=True, text=True)
        
        match = re.search(r'cluster (\d+)', result.stdout)
        if match:
            job_id = match.group(1)
            print(f" -> Assigned DL JobID: {job_id}")
            df.at[index, 'DL_Status'] = 'SUBMITTED'
            df.at[index, 'DL_JobID'] = str(job_id)
        else:
            print(f" -> Failed: {result.stderr}")
        os.remove(sub_filename)

    df.to_csv(DB_FILE, index=False, sep=',')

def submit_cellranger(count, category):
    auto_heal_db()
    df = pd.read_csv(DB_FILE, sep=',', dtype=str).fillna('')
    
    def is_cr_eligible(row):
        # 1. Reject if marked as a duplicate
        if str(row.get('Is_Duplicate', '')).strip().lower() == 'duplicate': 
            return False
            
        # 2. Reject if the download is not fully completed
        if str(row.get('DL_Status', '')).strip() != 'COMPLETED': 
            return False
            
        # 3. Reject if there is no registered fastq path in the database
        if str(row.get('Fastq_Dir', '')).strip() == '': 
            return False
            
        # 4. Reject if Cell Ranger is already completed or currently running
        cr_status = str(row.get('CR_Status', '')).strip()
        if cr_status == 'COMPLETED': return False
        if cr_status.startswith('SUBMITTED') or cr_status.startswith('RUNNING'): return False
        
        return True

    # Apply the strict eligibility filter
    mask = df.apply(is_cr_eligible, axis=1)
    
    # Apply the Category filter if requested
    if category:
        mask = mask & (df['Category'].str.lower() == category.lower())
    
    pending_jobs = df[mask].head(count)
    if pending_jobs.empty:
        print(f"No eligible CellRanger jobs found to submit or resume (Category filter: {category if category else 'None'}).")
        return

    os.makedirs(LOG_DIR_CR, exist_ok=True)
    os.makedirs(HOME_CR_DIR, exist_ok=True)

    for index, row in pending_jobs.iterrows():
        sample_name = str(row['Sample_Name'])
        fastq_dir = str(row['Fastq_Dir'])
        
        cond = str(row['Condition_Group'])
        cat = str(row['Category'])
        db_status = str(row['CR_Status']) 
        
        final_cr_dest = os.path.join(HOME_CR_DIR, cond, cat)

        sub_content = f"""universe = docker
docker_image = etycksen/cellranger:latest
request_cpus = 16
request_memory = 64GB
request_disk = 50GB
+WantScratchMounted = true
+WantGPUHomeMounted = true
log = {LOG_DIR_CR}/{sample_name}_$(Cluster).log
output = {LOG_DIR_CR}/{sample_name}_$(Cluster).out
error = {LOG_DIR_CR}/{sample_name}_$(Cluster).err
executable = worker_cellranger.sh
arguments = {fastq_dir} {sample_name} {final_cr_dest} {db_status}
transfer_input_files = worker_cellranger.sh
queue 1
"""
        sub_filename = f".tmp_sub_cr_{sample_name}.sub"
        with open(sub_filename, 'w') as f: f.write(sub_content)

        print(f"Submitting CellRanger for {sample_name} -> {cond}/{cat}")
        result = subprocess.run(['condor_submit', sub_filename], capture_output=True, text=True)
        
        match = re.search(r'cluster (\d+)', result.stdout)
        if match:
            df.at[index, 'CR_Status'] = 'SUBMITTED'
            df.at[index, 'CR_JobID'] = str(match.group(1))
        os.remove(sub_filename)

    df.to_csv(DB_FILE, index=False, sep=',')

def update_status():
    auto_heal_db()
    df = pd.read_csv(DB_FILE, sep=',', dtype=str).fillna('')
    
    # Update Downloads
    for index, row in df[df['DL_Status'].str.contains('SUBMITTED|RUNNING')].iterrows():
        out_file = os.path.join(LOG_DIR_DL, f"{row['SRR_ID']}_{row['DL_JobID']}.out")
        if os.path.exists(out_file):
            with open(out_file, 'r') as f: content = f.read()
            
            split_match = re.search(r'\[PROGRESS\] SPLIT_COUNT=(\d+)', content)
            if split_match: df.at[index, 'Split_Count'] = split_match.group(1)

            if "[PIPELINE_SUCCESS]" in content:
                path_match = re.search(r'\[PIPELINE_SUCCESS\] FINAL_DIR=(.+)', content)
                if path_match:
                    df.at[index, 'Fastq_Dir'] = str(path_match.group(1).strip())
                    df.at[index, 'DL_Status'] = 'COMPLETED'
            elif "[PIPELINE_FAILED]" in content:
                stage_match = re.search(r'\[PIPELINE_FAILED\] STAGE=(.+)', content)
                failed_stage = stage_match.group(1).strip() if stage_match else "UNKNOWN"
                df.at[index, 'DL_Status'] = f"FAILED_AT_{failed_stage}"
            else:
                stage_matches = re.findall(r'\[PROGRESS\] STAGE=(.+)', content)
                if stage_matches:
                    latest_stage = stage_matches[-1].strip()
                    if "COMPLETED" in latest_stage:
                        df.at[index, 'DL_Status'] = latest_stage
                    else:
                        df.at[index, 'DL_Status'] = f"RUNNING_{latest_stage}"

    # Update Cell Ranger
    for index, row in df[df['CR_Status'].str.contains('SUBMITTED|RUNNING')].iterrows():
        out_file = os.path.join(LOG_DIR_CR, f"{row['Sample_Name']}_{row['CR_JobID']}.out")
        if os.path.exists(out_file):
            with open(out_file, 'r') as f: content = f.read()
            if "[CR_PIPELINE_SUCCESS]" in content:
                match = re.search(r'\[CR_PIPELINE_SUCCESS\] CR_FINAL_DIR=(.+)', content)
                if match:
                    df.at[index, 'CR_Out_Dir'] = str(match.group(1).strip())
                    df.at[index, 'CR_Status'] = 'COMPLETED'
            elif "Pipestance failed" in content or "error" in content.lower():
                df.at[index, 'CR_Status'] = 'FAILED'
            else:
                # Capture running stages if the wrapper broadcasts them
                stage_matches = re.findall(r'\[PROGRESS\] STAGE=(.+)', content)
                if stage_matches:
                    latest_stage = stage_matches[-1].strip()
                    df.at[index, 'CR_Status'] = f"RUNNING_{latest_stage}"

    df.to_csv(DB_FILE, index=False, sep=',')
    print("Database synced with cluster logs!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--init', metavar='CSV_FILE')
    parser.add_argument('--upgrade-db', action='store_true')
    parser.add_argument('--submit', choices=['download', 'cellranger'])
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--category', type=str)
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    if args.init: init_db(args.init)
    elif args.upgrade_db: upgrade_db()
    elif args.submit == 'download': submit_downloads(args.count, args.category)
    # The fix: passing args.category down to submit_cellranger
    elif args.submit == 'cellranger': submit_cellranger(args.count, args.category)
    elif args.update: update_status()
