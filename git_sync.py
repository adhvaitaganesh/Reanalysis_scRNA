import os
import pandas as pd
import subprocess
import hashlib
from datetime import datetime

# --- System Paths ---
HOME_DIR = "/home/gahe00001/natpro"
os.chdir(HOME_DIR)

TRACKER_FILE = "pipeline_tracker.csv"
REGISTRY_FILE = "output_registry.csv"
CR_BASE_DIR = "data_cellranger"

def get_md5(file_path):
    """Calculates the MD5 checksum of a file to detect changes/corruption."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None

def run_git_cmd(cmd):
    """Executes a Git command and returns the output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()

def init_registry():
    """Initializes the output registry if it doesn't exist."""
    if not os.path.exists(REGISTRY_FILE):
        cols = ['Sample_Name', 'Condition_Group', 'Category', 'File_Type', 
                'Relative_Path', 'MD5_Checksum', 'Git_Commit_Hash', 'Pushed_To_Remote']
        df = pd.DataFrame(columns=cols)
        df.to_csv(REGISTRY_FILE, index=False, sep=',')
        print(f"[*] Initialized new data ledger: {REGISTRY_FILE}")

def sync_to_git():
    init_registry()
    
    # Load databases
    tracker_df = pd.read_csv(TRACKER_FILE, sep=',', dtype=str).fillna('')
    registry_df = pd.read_csv(REGISTRY_FILE, sep=',', dtype=str).fillna('')
    
    files_to_sync = []
    
    # 1. Find all COMPLETED Cell Ranger jobs
    cr_completed = tracker_df[tracker_df['CR_Status'] == 'COMPLETED']
    
    if cr_completed.empty:
        print("No completed Cell Ranger jobs found to sync.")
        return

    print(f"[*] Scanning {len(cr_completed)} completed samples for Web Summaries...")

    # 2. Locate files and calculate hashes
    for _, row in cr_completed.iterrows():
        sample = str(row['Sample_Name'])
        cond = str(row['Condition_Group'])
        cat = str(row['Category'])
        
        # Path: data_cellranger/Condition/Category/Sample/outs/web_summary.html
        rel_path = os.path.join(CR_BASE_DIR, cond, cat, sample, "outs", "web_summary.html")
        
        if os.path.exists(rel_path):
            current_md5 = get_md5(rel_path)
            
            # Check if it's already in the registry and pushed
            reg_match = registry_df[registry_df['Relative_Path'] == rel_path]
            
            if reg_match.empty or reg_match.iloc[0]['MD5_Checksum'] != current_md5 or reg_match.iloc[0]['Pushed_To_Remote'] == '':
                files_to_sync.append({
                    'Sample_Name': sample,
                    'Condition_Group': cond,
                    'Category': cat,
                    'File_Type': 'Web_Summary',
                    'Relative_Path': rel_path,
                    'MD5_Checksum': current_md5
                })

    if not files_to_sync:
        print("[*] All Web Summaries are already synced and up to date!")
        return

    print(f"[*] Found {len(files_to_sync)} new or updated Web Summaries to sync.")

    # 3. Stage files in Git
    # We stage the scripts and CSVs normally
    run_git_cmd(f"git add orchestrator.py validate_db.py git_sync.py worker_*.sh {TRACKER_FILE} {REGISTRY_FILE}")
    
    # We FORCE add the web summaries so they bypass the .gitignore
    for f in files_to_sync:
        print(f" -> Staging: {f['Relative_Path']}")
        run_git_cmd(f"git add -f {f['Relative_Path']}")

    # 4. Commit Changes
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_msg = f"Automated Sync: Added/Updated {len(files_to_sync)} Web Summaries ({timestamp})"
    
    code, stdout, stderr = run_git_cmd(f'git commit -m "{commit_msg}"')
    if code != 0 and "nothing to commit" not in stdout:
        print(f"[ERROR] Git Commit Failed:\n{stderr}")
        return

    # Get the commit hash we just created
    _, commit_hash, _ = run_git_cmd("git rev-parse HEAD")
    print(f"[*] Created Commit: {commit_hash[:7]}")

    # 5. Push to Remote
    print("[*] Pushing to GitHub remote...")
    code, stdout, stderr = run_git_cmd("git push origin main") # Assumes branch is 'main'
    
    if code == 0 or "Everything up-to-date" in stderr:
        print("[SUCCESS] Successfully pushed to remote!")
        
        # 6. Update the Registry Database
        for f in files_to_sync:
            # Remove old entry if exists, append new one
            registry_df = registry_df[registry_df['Relative_Path'] != f['Relative_Path']]
            
            new_row = pd.DataFrame([{
                'Sample_Name': f['Sample_Name'],
                'Condition_Group': f['Condition_Group'],
                'Category': f['Category'],
                'File_Type': f['File_Type'],
                'Relative_Path': f['Relative_Path'],
                'MD5_Checksum': f['MD5_Checksum'],
                'Git_Commit_Hash': commit_hash,
                'Pushed_To_Remote': timestamp
            }])
            registry_df = pd.concat([registry_df, new_row], ignore_index=True)
            
        registry_df.to_csv(REGISTRY_FILE, index=False, sep=',')
        
        # Stage and commit the updated registry one last time so the remote has the timestamps
        run_git_cmd(f"git add {REGISTRY_FILE}")
        run_git_cmd('git commit -m "Update output registry with sync timestamps"')
        run_git_cmd("git push origin main")
        
        print("[*] Ledger updated and synced.")
    else:
        print(f"[ERROR] Failed to push to remote. Check your internet or Git authentication.\n{stderr}")

if __name__ == "__main__":
    sync_to_git()
