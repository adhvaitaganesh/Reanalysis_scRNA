import pandas as pd
import os

# --- System Path ---
DB_FILE = "/home/gahe00001/natpro/pipeline_tracker.csv"

def check_raw_syntax():
    """Checks the raw text file for missing or extra commas caused by manual editing."""
    with open(DB_FILE, 'r') as f:
        lines = f.readlines()
    
    if not lines:
        return True
        
    # CHANGED: Splitting by comma instead of semicolon
    expected_cols = len(lines[0].split(','))
    errors = []
    
    for i, line in enumerate(lines):
        # Using a very basic split here; note that real CSV parsers handle quotes, 
        # but this simple check helps catch glaringly obvious missing commas at the end of lines.
        cols = len(line.split(','))
        if cols != expected_cols:
            errors.append(f"Line {i+1} has {cols} columns (expected {expected_cols}).")
            
    if errors:
        print("\n[FATAL] STRUCTURAL CSV ERRORS DETECTED:")
        for err in errors:
            print(f"  - {err}")
        print("\nPlease open the CSV in a text editor and fix the missing or extra commas.")
        print("The script cannot safely automatically fix structural delimiter errors.")
        return False
        
    return True

def main():
    print(f"Scanning {DB_FILE}...\n")
    
    if not os.path.exists(DB_FILE):
        print(f"Error: {DB_FILE} not found.")
        return

    # 1. Structural Check
    if not check_raw_syntax():
        return

    # 2. Load Data for Logical Checks (CHANGED: sep=',')
    df = pd.read_csv(DB_FILE, sep=',', dtype=str).fillna('')
    
    issues_found = []
    
    if 'Is_Duplicate' not in df.columns:
        issues_found.append("Add missing 'Is_Duplicate' column.")

    cols_to_sanitize = ['Experiment_Name', 'Condition_Group', 'Category', 'Sample_Name']
    space_count = 0
    for col in cols_to_sanitize:
        if col in df.columns:
            mask = df[col].str.contains(' ', na=False) | (df[col] != df[col].str.strip())
            space_count += mask.sum()
            
    if space_count > 0:
        issues_found.append(f"Sanitize spaces to underscores ('_') in {space_count} path-critical fields.")

    if 'Category' in df.columns:
        legacy_cat_fixes = df['Category'].str.contains('all cells', case=False).sum()
        if legacy_cat_fixes > 0:
            issues_found.append(f"Rename 'all cells' to 'unsorted' in {legacy_cat_fixes} entries.")

    df_sim = df.copy()
    if 'Experiment_Name' in df_sim.columns:
        df_sim['Experiment_Name'] = df_sim['Experiment_Name'].str.strip().str.replace(' ', '_')
        dup_mask = df_sim.duplicated(subset=['Experiment_Name'], keep='first') & (df_sim['Experiment_Name'] != '')
        dup_count = dup_mask.sum()
        
        if dup_count > 0:
            issues_found.append(f"Flag {dup_count} duplicate Experiment_Name(s) as 'COMPLETED' and mark as 'duplicate'.")

    # 3. Report Phase
    if not issues_found:
        print("[SUCCESS] Database is perfectly clean! No formatting errors or duplicates found.")
        return

    print("--- Database Validation Report ---")
    for i, issue in enumerate(issues_found, 1):
        print(f"{i}. {issue}")

    # 4. Approval Phase
    ans = input("\nDo you approve and want to apply these changes? [y/N]: ")
    
    if ans.lower() != 'y':
        print("Aborted. No changes were made to the database.")
        return

    # 5. Execution Phase (Write Changes)
    if 'Is_Duplicate' not in df.columns:
        df['Is_Duplicate'] = ''
        
    for col in cols_to_sanitize:
        if col in df.columns:
            df[col] = df[col].str.strip().str.replace(' ', '_')

    if 'Category' in df.columns:
        df['Category'] = df['Category'].str.replace('all cells', 'unsorted', case=False)

    if 'Experiment_Name' in df.columns:
        dup_mask = df.duplicated(subset=['Experiment_Name'], keep='first') & (df['Experiment_Name'] != '')
        df.loc[dup_mask, 'DL_Status'] = 'COMPLETED'
        df.loc[dup_mask, 'Is_Duplicate'] = 'duplicate'

    # Save to file (CHANGED: sep=',')
    df.to_csv(DB_FILE, index=False, sep=',')
    print(f"\n[SUCCESS] Changes applied and saved safely to {DB_FILE}!")

if __name__ == "__main__":
    main()
