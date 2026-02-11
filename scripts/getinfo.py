import pandas as pd
import os

TARGET_UNITID = 193900
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data_raw')

FILES = {
    "Directory": "hd2024.csv",
    "Enrollment": "ef2024a.csv",
    "Admissions": "adm2024.csv",
    "Cost": "cost1_2024.csv",
    "FinAid": "sfa2324.csv",
    "Completions": "c2024_a.csv"
}

def load_and_extract(file_key, file_name, unitid):
    path = os.path.join(DATA_DIR, file_name)
    
    if not os.path.exists(path):
        print(f"Error: File not found at {path}")
        return None

    try:
        try:
            df = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='latin1', low_memory=False)
        
        df.columns = df.columns.str.strip().str.upper()
        
        target_rows = df[df['UNITID'] == unitid]
        
        if target_rows.empty:
            print(f"Warning: UNITID {unitid} not found in {file_name}")
            return None
            
        if file_key == "Completions":
            return target_rows
            
        return target_rows.iloc[0]
        
    except Exception as e:
        print(f"Error reading {file_name}: {e}")
        return None

def run_sanity_check():
    print(f"--- STARTING 2024 SANITY CHECK FOR UNITID: {TARGET_UNITID} ---")
    print("")

    hd_data = load_and_extract("Directory", FILES["Directory"], TARGET_UNITID)
    if hd_data is not None:
        print(f"[{FILES['Directory']}] Institution Info:")
        print(f"  Name: {hd_data.get('INSTNM', 'N/A')}")
        print(f"  City: {hd_data.get('CITY', 'N/A')}")
        print("-" * 30)

    ef_data = load_and_extract("Enrollment", FILES["Enrollment"], TARGET_UNITID)
    if ef_data is not None:
        print(f"[{FILES['Enrollment']}] Enrollment:")
        print(f"  Total Students: {ef_data.get('EFTOTLT', 'N/A')}")
        print("-" * 30)

    cost_data = load_and_extract("Cost", FILES["Cost"], TARGET_UNITID)
    if cost_data is not None:
        tuition = cost_data.get('TUITION1', 0)
        fees = cost_data.get('FEE1', 0)
        on_campus_total = cost_data.get('CINSON', 'N/A') 
        
        print(f"[{FILES['Cost']}] Cost & Tuition:")
        print(f"  Tuition:       ${tuition}")
        print(f"  Fees:          ${fees}")
        print(f"  Total On-Campus Cost: ${on_campus_total}")
        print("-" * 30)

    adm_data = load_and_extract("Admissions", FILES["Admissions"], TARGET_UNITID)
    if adm_data is not None:
        apps = adm_data.get('APPLCN', 0)
        admits = adm_data.get('ADMSSN', 0)
        
        print(f"[{FILES['Admissions']}] Admissions:")
        print(f"  Applicants: {apps}")
        if apps > 0:
            print(f"  Acceptance Rate: {(admits/apps)*100:.2f}%")
        print("-" * 30)

    sfa_data = load_and_extract("FinAid", FILES["FinAid"], TARGET_UNITID)
    if sfa_data is not None:
        print(f"[{FILES['FinAid']}] Financial Aid:")
        print(f"  % Undergrads w/ Pell Grant: {sfa_data.get('UPGRNTP', 'N/A')}%")
        print(f"  Avg Net Price ($0-$30k):   ${sfa_data.get('NPT412', 'N/A')}")
        print(f"  Avg Net Price ($48k-$75k): ${sfa_data.get('NPT432', 'N/A')}")
        print("-" * 30)

    c_data = load_and_extract("Completions", FILES["Completions"], TARGET_UNITID)
    if c_data is not None:
        print(f"[{FILES['Completions']}] Degrees Awarded (Computing):")
        
        if 'MAJORNUM' in c_data.columns:
            c_data = c_data[c_data['MAJORNUM'] == 1]
            
        c_data['CIPCODE'] = c_data['CIPCODE'].astype(str).str.strip()
        
        cs_family = c_data[c_data['CIPCODE'].str.startswith('11.')]
        
        if not cs_family.empty:
            for index, row in cs_family.iterrows():
                print(f"    - CIP {row['CIPCODE']}: {row['CTOTALT']} degrees")
            
            total_computing = cs_family['CTOTALT'].sum()
            print(f"  Total Computing Degrees (All 11.xxxx): {total_computing}")
        else:
            print("  No Computing degrees found (No CIP starting with 11.)")
        print("-" * 30)

if __name__ == "__main__":
    run_sanity_check()