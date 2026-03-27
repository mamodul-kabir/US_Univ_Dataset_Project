import pandas as pd
import os

def build_aggregated_dataset():
    common_path = 'data_clean/common_universities_2019_2024.csv'    
    
    if not os.path.exists(common_path):
        print(f"Base file not found: {common_path}")
        return

    df = pd.read_csv(common_path)
    df['UNITID'] = pd.to_numeric(df['UNITID'], errors='coerce').fillna(0).astype(int)
    
    file_merges = [
        {
            'path': 'data_raw/2024/hd2024.csv',
            'columns': ['UNITID', 'CYACTIVE']
        },
        {
            'path': 'data_raw/2024/adm2024.csv',
            'columns': ['UNITID', 'ADMCON7']
        },
        {
            'path': 'data_raw/2024/ic2024.csv',
            'columns': ['UNITID', 'OPENADMP']
        }
    ]
    
    for file_info in file_merges:
        file_path = file_info['path']
        target_cols = file_info['columns']
        
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        raw_columns = pd.read_csv(file_path, nrows=0, encoding='cp1252', encoding_errors='replace').columns
        
        actual_usecols = []
        col_mapping = {}
        
        for target in target_cols:
            if target == 'UNITID':
                matched_col = next((col for col in raw_columns if 'UNITID' in col.upper()), None)
            else:
                matched_col = next((col for col in raw_columns if col.strip().upper() == target), None)
                
            if matched_col:
                actual_usecols.append(matched_col)
                col_mapping[matched_col] = target
                
        if actual_usecols:
            temp_df = pd.read_csv(file_path, usecols=actual_usecols, encoding='cp1252', encoding_errors='replace')
            temp_df = temp_df.rename(columns=col_mapping)
            temp_df['UNITID'] = pd.to_numeric(temp_df['UNITID'], errors='coerce').fillna(0).astype(int)
            
            df = pd.merge(df, temp_df, on='UNITID', how='left')

    # Summary: Institution Activity
    if 'CYACTIVE' in df.columns:
        active_count = (df['CYACTIVE'] == 1).sum()
        inactive_count = df['CYACTIVE'].isin([2, 3]).sum()
        print("\n" + "="*40)
        print("INSTITUTION ACTIVITY SUMMARY (2024)")
        print("="*40)
        print(f"Active (1):          {active_count}")
        print(f"Inactive (2/3):      {inactive_count}")
        print(f"Total Records:       {len(df)}")

    # Summary: SAT/ACT Test Policy (ADMCON7)
    if 'ADMCON7' in df.columns:
        test_required = (df['ADMCON7'] == 1).sum()
        test_optional = (df['ADMCON7'] == 5).sum()
        test_blind = (df['ADMCON7'] == 3).sum()
        test_other = df['ADMCON7'].isin([2, 4]).sum()
        test_missing = df['ADMCON7'].isna().sum()

        print("\n" + "="*40)
        print("ADMISSIONS TEST POLICY SUMMARY (2024)")
        print("="*40)
        print(f"Test Required (1):   {test_required}")
        print(f"Test Optional (5):   {test_optional}")
        print(f"Test Blind (3):      {test_blind}")
        print(f"Other (2/4):         {test_other}")
        print(f"No Data:    {test_missing}")
        print("="*40 + "\n")
    
    if 'OPENADMP' in df.columns: 
        open_adm = (df['OPENADMP'] == 1).sum()
        not_open_adm = (df['OPENADMP'] == 2).sum()
        other = ((df['OPENADMP'] != 1) & (df['OPENADMP'] != 2)).sum()

        print("\n" + "="*40)
        print("OPEN ADMISSION STATS (2024)")
        print("="*40)
        print(f"Open Admission:   {open_adm}")
        print(f"Not open admission:   {not_open_adm}")
        print(f"Others:   {other}")
        print(f"Open Admission %:   {(open_adm/(open_adm + not_open_adm) * 100)} % ")
        print(f"Not open admission %:   {(not_open_adm/(open_adm + not_open_adm)* 100)} % ")
        print(f"Missing:   {(other/len(df)) * 100} %")
        
        print("="*40 + "\n")
    return df

if __name__ == '__main__':
    build_aggregated_dataset()