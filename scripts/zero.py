import pandas as pd
import os

def track_unique_tuition_uids():
    common_path = 'data_clean/common_universities_2019_2024.csv'
    file1_path = 'data_raw/2024/cost1_2024.csv'
    file2_path = 'data_freq/2024/drvcost2024.csv'

    if not all(os.path.exists(p) for p in [common_path, file1_path, file2_path]):
        print("One or more required files are missing.")
        return

    df_common = pd.read_csv(common_path, usecols=['UNITID'])
    df_common['UNITID'] = pd.to_numeric(df_common['UNITID'], errors='coerce').fillna(0).astype(int)
    valid_uids = set(df_common['UNITID'])

    df_cost1 = pd.read_csv(file1_path, usecols=['UNITID', 'TUITION1'], encoding='cp1252', encoding_errors='replace')
    df_cost1['UNITID'] = pd.to_numeric(df_cost1['UNITID'], errors='coerce').fillna(0).astype(int)
    
    df_drvcost = pd.read_csv(file2_path, usecols=['UNITID', 'TUFEYR3'], encoding='cp1252', encoding_errors='replace')
    df_drvcost['UNITID'] = pd.to_numeric(df_drvcost['UNITID'], errors='coerce').fillna(0).astype(int)

    # Filter to common UIDs AND drop rows where the target metric is missing (NaN)
    valid_cost1_uids = set(df_cost1[df_cost1['UNITID'].isin(valid_uids) & df_cost1['TUITION1'].notna()]['UNITID'])
    valid_drvcost_uids = set(df_drvcost[df_drvcost['UNITID'].isin(valid_uids) & df_drvcost['TUFEYR3'].notna()]['UNITID'])

    # Find the unique IDs using set differences
    only_in_cost1 = valid_cost1_uids - valid_drvcost_uids
    only_in_drvcost = valid_drvcost_uids - valid_cost1_uids

    print(f"UIDs with data ONLY in {file1_path} (TUITION1): {len(only_in_cost1)}")
    print(f"UIDs with data ONLY in {file2_path} (TUFEYR3): {len(only_in_drvcost)}")

    # Save the results to CSV files for inspection
    if len(only_in_cost1) > 0:
        pd.DataFrame({'UNITID': list(only_in_cost1)}).to_csv('unique_to_cost1.csv', index=False)
        print("-> Saved unique IDs to 'unique_to_cost1.csv'")
        
    if len(only_in_drvcost) > 0:
        pd.DataFrame({'UNITID': list(only_in_drvcost)}).to_csv('unique_to_drvcost.csv', index=False)
        print("-> Saved unique IDs to 'unique_to_drvcost.csv'")

if __name__ == '__main__':
    track_unique_tuition_uids()