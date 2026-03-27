import pandas as pd
import sys
import os

hd_files = [
    "data_raw/2019/hd2019.csv",
    "data_raw/2020/hd2020.csv",
    "data_raw/2021/hd2021.csv",
    "data_raw/2022/hd2022.csv",
    "data_raw/2023/hd2023.csv",
    "data_raw/2024/hd2024.csv"
]

common_unitids = None

for file_path in hd_files:
    if os.path.exists(file_path):
        try:
            columns = pd.read_csv(file_path, nrows=0, encoding='cp1252', encoding_errors='replace').columns
            unitid_col_raw = next(col for col in columns if 'UNITID' in col.upper())
            
            df = pd.read_csv(file_path, usecols=[unitid_col_raw], encoding='cp1252', encoding_errors='replace')
            df = df.rename(columns={unitid_col_raw: 'UNITID'})
            current_unitids = set(df['UNITID'])
            
            if common_unitids is None:
                common_unitids = current_unitids
            else:
                common_unitids = common_unitids.intersection(current_unitids)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            sys.exit(1)
    else:
        print(f"Warning: The file {file_path} was not found.")

if common_unitids:
    print(f"Successfully found {len(common_unitids)} common universities across all specified years.")
    
    hd2024_path = "data_raw/2024/hd2024.csv"
    if os.path.exists(hd2024_path):
        columns_2024 = pd.read_csv(hd2024_path, nrows=0, encoding='cp1252', encoding_errors='replace').columns
        
        unitid_col_24 = next(col for col in columns_2024 if 'UNITID' in col.upper())
        instnm_col_24 = next(col for col in columns_2024 if 'INSTNM' in col.upper())
        iclevel_col_24 = next(col for col in columns_2024 if 'ICLEVEL' in col.upper())
        sector_col_24 = next(col for col in columns_2024 if 'SECTOR' in col.upper())
        active_col_24 = next(col for col in columns_2024 if 'CYACTIVE' in col.upper())
        
        df_2024 = pd.read_csv(hd2024_path, usecols=[unitid_col_24, instnm_col_24, iclevel_col_24, sector_col_24, active_col_24], encoding='cp1252', encoding_errors='replace')
        df_2024 = df_2024.rename(columns={
            unitid_col_24: 'UNITID', 
            instnm_col_24: 'INSTNM', 
            iclevel_col_24: 'ICLEVEL',
            sector_col_24: 'SECTOR', 
            active_col_24: 'CYACTIVE'
        })
        
        common_universities_df = df_2024[
            (df_2024['UNITID'].isin(common_unitids)) & 
            (df_2024['ICLEVEL'] == 1) & 
            (df_2024['SECTOR'] != 0) & 
            (df_2024['CYACTIVE'] == 1)
        ]
        
        common_universities_df = common_universities_df.drop(columns=['ICLEVEL', 'SECTOR', 'CYACTIVE'])
        
        os.makedirs("data_clean", exist_ok=True)
        
        output_path = "data_clean/common_universities_2019_2024.csv"
        common_universities_df.to_csv(output_path, index=False)
        print(f"Saved the restricted list of degree-granting common universities to {output_path}")
        print(common_universities_df.head())
else:
    print("No common universities were found, or the required files are missing.")