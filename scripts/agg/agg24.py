import pandas as pd
import os

def build_aggregated_dataset():
    common_path = 'data_clean/common_universities_2019_2024.csv'
    output_path = 'data_final/agg2024.csv'
    
    os.makedirs('data_final', exist_ok=True)
    
    df = pd.read_csv(common_path)
    df['UNITID'] = pd.to_numeric(df['UNITID'], errors='coerce').fillna(0).astype(int)
    
    file_merges = [
        {
            'path': 'data_raw/2024/hd2024.csv',
            'columns': ['UNITID', 'HLOFFER', 'CARNEGIERSCH']
        }, 
        {
            'path' : 'data_freq/2024/drvadm2024.csv',
            'columns' : ['UNITID', 'DVADM01', 'DVADM04']
        }, 
        {
            'path': 'data_raw/2024/adm2024.csv',
            'columns': ['UNITID', 'SATPCT', 'SATVR50', 'SATMT50', 'ACTPCT', 'ACTCM50']
        }, 
        {
            'path' : 'data_raw/2024/cost1_2024.csv',
            'columns' : ['UNITID', 'TUITION1']
        }, 
        {
            'path' : 'data_freq/2024/drvom2024.csv',
            'columns' : ['UNITID', 'OM1TOTLAWDP4', 'OM1TOTLAWDP8', 'OM1TOTLENAP8']
        },
        {
            'path' : 'data_freq/2024/drvhr2024.csv',
            'columns' : ['UNITID', 'SALTOTL', 'SFTETOTL', 'SFTEINST']
        },
        {
            'path' : 'data_freq/2024/drvgr2024.csv',
            'columns' : ['UNITID', 'GRRTTOT', 'TRRTTOT', 'GBA6RTT', 'PGBA6RT', 'NRBA6RT']
        },
        {
            'path' : 'data_freq/2024/drvf2024.csv',
            'columns' : [
                'UNITID', 
                'F1COREXP', 'F2COREXP', 'F3COREXP',
                'F1INSTFT', 'F2INSTFT', 'F3INSTFT',
                'F1TUFEPC', 'F2TUFEPC', 'F3TUFEPC',
                'F1INSTPC', 'F2INSTPC', 'F3INSTPC'
            ]
        }, 
        {
            'path' : 'data_freq/2024/drvef2024.csv',
            'columns' : ['UNITID', 'ENRTOT', 'EFUG']
        },
        {
            'path' : 'data_raw/2024/ef2024d.csv',
            'columns' : ['UNITID', 'STUFACR']
        },
        {
            'path' : 'data_freq/2024/drvc2024.csv',
            'columns' : [
                'UNITID',
                'ASCDEG', 
                'BASDEG',
                'MASDEG',
                'DOCDEGRS',
                'DOCDEGPP',
                'DOCDEGOT'
            ]
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

    finance_map = {
        'COREXP': ['F1COREXP', 'F2COREXP', 'F3COREXP'],
        'INSTFT': ['F1INSTFT', 'F2INSTFT', 'F3INSTFT'],
        'TUFEPC': ['F1TUFEPC', 'F2TUFEPC', 'F3TUFEPC'],
        'INSTPC': ['F1INSTPC', 'F2INSTPC', 'F3INSTPC']
    }

    for new_col, sources in finance_map.items():
        available_sources = [s for s in sources if s in df.columns]
        if available_sources:
            first_source_idx = df.columns.get_loc(available_sources[0])
            combined_data = df[available_sources].bfill(axis=1).iloc[:, 0]
            df = df.drop(columns=available_sources)
            df.insert(first_source_idx, new_col, combined_data)

    if 'CARNEGIERSCH' in df.columns:
        df['IS_R1'] = (df['CARNEGIERSCH'] == 1).astype(int)
        cols = list(df.columns)
        idx1, idx2 = cols.index('IS_R1'), cols.index('CARNEGIERSCH')
        cols[idx1], cols[idx2] = cols[idx2], cols[idx1]
        df = df[cols]
        df = df.drop(columns=['CARNEGIERSCH'])

    if 'SATVR50' in df.columns and 'SATMT50' in df.columns:
        df['SATCM50'] = df['SATVR50'] + df['SATMT50']
        cols = list(df.columns)
        idx1, idx2 = cols.index('SATCM50'), cols.index('SATVR50')
        cols[idx1], cols[idx2] = cols[idx2], cols[idx1]
        df = df[cols]
        df = df.drop(columns=['SATVR50', 'SATMT50'])
    
    if 'DOCDEGRS' in df.columns and 'DOCDEGPP' in df.columns and 'DOCDEGOT' in df.columns:
        df['DOCDETOT'] = df['DOCDEGRS'] + df['DOCDEGPP'] + df['DOCDEGOT']
        df = df.drop(columns=['DOCDEGRS', 'DOCDEGPP', 'DOCDEGOT'])

    df.to_csv(output_path, index=False)
    print(f"Successfully generated {output_path}")

if __name__ == '__main__':
    build_aggregated_dataset()