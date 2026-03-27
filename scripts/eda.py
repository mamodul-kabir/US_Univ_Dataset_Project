import pandas as pd
import os

def generate_eda_report():
    input_path = 'data_final/agg2024.csv'
    output_dir = 'documentation/eda_2024'
    
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_csv(input_path)
    
    stats_df = df.describe(percentiles=[0.5]).T
    stats_df['missing_count'] = df.isnull().sum()
    stats_df['missing_percentage'] = (df.isnull().sum() / len(df)) * 100
    stats_df.to_csv(os.path.join(output_dir, 'descriptive_statistics.csv'))
    
    missing_threshold = 0.40
    row_missing_pct = df.isnull().mean(axis=1)
    empty_institutions = df[row_missing_pct > missing_threshold].copy()
    empty_institutions['percent_missing'] = row_missing_pct[row_missing_pct > missing_threshold] * 100
    empty_institutions = empty_institutions.sort_values(by="percent_missing")
    empty_institutions.to_csv(os.path.join(output_dir, 'highly_missing_institutions.csv'), index=False)
    
    context_missing = []
    if 'EFUG' in df.columns:
        ug_schools = df[(df['EFUG'] > 0)]
        
        # STUFACR is included in the list below
        ug_vars = ['UAGRNTA','UDGAGRNTN', 'STUFACR']
        
        for col in ug_vars:
            if col in df.columns:
                miss_pct = ug_schools[col].isnull().mean() * 100
                avail_count = ug_schools[col].notnull().sum()  # Raw count of present values
                context_missing.append({
                    'Variable': col, 
                    'Condition': 'Has Undergraduates(EFUG > 0)', 
                    'Missing_Pct': miss_pct,
                    'Available_count': avail_count
                })
    # UPDATED CONDITION: Has Undergrads (EFUG > 0) AND Not Open Admission (OPENADMP == 2)
    if 'EFUG' in df.columns and 'OPENADMP' in df.columns:
        ug_schools = df[(df['EFUG'] > 0) & (df['OPENADMP'] == 2)]
        
        # STUFACR is included in the list below
        ug_vars = ['DVADM01', 'DVADM04', 'SATPCT', 'SATCM50', 'ACTPCT', 'ACTCM50']
        
        for col in ug_vars:
            if col in df.columns:
                miss_pct = ug_schools[col].isnull().mean() * 100
                avail_count = ug_schools[col].notnull().sum()  # Raw count of present values
                context_missing.append({
                    'Variable': col, 
                    'Condition': 'Has Undergraduates & Not Open Admission (EFUG > 0 & OPENADMP == 2)', 
                    'Missing_Pct': miss_pct,
                    'Available_count': avail_count
                })
    if 'EFUG' in df.columns and 'FT_FTUG' in df.columns:
        # Condition: Has Undergraduates(EFUG > 0)
        ug_schools = df[(df['EFUG'] > 0) & (df['FT_FTUG'] == 1)]
        
        # STUFACR is included in the list below
        ug_vars = ['TUITION1', 'GRRTTOT', 'OM1TOTLAWDP4', 'OM1TOTLAWDP8', 'OM1TOTLENAP8']
        
        for col in ug_vars:
            if col in df.columns:
                miss_pct = ug_schools[col].isnull().mean() * 100
                avail_count = ug_schools[col].notnull().sum()  # Raw count of present values
                context_missing.append({
                    'Variable': col, 
                    'Condition': 'Has Undergraduates(EFUG > 0) and enrolls full time first time u.grads', 
                    'Missing_Pct': miss_pct,
                    'Available_count': avail_count
                })

    if 'HLOFFER' in df.columns:
        if 'MASDEG' in df.columns:
            master_schools = df[df['HLOFFER'] >= 7]
            context_missing.append({
                'Variable': 'MASDEG', 
                'Condition': "Offers Master's or higher (HLOFFER >= 7)", 
                'Missing_Pct': master_schools['MASDEG'].isnull().mean() * 100,
                'Available_count': master_schools['MASDEG'].notnull().sum()
            })
            
        if 'DOCDETOT' in df.columns:
            doc_schools = df[df['HLOFFER'] == 9]
            context_missing.append({
                'Variable': 'DOCDETOT', 
                'Condition': "Offers Doctor's (HLOFFER == 9)", 
                'Missing_Pct': doc_schools['DOCDETOT'].isnull().mean() * 100,
                'Available_count': doc_schools['DOCDETOT'].notnull().sum()
            })

    if 'SFTEINST' in df.columns and 'SALTOTL' in df.columns:
        staff_schools = df[df['SFTEINST'] > 0]
        miss_pct = staff_schools['SALTOTL'].isnull().mean() * 100
        avail_count = staff_schools['SALTOTL'].notnull().sum()
        context_missing.append({
            'Variable': 'SALTOTL', 
            'Condition': 'Has Instructional Staff (SFTEINST > 0)', 
            'Missing_Pct': miss_pct,
            'Available_count': avail_count
        })
        
    context_df = pd.DataFrame(context_missing)
    
    if not context_df.empty:
        # Reordering columns just to make it look clean, optional but recommended
        cols = ['Variable', 'Condition', 'Available_count', 'Missing_Pct']
        context_df = context_df[cols]
        context_df.to_csv(os.path.join(output_dir, 'contextual_missingness.csv'), index=False)
        
    print(f"EDA reports generated in {output_dir}")

if __name__ == '__main__':
    generate_eda_report()