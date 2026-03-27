import pandas as pd
import os

def export():
    input_path = 'data_final/agg2024.csv'
    output_path = 'data_final/brief_2024.csv'
    
    df = pd.read_csv(input_path)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    columns_to_keep = ['INSTNM', 'STABBR', 'CNTLAFFI', 'IS_R1', 'COREXP', 'ENRTOT']
    
    df_filtered = df[columns_to_keep]
    
    df_filtered.to_csv(output_path, index=False)
    
    print(f"Exported columns to {output_path}")

if __name__ == '__main__':
    export()