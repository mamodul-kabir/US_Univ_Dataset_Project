import pandas as pd
import numpy as np
import os

def generate_drvadm(year):
    raw_file = f"../data_raw/{year}/adm{year}_rv.csv"
    output_dir = f"../data_freq/{year}"
    output_file = f"{output_dir}/drvadm{year}_rv.csv"

    if not os.path.exists(raw_file):
        print(f"Error: Raw file {raw_file} not found.")
        return

    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(raw_file)
    df.columns = df.columns.str.upper()

    drv = pd.DataFrame({'UNITID': df['UNITID']})

    def calc_derived(num_col, den_col):
        if num_col not in df.columns or den_col not in df.columns:
            return pd.Series(dtype='Int64')

        num = pd.to_numeric(df[num_col], errors='coerce').fillna(0)
        den = pd.to_numeric(df[den_col], errors='coerce').fillna(0)

        # Calculate ratio strictly where the denominator is > 0
        ratio = np.where(den > 0, (num / den) * 100, np.nan)
        
        # Apply standard half-up arithmetic rounding and cast to nullable integer
        return pd.Series(np.floor(ratio + 0.5), index=df.index).astype('Int64')

    drv['DVADM01'] = calc_derived('ADMSSN', 'APPLCN')
    drv['DVADM02'] = calc_derived('ADMSSNM', 'APPLCNM')
    drv['DVADM03'] = calc_derived('ADMSSNW', 'APPLCNW')
    
    drv['DVADM04'] = calc_derived('ENRLT', 'ADMSSN')
    drv['DVADM05'] = calc_derived('ENRLM', 'ADMSSNM')
    drv['DVADM06'] = calc_derived('ENRLW', 'ADMSSNW')
    
    drv['DVADM07'] = calc_derived('ENRLFT', 'ADMSSN')
    drv['DVADM08'] = calc_derived('ENRLFTM', 'ADMSSNM')
    drv['DVADM09'] = calc_derived('ENRLFTW', 'ADMSSNW')
    
    drv['DVADM10'] = calc_derived('ENRLPT', 'ADMSSN')
    drv['DVADM11'] = calc_derived('ENRLPTM', 'ADMSSNM')
    drv['DVADM12'] = calc_derived('ENRLPTW', 'ADMSSNW')

    drv.to_csv(output_file, index=False)
    print(f"Successfully generated {output_file}")

generate_drvadm(2019)
generate_drvadm(2020)