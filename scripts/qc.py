import pandas as pd
import sys
import os

def get_university_value(filename, varname, uid):
    if not os.path.exists(filename):
        return f"File {filename} not found."

    try:
        columns = pd.read_csv(filename, nrows=0, encoding='cp1252', encoding_errors='replace').columns
        
        target_col = next(col for col in columns if col.strip().upper() == varname.strip().upper())
        unitid_col = next(col for col in columns if 'UNITID' in col.upper())
        
        df = pd.read_csv(filename, usecols=[unitid_col, target_col], encoding='cp1252', encoding_errors='replace')
        df = df.rename(columns={unitid_col: 'UNITID', target_col: varname.upper()})
        
        result = df[df['UNITID'] == int(uid)]
        
        if not result.empty:
            return result.iloc[0][varname.upper()]
        else:
            return f"UID {uid} not found."
            
    except StopIteration:
        return f"Error: Variable {varname} is missing from {filename}."
    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python qc.py <filename> <varname>")
        sys.exit(1)
        
    file_name = "data_raw/" + sys.argv[1]
    variable_name = sys.argv[2]
    university_id = 193900
    
    output = get_university_value(file_name, variable_name, university_id)
    print(output)