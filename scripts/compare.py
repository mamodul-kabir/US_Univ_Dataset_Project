import pandas as pd

SCORECARD_PATH = 'data_raw/Most-Recent-Cohorts-Institution.csv'
IPEDS_PATH = 'data_raw/hd2022.csv'
OUTPUT_PATH = 'data_clean/merged_university_data.csv'
MISMATCH_REPORT = 'documentation/mismatch_report.csv'

def process_university_data():
    print("Loading datasets...")
    scorecard = pd.read_csv(SCORECARD_PATH, low_memory=False)
    ipeds = pd.read_csv(IPEDS_PATH, encoding='cp1252') 

    print(f"Initial Scorecard rows: {len(scorecard)}")
    scorecard_active = scorecard[scorecard['CURROPER'] == 1].copy()
    print(f"Scorecard rows after filtering CURROPER=1: {len(scorecard_active)}")

    ipeds.columns = [col.upper() for col in ipeds.columns]
    scorecard_active.columns = [col.upper() for col in scorecard_active.columns]

    merged_df = pd.merge(
        ipeds[['UNITID', 'INSTNM', 'CITY', 'STABBR']], 
        scorecard_active[['UNITID', 'INSTNM', 'CONTROL']], 
        on='UNITID', 
        how='outer', 
        suffixes=('_IPEDS', '_SCORECARD'),
        indicator=True
    )

    mismatches = merged_df[merged_df['_merge'] != 'both']
    mismatches.to_csv(MISMATCH_REPORT, index=False)
    
    print(f"Mismatch report saved to {MISMATCH_REPORT}")
    print(f"Count - In IPEDS only: {len(merged_df[merged_df['_merge'] == 'left_only'])}")
    print(f"Count - In Scorecard only: {len(merged_df[merged_df['_merge'] == 'right_only'])}")


    final_dataset = merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])
    final_dataset.to_csv(OUTPUT_PATH, index=False)
    
    print(f"Cleaned dataset saved to {OUTPUT_PATH}")
    print(f"Final valid comparison count: {len(final_dataset)}")

if __name__ == "__main__":
    process_university_data()