"""
Economic Security Index (ESI) - MVP Data Pipeline
Author: Rafael Munoz
Purpose: Ingest and clean World Bank macroeconomic data across 4 EWC-aligned pillars.
"""

import wbgapi as wb
import pandas as pd

def main():
    print("[*] Initiating ESI Data Pipeline MVP (wbgapi + Custom Headers)...")

    # 1. NETWORK CONFIGURATION
    # Injecting our transparent User-Agent into wbgapi's underlying requests module
    wb.get_options['headers'] = {
        "User-Agent": "RafaelMunoz-Research/1.0 (rcmunoz@hawaii.edu)"
    }
    # Enforce a 10-second timeout so the CPU never hangs indefinitely
    wb.get_options['timeout'] = 10 

    # 2. TARGET ECONOMIES & INDICATORS
    target_countries = ['PHL', 'IDN', 'VNM', 'FJI', 'AUS']
    
    indicators = {
        'EG.IMP.CONS.ZS': 'Pillar_1_Energy_Imports',
        'NV.IND.TOTL.ZS': 'Pillar_2_Industry_Value_Added',
        'BX.KLT.DINV.WD.GD.ZS': 'Pillar_3_FDI_Inflows',
        'TX.VAL.TECH.MF.ZS': 'Pillar_4_High_Tech_Exports'
    }

    # 3. ANTI-THROTTLE DATA INGESTION
    print("[*] Pulling data from World Bank API...")
    
    raw_data_frames = []
    
    for code, pillar_name in indicators.items():
        print(f"    -> Requesting {pillar_name} ({code})...")
        try:
            # Query one indicator at a time using wbgapi
            df_chunk = wb.data.DataFrame(
                series=code, 
                economy=target_countries, 
                mrv=5, 
                numericTimeKeys=True
            )
            
            # wbgapi sometimes drops the 'series' column if querying a single code. 
            # We explicitly add it back so our melt() function works later.
            df_chunk['series'] = code
            raw_data_frames.append(df_chunk)
            print("       [OK] Success.")
            
        except Exception as e:
            print(f"       [FAILED] Could not retrieve {code}. Error: {e}")

    # Combine all successful chunks
    if not raw_data_frames:
        print("[-] Critical Error: All API calls failed. Exiting.")
        return
        
    raw_data = pd.concat(raw_data_frames)

    # 4. DATA TRANSFORMATION & STRUCTURAL IMPUTATION
    print("\n[*] Cleaning, reshaping, and imputing data...")
    
    df = raw_data.reset_index()
    
    df_long = df.melt(
        id_vars=['economy', 'series'],
        var_name='year',
        value_name='value'
    )

    # Map the human-readable Pillar names
    df_long['indicator_name'] = df_long['series'].map(indicators)

    # Sort hierarchically for time-series integrity
    df_long = df_long.sort_values(by=['economy', 'indicator_name', 'year'])

    # Imputation: Linear interpolation for internal gaps
    df_long['value'] = df_long.groupby(['economy', 'indicator_name'])['value'].transform(
        lambda group: group.interpolate(method='linear')
    )

    # Imputation: Forward-fill structural data (capped at 2 years)
    df_long['value'] = df_long.groupby(['economy', 'indicator_name'])['value'].transform(
        lambda group: group.ffill(limit=2)
    )

    df_clean = df_long.dropna(subset=['value']).copy()

    # 5. OUTPUT GENERATION
    print("\n--- PIPELINE SUCCESS ---")
    print(df_clean.head(15))
    
    df_clean.to_csv("esi_mvp_cleaned.csv", index=False)
    print("\n[+] Data successfully saved to 'esi_mvp_cleaned.csv'")

if __name__ == "__main__":
    main()