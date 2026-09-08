"""
Economic Security Index (ESI) - MVP Data Pipeline
Author: Rafael Munoz
Purpose: Direct REST API Ingestion & Panel Imputation across 4 EWC Pillars.
"""

import requests      # Used to send HTTP requests to the World Bank servers over the internet
import pandas as pd  # Used to organize our data into rows and columns (DataFrames) and do math
import time          # Used to pause our script so we don't overwhelm the World Bank servers

def main():
    print("Initiating ESI Data Pipeline MVP...")

    # ==========================================
    # STEP 1: CONFIGURATION & SETUP
    # ==========================================
    
    # We define our 5 target countries using their official 3-letter ISO codes
    target_countries = ['PHL', 'IDN', 'VNM', 'FJI', 'AUS']
    
    # We map the World Bank's database codes to our custom EWC Pillar names
    # The dictionary format is 'API_CODE': 'Our_Custom_Name'
    indicators = {
        'EG.IMP.CONS.ZS': 'Pillar_1_Energy_Imports',
        'NV.IND.TOTL.ZS': 'Pillar_2_Industry_Value_Added',
        'BX.KLT.DINV.WD.GD.ZS': 'Pillar_3_FDI_Inflows',
        'TX.VAL.TECH.MF.ZS': 'Pillar_4_High_Tech_Exports'
    }

    # We identify ourselves transparently to the World Bank firewall. 
    # This prevents our script from being blocked as a malicious bot.
    headers = {
        "User-Agent": "RafaelMunoz-Research/1.0 (rcmunoz@hawaii.edu)"
    }

    # This empty list will hold all the data we download before we turn it into a table
    raw_records = []

    print("Pulling data from World Bank API...")
    
    # ==========================================
    # STEP 2: DATA INGESTION (API LOOP)
    # ==========================================
    
    # We loop through ONE country and ONE indicator at a time.
    # This keeps our request size small and prevents the server from timing out.
    for country in target_countries:
        for code, pillar_name in indicators.items():
            
            # Construct the specific URL for this exact country and indicator.
            # mrv=6 means "Most Recent Values = 6", which gives us 6 years of data.
            url = f"https://api.worldbank.org/v2/country/{country}/indicator/{code}?format=json&mrv=6"
            print(f" -> Requesting {country}: {pillar_name}...")
            
            try:
                # Send the request to the URL. Wait a maximum of 15 seconds for a reply.
                response = requests.get(url, headers=headers, timeout=15)
                
                # HTTP Status Code 200 means "OK / Success"
                if response.status_code == 200:
                    
                    # Convert the text response into a Python JSON object (a dictionary/list)
                    payload = response.json()
                    
                    # The World Bank JSON always returns a list with 2 items.
                    # Item 0 is page metadata. Item 1 is the actual list of data records.
                    if len(payload) == 2 and isinstance(payload[1], list):
                        
                        # Loop through each year's record in the data
                        for entry in payload[1]:
                            
                            # Only save the record if the 'value' is not empty (None)
                            if entry.get('value') is not None:
                                
                                # Append the clean data piece to our master list
                                raw_records.append({
                                    'economy': entry['countryiso3code'],
                                    'series': code,
                                    'year': int(entry['date']),
                                    'value': float(entry['value']),
                                    'indicator_name': pillar_name
                                })
                
            except Exception as e:
                # If the internet drops or the server crashes, print the error but keep the loop running
                print(f"    [FAILED] {country} - {code}: {e}")
                
            # Pause for half a second (0.5s) before asking for the next file. 
            # This is good API etiquette.
            time.sleep(0.5)

    # Safety check: If the list is entirely empty, stop the program.
    if not raw_records:
        print("Error: No data extracted.")
        return

    # ==========================================
    # STEP 3: DATA CLEANING & IMPUTATION
    # ==========================================
    print("\nCleaning and imputing data...")
    
    # Convert our raw list of dictionaries into a Pandas DataFrame (a 2D table)
    df_long = pd.DataFrame(raw_records)
    
    # Sort the table alphabetically by country, then indicator, then chronologically by year.
    # This is required before we do any time-series math.
    df_long = df_long.sort_values(by=['economy', 'indicator_name', 'year'])

    # IMPUTATION 1: Linear Interpolation
    # If a country has data for 2020 and 2022, but 2021 is missing, this draws a straight line
    # to estimate the 2021 value. We group by economy and indicator so we don't mix countries.
    df_long['value'] = df_long.groupby(['economy', 'indicator_name'])['value'].transform(
        lambda group: group.interpolate(method='linear')
    )

    # IMPUTATION 2: Forward Fill
    # The World Bank is often 1-2 years late in publishing data.
    # This takes the most recent valid number and carries it forward (up to 2 years maximum).
    df_long['value'] = df_long.groupby(['economy', 'indicator_name'])['value'].transform(
        lambda group: group.ffill(limit=2)
    )

    # Finally, if any row is still entirely missing data after our fixes, we drop it.
    # .copy() ensures we allocate fresh memory for the final clean table.
    df_clean = df_long.dropna(subset=['value']).copy()

    # ==========================================
    # STEP 4: OUTPUT
    # ==========================================
    print("\n--- PIPELINE SUCCESS ---")
    print(df_clean.head(15)) # Print the top 15 rows to the screen for visual inspection
    
    # Save the table to a CSV file. index=False prevents Pandas from exporting row numbers.
    df_clean.to_csv("esi_mvp_cleaned.csv", index=False)
    print("\nData saved to 'esi_mvp_cleaned.csv'")

# This tells Python to execute the main() function when we run the script directly.
if __name__ == "__main__":
    main()