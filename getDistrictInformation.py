import pandas as pd
import requests
import json
import time
from datetime import datetime
import numpy as np

def fetch_weather_data(lat, lon):
    """
    Fetch weather data from Open-Meteo API for given coordinates
    """
    url = f"https://historical-forecast-api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&start_date=2024-01-01&end_date=2024-12-31&daily=uv_index_max,temperature_2m_mean,cloud_cover_mean,relative_humidity_2m_mean,sunshine_duration,precipitation_sum,et0_fao_evapotranspiration,temperature_2m_min,temperature_2m_max,shortwave_radiation_sum&models=best_match"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for lat={lat}, lon={lon}: {e}")
        return None

def process_weather_metrics(data):
    """
    Process the weather data and calculate aggregated metrics
    Since the API returns daily values for the entire year, we need to aggregate them
    """
    if not data or 'daily' not in data:
        return None
    
    daily = data['daily']
    
    # Calculate aggregated metrics for the year
    metrics = {
        'uv_index_max': np.mean([x for x in daily['uv_index_max'] if x is not None]),
        'temperature_2m_mean': np.mean([x for x in daily['temperature_2m_mean'] if x is not None]),
        'cloud_cover_mean': np.mean([x for x in daily['cloud_cover_mean'] if x is not None]),
        'relative_humidity_2m_mean': np.mean([x for x in daily['relative_humidity_2m_mean'] if x is not None]),
        'sunshine_duration': np.sum([x for x in daily['sunshine_duration'] if x is not None]),  # Total for the year
        'precipitation_sum': np.sum([x for x in daily['precipitation_sum'] if x is not None]),  # Total for the year
        'et0_fao_evapotranspiration': np.sum([x for x in daily['et0_fao_evapotranspiration'] if x is not None]),  # Total
        'temperature_2m_min': np.mean([x for x in daily['temperature_2m_min'] if x is not None]),
        'temperature_2m_max': np.mean([x for x in daily['temperature_2m_max'] if x is not None]),
        'shortwave_radiation_sum': np.sum([x for x in daily['shortwave_radiation_sum'] if x is not None])  # Total
    }
    
    # Round values to reasonable precision
    for key in metrics:
        if pd.notna(metrics[key]):
            metrics[key] = round(metrics[key], 2)
    
    return metrics

def main():
    # Read the CSV file
    input_file = 'dataset/district_with_information.csv'
    output_file = 'dataset/district_with_weather_data.csv'
    
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    total_rows = len(df)
    print(f"Found {total_rows} locations to process")
    
    # Process each row
    for idx, row in df.iterrows():
        state = row['state']
        district = row['district']
        lat = row['lat']
        lon = row['lon']
        
        print(f"\nProcessing {idx+1}/{total_rows}: {district}, {state} (lat={lat}, lon={lon})")
        
        # Fetch weather data
        weather_data = fetch_weather_data(lat, lon)
        
        if weather_data:
            # Process the metrics
            metrics = process_weather_metrics(weather_data)
            
            if metrics:
                # Update the dataframe with the metrics
                for metric, value in metrics.items():
                    df.at[idx, metric] = value
                
                print(f"✓ Successfully updated weather data for {district}")
            else:
                print(f"✗ Failed to process weather data for {district}")
        else:
            print(f"✗ Failed to fetch weather data for {district}")
        
        # Save progress periodically (every 10 rows)
        if (idx + 1) % 10 == 0:
            print(f"\nSaving progress to {output_file}...")
            df.to_csv(output_file, index=False)
            print("Progress saved!")
        
        # Add a small delay to avoid hitting API rate limits
        time.sleep(0.5)  # 500ms delay between requests
    
    # Save final results
    print(f"\nSaving final results to {output_file}...")
    df.to_csv(output_file, index=False)
    print("Complete! All weather data has been fetched and saved.")
    
    # Display summary statistics
    print("\nSummary of fetched data:")
    print(f"Total locations processed: {total_rows}")
    print(f"Successful updates: {df['uv_index_max'].notna().sum()}")
    print(f"Failed updates: {df['uv_index_max'].isna().sum()}")
    
    # Show sample of results
    print("\nSample of results (first 5 rows):")
    print(df[['state', 'district', 'temperature_2m_mean', 'precipitation_sum']].head())

if __name__ == "__main__":
    main()