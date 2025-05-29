import requests
import json
import numpy as np
import pandas as pd

def fetch_and_display_weather(state, district, lat, lon):
    """
    Fetch weather data and display in CSV format
    """
    print(f"Fetching weather data for {district}, {state}")
    print(f"Coordinates: lat={lat}, lon={lon}")
    print("-" * 80)
    
    # Construct URL
    url = f"https://historical-forecast-api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&start_date=2024-01-01&end_date=2024-12-31&daily=uv_index_max,temperature_2m_mean,cloud_cover_mean,relative_humidity_2m_mean,sunshine_duration,precipitation_sum,et0_fao_evapotranspiration,temperature_2m_min,temperature_2m_max,shortwave_radiation_sum&models=best_match"
    
    try:
        # Make request
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"Error: API returned status code {response.status_code}")
            return
        
        # Parse JSON
        data = response.json()
        
        if 'daily' in data:
            daily = data['daily']
            
            # Calculate aggregated metrics
            metrics = {
                'uv_index_max': np.mean([x for x in daily['uv_index_max'] if x is not None]),
                'temperature_2m_mean': np.mean([x for x in daily['temperature_2m_mean'] if x is not None]),
                'cloud_cover_mean': np.mean([x for x in daily['cloud_cover_mean'] if x is not None]),
                'relative_humidity_2m_mean': np.mean([x for x in daily['relative_humidity_2m_mean'] if x is not None]),
                'sunshine_duration': np.sum([x for x in daily['sunshine_duration'] if x is not None]),
                'precipitation_sum': np.sum([x for x in daily['precipitation_sum'] if x is not None]),
                'et0_fao_evapotranspiration': np.sum([x for x in daily['et0_fao_evapotranspiration'] if x is not None]),
                'temperature_2m_min': np.mean([x for x in daily['temperature_2m_min'] if x is not None]),
                'temperature_2m_max': np.mean([x for x in daily['temperature_2m_max'] if x is not None]),
                'shortwave_radiation_sum': np.sum([x for x in daily['shortwave_radiation_sum'] if x is not None])
            }
            
            # Round values to 2 decimal places
            for key in metrics:
                if pd.notna(metrics[key]):
                    metrics[key] = round(metrics[key], 2)
            
            # Print header
            print("\nCSV Header:")
            print("state,district,lat,lon,uv_index_max,temperature_2m_mean,cloud_cover_mean,relative_humidity_2m_mean,sunshine_duration,precipitation_sum,et0_fao_evapotranspiration,temperature_2m_min,temperature_2m_max,shortwave_radiation_sum")
            
            # Print data in CSV format
            print("\nCSV Data:")
            csv_line = f"{state},{district},{lat},{lon},{metrics['uv_index_max']},{metrics['temperature_2m_mean']},{metrics['cloud_cover_mean']},{metrics['relative_humidity_2m_mean']},{metrics['sunshine_duration']},{metrics['precipitation_sum']},{metrics['et0_fao_evapotranspiration']},{metrics['temperature_2m_min']},{metrics['temperature_2m_max']},{metrics['shortwave_radiation_sum']}"
            print(csv_line)
            
            # Also print detailed breakdown
            print("\n" + "="*80)
            print("DETAILED BREAKDOWN:")
            print("="*80)
            print(f"State: {state}")
            print(f"District: {district}")
            print(f"Latitude: {lat}")
            print(f"Longitude: {lon}")
            print(f"UV Index Max (avg): {metrics['uv_index_max']}")
            print(f"Temperature Mean (avg): {metrics['temperature_2m_mean']}°C")
            print(f"Cloud Cover Mean (avg): {metrics['cloud_cover_mean']}%")
            print(f"Relative Humidity Mean (avg): {metrics['relative_humidity_2m_mean']}%")
            print(f"Sunshine Duration (total): {metrics['sunshine_duration']} seconds ({metrics['sunshine_duration']/3600:.2f} hours)")
            print(f"Precipitation Sum (total): {metrics['precipitation_sum']} mm")
            print(f"ET0 FAO Evapotranspiration (total): {metrics['et0_fao_evapotranspiration']} mm")
            print(f"Temperature Min (avg): {metrics['temperature_2m_min']}°C")
            print(f"Temperature Max (avg): {metrics['temperature_2m_max']}°C")
            print(f"Shortwave Radiation Sum (total): {metrics['shortwave_radiation_sum']} MJ/m²")
            
        else:
            print("\nError: No 'daily' data found in response")
            
    except requests.exceptions.RequestException as e:
        print(f"\nRequest error: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {type(e).__name__}: {e}")

# Test with Batu Pahat coordinates
if __name__ == "__main__":
    # Batu Pahat, Johor
    state = "Johor"
    district = "Batu Pahat"
    lat = 1.8472584
    lon = 102.9346697
    
    fetch_and_display_weather(state, district, lat, lon)