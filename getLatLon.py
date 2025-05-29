import pandas as pd
import requests
import time
import json

def get_district_coordinates(api_key, input_csv='dataset/district.csv', output_csv='district_with_coordinates.csv'):
    """
    Reads district.csv and fetches lat/lon coordinates for each district using OpenWeatherMap API.
    
    Parameters:
    - api_key: Your OpenWeatherMap API key
    - input_csv: Path to input CSV file (default: 'district.csv')
    - output_csv: Path to output CSV file (default: 'district_with_coordinates.csv')
    """
    
    # Read the district CSV file
    df = pd.read_csv(input_csv)
    
    # Initialize lists to store coordinates
    latitudes = []
    longitudes = []
    
    # Base URL for the API
    base_url = "http://api.openweathermap.org/geo/1.0/direct"
    
    # Process each row
    for idx, row in df.iterrows():
        state = row['state']
        district = row['district']
        
        # Construct the query string
        # Format: "district, state, Malaysia" for better accuracy
        query = f"{district}, {state}, Malaysia"
        
        # API parameters
        params = {
            'q': query,
            'limit': 1,
            'appid': api_key
        }
        
        try:
            # Make API request
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise an error for bad status codes
            
            # Parse JSON response
            data = response.json()
            
            if data and len(data) > 0:
                # Extract lat and lon from first result
                lat = data[0].get('lat', None)
                lon = data[0].get('lon', None)
                latitudes.append(lat)
                longitudes.append(lon)
                print(f"✓ Found coordinates for {district}, {state}: ({lat}, {lon})")
            else:
                # No results found
                latitudes.append(None)
                longitudes.append(None)
                print(f"✗ No coordinates found for {district}, {state}")
                
        except requests.exceptions.RequestException as e:
            print(f"✗ API error for {district}, {state}: {e}")
            latitudes.append(None)
            longitudes.append(None)
        
        # Add a small delay to respect API rate limits
        # OpenWeatherMap free tier allows 60 calls/minute
        time.sleep(1)  # 1 second delay between requests
    
    # Add coordinates to dataframe
    df['lat'] = latitudes
    df['lon'] = longitudes
    
    # Save to new CSV file
    df.to_csv(output_csv, index=False)
    print(f"\n✓ Saved results to {output_csv}")
    
    # Print summary
    successful = df[['lat', 'lon']].notna().all(axis=1).sum()
    total = len(df)
    print(f"\nSummary: Successfully geocoded {successful}/{total} districts")
    
    return df

# Example usage
if __name__ == "__main__":
    # Replace with your actual API key
    API_KEY = "xxx"
    df_result = get_district_coordinates(API_KEY)