import requests
import json
import boto3
import csv
from datetime import datetime
import io

# S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    api_url = 'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relative_humidity_2m,windspeed_10m&timezone=Europe/Berlin'
    
    # Fetch data from API
    try:
        response = requests.get(api_url)
        response.raise_for_status() 
        weather_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error fetching data: {e}")
        }
    
    # Use the time the script is run for the S3 file name (fetch_timestamp)
    fetch_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f"weather_data_split_{fetch_timestamp}.csv"
    
    # Prepare CSV output in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # NEW CSV HEADER: Two columns for date and time
    writer.writerow(['forecast_date', 'forecast_time', 'temperature_2m', 'humidity_2m', 'windspeed_10m'])

    # Check for required data keys
    if 'hourly' not in weather_data or 'time' not in weather_data['hourly']:
        print("API response structure is missing required 'hourly' data.")
        return {
            'statusCode': 500,
            'body': json.dumps("API response structure is incomplete or missing 'time' data.")
        }

    # Extract the separate arrays for easier access
    try:
        hourly_datetime_strings = weather_data['hourly']['time']
        temps = weather_data['hourly']['temperature_2m']
        hums = weather_data['hourly']['relative_humidity_2m']
        winds = weather_data['hourly']['windspeed_10m']
        
        # Iterate, split the datetime string, and write the row
        for i in range(len(hourly_datetime_strings)):
            # Example: "2025-10-13T00:00" -> ("2025-10-13", "00:00")
            date_part, time_part = hourly_datetime_strings[i].split('T')
            
            writer.writerow([
                date_part,                  # New column 1: Date part
                time_part,                  # New column 2: Time part
                temps[i],
                hums[i],
                winds[i]
            ])
            
    except KeyError as e:
        print(f"Missing expected key in API data: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Missing expected key in API data: {e}")
        }
    
    # Seek to the beginning of the StringIO buffer to read it
    output.seek(0)
    
    # Store the CSV in the 'load' folder
    try:
        s3.put_object(
            Bucket='my-etldata-bucket', # Replace with your bucket name
            Key=f"load/{file_name}",
            Body=output.getvalue()
        )
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error uploading to S3: {e}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f"Weather data saved as CSV to load/{file_name}")
    }
