import math
from datetime import datetime

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r

def calculate_speed_kmh(tx1, tx2):
    """
    Calculates the speed in km/h between two transactions.
    Assumes transactions have 'lat', 'lon', and 'timestamp' (ISO 8601 string) fields.
    If 'lat' or 'lon' are missing, or if time difference is 0, returns 0.
    """
    if 'lat' not in tx1 or 'lon' not in tx1 or 'lat' not in tx2 or 'lon' not in tx2:
        return 0

    try:
        # Assuming timestamp is in ISO 8601 format like "2023-10-27T10:00:00Z" or just a unix timestamp. 
        # For this example, assuming ISO 8601 string.
        # Adjust parsing based on actual data format.
        time1 = datetime.fromisoformat(tx1['timestamp'].replace('Z', '+00:00'))
        time2 = datetime.fromisoformat(tx2['timestamp'].replace('Z', '+00:00'))
        
        time_diff_hours = abs((time2 - time1).total_seconds()) / 3600.0
        
        if time_diff_hours == 0:
            return 0
            
        distance_km = haversine_distance(tx1['lat'], tx1['lon'], tx2['lat'], tx2['lon'])
        
        return distance_km / time_diff_hours
    except (ValueError, KeyError, TypeError):
        # Fallback if timestamp parsing fails or fields are missing/wrong type
        return 0
