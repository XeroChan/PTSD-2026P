import math
from datetime import datetime

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates the straight-line distance between two GPS points in kilometers."""
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371.0
    return c * r

def calculate_speed_kmh(time1_iso: str, lat1: float, lon1: float, time2_iso: str, lat2: float, lon2: float) -> float:
    """Returns the speed in km/h based on two points in time and space."""
    try:
        t1 = datetime.fromisoformat(time1_iso.replace('Z', '+00:00'))
        t2 = datetime.fromisoformat(time2_iso.replace('Z', '+00:00'))
        hours = abs((t2 - t1).total_seconds()) / 3600.0
        
        if hours == 0:
            return 0.0
            
        dist = haversine_distance(lat1, lon1, lat2, lon2)
        return dist / hours
    except Exception:
        return 0.0