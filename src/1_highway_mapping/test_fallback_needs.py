import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from local_geocoder import geocode_junction

pts = ["Antas", "Granja", "Canelas (Gaia)", "IC2", "ER1-18"]
for pt in pts:
    res = geocode_junction(pt, "A28") 
    if res:
        print(f"{pt}: ({res.x}, {res.y})")
    else:
        print(f"{pt}: NOT FOUND")

pts2 = ["Canelas (Gaia)", "IC2", "Hospital", "A20/A29"]
for pt in pts2:
    res = geocode_junction(pt, "A29") 
    if res:
        print(f"A29 {pt}: ({res.x}, {res.y})")
    else:
        print(f"A29 {pt}: NOT FOUND")

