import os
import json

def interpolate_nulls(vals):
    new_vals = list(vals)
    for i in range(len(vals)):
        if vals[i] is None:
            # Find nearest valid previous value
            prev_v = None
            for j in range(i - 1, -1, -1):
                if vals[j] is not None:
                    prev_v = vals[j]
                    break
            
            # Find nearest valid next value
            next_v = None
            for j in range(i + 1, len(vals)):
                if vals[j] is not None:
                    next_v = vals[j]
                    break
            
            # Calculate the interpolated value
            if prev_v is not None and next_v is not None:
                new_vals[i] = int(round((prev_v + next_v) / 2.0))
            elif prev_v is not None:
                new_vals[i] = prev_v
            elif next_v is not None:
                new_vals[i] = next_v
            # If both are None, it remains None
    return new_vals

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    modified = False
    
    # Check all possible section names where sublancos might be stored
    section_keys = ['sublancosNorte', 'sublancosSul', 'sublancosCentro', 'sublancosNorte2', 'sublancos']
    
    for section_key in section_keys:
        if section_key in data:
            segments = data[section_key]
            
            # Gather all years present across the segments
            years = set()
            for seg in segments:
                for k in seg.keys():
                    if k.isdigit() and len(k) == 4:
                        years.add(k)
            
            for year in years:
                months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
                for month in months:
                    # Extract the array of values for this specific year and month
                    vals = [seg.get(year, {}).get(month) for seg in segments]
                    
                    # If there's at least one None, attempt interpolation
                    if None in vals and any(v is not None for v in vals):
                        new_vals = interpolate_nulls(vals)
                        
                        # Apply interpolated values back to the segments
                        for i, new_v in enumerate(new_vals):
                            if vals[i] is None and new_v is not None:
                                if year not in segments[i]:
                                    segments[i][year] = {}
                                segments[i][year][month] = new_v
                                modified = True

    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Updated: {filepath}")
    else:
        print(f"No nulls to fix in: {filepath}")

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/01_raw/highway_traffic'))
    
    # Dirs to process (only raw folder going forward)
    dirs_to_check = [
        base_dir
    ]
    
    for d in dirs_to_check:
        if not os.path.exists(d):
            continue
            
        for filename in os.listdir(d):
            if filename.endswith('_traffic_data.json'):
                filepath = os.path.join(d, filename)
                process_file(filepath)

if __name__ == "__main__":
    main()
