import json
import os
import sys

# 1. Load the existing fallbacks
sys.path.insert(0, os.path.abspath('code/Enable/src/1_highway_mapping'))
try:
    from extra_fallbacks import EXTRA_FALLBACKS
except ImportError as e:
    print(f"Error importing: {e}")
    sys.exit(1)

# 2. Parse all JSON files to build Node -> set(Highways) mapping
input_dir = 'code/Enable/data/01_raw/highway_traffic/processed'
node_to_hw = {}

for f in os.listdir(input_dir):
    if f.endswith('_traffic_data.json'):
        with open(os.path.join(input_dir, f), 'r', encoding='utf-8') as fp:
            data = json.load(fp)
            hw = data.get('autoestrada')
            if not hw:
                continue
            
            sublancos = data.get("sublancosNorte", []) + data.get("sublancosNorte2", []) + \
                        data.get("sublancosSul", []) + data.get("sublancosCentro", []) + \
                        data.get("sublancos", [])
            
            for item in sublancos:
                sub = item.get('sublanco', '')
                if ' – ' in sub:
                    parts = sub.split(' – ')
                    n1, n2 = parts[0].strip(), parts[1].strip()
                    node_to_hw.setdefault(n1, set()).add(hw)
                    node_to_hw.setdefault(n2, set()).add(hw)
                else:
                    node_to_hw.setdefault(sub + " Norte", set()).add(hw)
                    node_to_hw.setdefault(sub + " Sul", set()).add(hw)

# 3. Analyze and map current fallbacks
new_fallbacks = {}

for key, coords in EXTRA_FALLBACKS.items():
    if isinstance(key, tuple):
        new_fallbacks[key] = coords
    elif isinstance(key, str):
        hw_set = node_to_hw.get(key, set())
        
        if len(hw_set) >= 1:
            for hw in hw_set:
                new_fallbacks[(hw, key)] = coords
        else:
            new_fallbacks[("GLOBAL", key)] = coords

print(f"Total initial fallbacks: {len(EXTRA_FALLBACKS)}")
print(f"Unified fallbacks count: {len(new_fallbacks)}")

# 4. Rewrite extra_fallbacks.py
out_path = 'code/Enable/src/1_highway_mapping/extra_fallbacks.py'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write("EXTRA_FALLBACKS = {\n")
    # Sort them nicely: by highway, then by node name
    for k, v in sorted(new_fallbacks.items(), key=lambda x: (x[0][0], x[0][1])):
        # Escape quotes inside node names if necessary (very rare, but safe)
        node_name = k[1].replace('"', '\\"')
        f.write(f'    ("{k[0]}", "{node_name}"): ({v[0]}, {v[1]}),\n')
    f.write("}\n")
    
print("Successfully unified and saved extra_fallbacks.py")

