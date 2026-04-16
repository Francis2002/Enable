import pandas as pd
import glob
import json

files = sorted(glob.glob('code/Enable/data/production/mobie_labels/*.csv'))
if len(files) == 0:
    print("No files found.")
    exit()

def get_stats(f):
    df = pd.read_csv(f, sep=';')
    return {
        "file": f.split('/')[-1],
        "total_sockets": len(df),
        "available": len(df[df['ESTADO DA TOMADA'] == 'Disponível']),
        "in_use": len(df[df['ESTADO DA TOMADA'] == 'Em uso']),
        "offline": len(df[df['ESTADO DA TOMADA'] == 'Offline']),
        "stations": len(df['ID'].unique())
    }

print("Reading first 5 files...")
for f in files[:5]:
    print(get_stats(f))

