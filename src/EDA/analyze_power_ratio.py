import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

# Absolute paths
csv_path = "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/data/01_raw/detailed-20260116.csv"
output_dir = "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/data/plots/output_power_ration"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

print("Loading data...")
# Read the dataset
df = pd.read_csv(csv_path, sep=';', decimal=',')

# Data Cleaning and Formatting
df['evse_max_power'] = pd.to_numeric(df['evse_max_power'], errors='coerce')
df['totalDuration'] = pd.to_numeric(df['totalDuration'], errors='coerce')
df['energia_total_transacao'] = pd.to_numeric(df['energia_total_transacao'], errors='coerce')

# Drop rows with missing critical values
df = df.dropna(subset=['evse_max_power', 'totalDuration', 'energia_total_transacao'])

print("Calculating power ratios...")
# Calculate Theoretical Max Energy in kWh (Power * Hours)
df['max_energy_kwh'] = df['evse_max_power'] * (df['totalDuration'] / 60)

# Filter out sessions with 0 max energy to avoid division by zero
df = df[df['max_energy_kwh'] > 0]

# Calculate Power Ratio
df['ratio'] = df['energia_total_transacao'] / df['max_energy_kwh']

print("Generating plots for individual sockets...")
# Set seaborn style for better aesthetics
sns.set_theme(style="whitegrid")

# Plot 1: Individual Sockets (Violin + Scatter colored by totalDuration)
for evse, group in df.groupby('idEVSE'):
    # Skip if there's only one point (can't draw a violin) or max power is missing
    if len(group) < 2:
        continue
        
    max_power = group['evse_max_power'].iloc[0]
    
    plt.figure(figsize=(10, 6))
    
    # Violin plot for distribution
    sns.violinplot(
        x='idEVSE', y='ratio', data=group, 
        inner=None, color='lightgray', linewidth=1.5, cut=0
    )
    
    # Scatter plot over it, colored by duration
    # Add a bit of jitter to x-axis to prevent total overlap
    jittered_x = np.random.normal(0, 0.04, size=len(group))
    scatter = plt.scatter(
        x=jittered_x, y=group['ratio'], 
        c=group['totalDuration'], cmap='viridis', 
        alpha=0.7, edgecolors='w', linewidth=0.5, s=50, zorder=3
    )
    
    # Add colorbar
    cbar = plt.colorbar(scatter)
    cbar.set_label('Session Duration (minutes)')
    
    plt.title(f"{max_power} kW - {evse}", fontsize=14, fontweight='bold')
    plt.ylabel("Power Ratio (Actual / Max Theoretical)", fontsize=12)
    plt.xlabel("") # Hide x-label as it's just the socket name in the title
    plt.xticks([]) # Hide x-ticks
    
    # Adjust layout and save
    plt.tight_layout()
    safe_evse_name = str(evse).replace('/', '_').replace('\\', '_')
    plt.savefig(os.path.join(output_dir, f"socket_{safe_evse_name}.png"), dpi=300)
    plt.close()

print("Generating grouped plots by max power...")
# Plot 2: Aggregated by Max Power
for power, group in df.groupby('evse_max_power'):
    # Only plot power levels with multiple sockets/sessions to make it meaningful
    if len(group['idEVSE'].unique()) < 2:
        continue
        
    plt.figure(figsize=(14, 7))
    
    # Use stripplot + violinplot or just boxplot/violinplot
    sns.violinplot(
        data=group, x='idEVSE', y='ratio', 
        inner='quartile', hue='idEVSE', palette='Set2', density_norm='width', legend=False, cut=0
    )
    
    plt.ylim(-0.05, 1.05)
    
    plt.title(f"Distribution of Power Ratio for {int(power)} kW Sockets", fontsize=16, fontweight='bold')
    plt.ylabel("Power Ratio (Actual / Max Theoretical)", fontsize=12)
    plt.xlabel("Socket (idEVSE)", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    
    # Optional: Add horizontal line at ratio = 1 (perfect efficiency)
    plt.axhline(y=1, color='r', linestyle='--', alpha=0.5, label='Ratio = 1')
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"grouped_power_{int(power)}kW.png"), dpi=300)
    plt.close()

print(f"All done! Plots saved to: {output_dir}")
