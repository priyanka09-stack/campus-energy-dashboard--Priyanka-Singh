import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import matplotlib.pyplot as plt


# Setup Logging
logging.basicConfig(
    filename='data_ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# initialize 
data_folder = Path('data')  # Folder containing CSV files
df_combined = pd.DataFrame()  # Master DataFrame

# loop through CSV file
for file_path in data_folder.glob('*.csv'):
    try:
        
        df = pd.read_csv(file_path, on_bad_lines='skip')
        
        
        df.columns = df.columns.str.strip().str.title()
        
        if 'Timestamp' not in df.columns:
            raise ValueError(f"'Timestamp' column missing in {file_path.name}")
        if 'Consumption' not in df.columns:
            raise ValueError(f"'Consumption' column missing in {file_path.name}")

       
        if 'Building' not in df.columns:
            df['Building'] = file_path.stem
        if 'Month' not in df.columns:
            df['Month'] = pd.to_datetime('today').strftime('%Y-%m')
        
        df_combined = pd.concat([df_combined, df], ignore_index=True)
        logging.info(f'Successfully loaded: {file_path.name}')
    
    except Exception as e:
        logging.error(f'Error with {file_path.name}: {e}')

print("Merged DataFrame preview:")
print(df_combined.head())
print(f"Total rows merged: {len(df_combined)}")
logging.info("Data ingestion completed successfully.")
#Task 2

df_combined['Timestamp'] = pd.to_datetime(df_combined['Timestamp'])

# function- Daily Totals
def calculate_daily_totals(df):
    daily_totals = df.groupby(['Building']).resample('D', on='Timestamp')['Consumption'].sum().reset_index()
    return daily_totals

#  function: weekly aggregates
def calculate_weekly_aggregates(df):
    weekly_totals = df.groupby(['Building']).resample('W-MON', on='Timestamp')['Consumption'].sum().reset_index()
    return weekly_totals

#Function- Building wise Summary
def building_wise_summary(df):
    summary_dict = {}
    buildings = df['Building'].unique()
    for b in buildings:
        building_data = df[df['Building'] == b]['Consumption']
        summary_dict[b] = {
            'Total': building_data.sum(),
            'Mean': building_data.mean(),
            'Min': building_data.min(),
            'Max': building_data.max()
        }
    return summary_dict

# run aggregation
daily_df = calculate_daily_totals(df_combined)
weekly_df = calculate_weekly_aggregates(df_combined)
building_summary = building_wise_summary(df_combined)

# Preview Outputs
print("Daily Totals:")
print(daily_df.head())

print("\nWeekly Totals:")
print(weekly_df.head())

print("\nBuilding-wise Summary:")
for building, stats in building_summary.items():
    print(f"{building}: {stats}")

#Task 3

class MeterReading:
    def __init__(self, timestamp, kwh):
        self.timestamp = timestamp
        self.kwh = kwh


class Building:
    def __init__(self, name):
        self.name = name
        self.meter_readings = []

    def add_reading(self, reading):
        self.meter_readings.append(reading)

    def calculate_total_consumption(self):
        return sum(r.kwh for r in self.meter_readings)

    def generate_report(self):
        consumptions = [r.kwh for r in self.meter_readings]
        report = {
            'Building': self.name,
            'Total_kWh': sum(consumptions),
            'Mean_kWh': sum(consumptions)/len(consumptions) if consumptions else 0,
            'Min_kWh': min(consumptions) if consumptions else 0,
            'Max_kWh': max(consumptions) if consumptions else 0,
            'Total_Readings': len(consumptions)
        }
        return report


class BuildingManager:
    def __init__(self):
        self.buildings = {}

    def add_reading(self, building_name, timestamp, kwh):
        if building_name not in self.buildings:
            self.buildings[building_name] = Building(building_name)
        reading = MeterReading(timestamp, kwh)
        self.buildings[building_name].add_reading(reading)

    def generate_all_reports(self):
        reports = {}
        for name, building in self.buildings.items():
            reports[name] = building.generate_report()
        return reports


manager = BuildingManager()

for index, row in df_combined.iterrows():
    manager.add_reading(row['Building'], pd.to_datetime(row['Timestamp']), row['Consumption'])

all_reports = manager.generate_all_reports()

for building, report in all_reports.items():
    print(report)


#  Setup Dashboard Figure
fig, axes = plt.subplots(3, 1, figsize=(14, 18))
plt.subplots_adjust(hspace=0.4)

#  Trend Line- Daily Consumption 
for building in daily_df['Building'].unique():
    df_building = daily_df[daily_df['Building'] == building]
    axes[0].plot(df_building['Timestamp'], df_building['Consumption'], label=building)
axes[0].set_title('Daily Energy Consumption Trend')
axes[0].set_xlabel('Date')
axes[0].set_ylabel('kWh')
axes[0].legend()
axes[0].grid(True)

#  Bar Chart- Average Weekly Usage per Building
avg_weekly = weekly_df.groupby('Building')['Consumption'].mean().reset_index()
axes[1].bar(avg_weekly['Building'], avg_weekly['Consumption'])
axes[1].set_title('Average Weekly Energy Usage per Building')
axes[1].set_xlabel('Building')
axes[1].set_ylabel('Average kWh')
axes[1].grid(axis='y')

# Scatter Plot: Peak-Hour Consumption 
daily_df['Hour'] = daily_df['Timestamp'].dt.hour
peak_hours = daily_df.groupby(['Building', 'Hour'])['Consumption'].max().reset_index()
for building in peak_hours['Building'].unique():
    df_building = peak_hours[peak_hours['Building'] == building]
    axes[2].scatter(df_building['Hour'], df_building['Consumption'], label=building, s=50)
axes[2].set_title('Peak-Hour Consumption per Building')
axes[2].set_xlabel('Hour of Day')
axes[2].set_ylabel('kWh')
axes[2].legend()
axes[2].grid(True)

plt.tight_layout()
plt.savefig('dashboard.png')
plt.show()

# Task 5
output_folder = os.path.join(os.getcwd(), 'output')
os.makedirs(output_folder, exist_ok=True)

cleaned_file = os.path.join(output_folder, 'cleaned_energy_data.csv')
df_combined.to_csv(cleaned_file, index=False)
print(f"Cleaned data exported to: {cleaned_file}")

building_summary_df = pd.DataFrame([report for report in all_reports.values()])
summary_file = os.path.join(output_folder, 'building_summary.csv')
building_summary_df.to_csv(summary_file, index=False)
print(f"Building summary exported to: {summary_file}")

total_consumption = df_combined['Consumption'].sum()

highest_building = (
    building_summary_df.loc[
        building_summary_df['Total_KWh'].idxmax(), 'Building'
    ] if not building_summary_df.empty else "N/A"
)

df_combined['Hour'] = df_combined['Timestamp'].dt.hour
peak_hour = df_combined.groupby('Hour')['Consumption'].sum().idxmax()

df_combined['Date'] = df_combined['Timestamp'].dt.date
df_combined['Week'] = df_combined['Timestamp'].dt.isocalendar().week

daily_totals = df_combined.groupby('Date')['Consumption'].sum()
weekly_totals = df_combined.groupby('Week')['Consumption'].sum()

daily_file = os.path.join(output_folder, 'daily_totals.csv')
weekly_file = os.path.join(output_folder, 'weekly_totals.csv')

daily_totals.to_csv(daily_file, header=['Daily_Consumption'])
weekly_totals.to_csv(weekly_file, header=['Weekly_Consumption'])

print(f"Daily totals exported to: {daily_file}")
print(f"Weekly totals exported to: {weekly_file}")

summary_text = f"""
Campus Energy Executive Summary
-----------------------------------
Total Campus Consumption: {total_consumption:.2f} kWh
Highest Consuming Building: {highest_building}
Peak Load Hour: {peak_hour}:00

Daily Consumption Trends:
"""

for date, value in daily_totals.items():
    summary_text += f"{date}: {value:.2f} kWh\n"

summary_text += "\nWeekly Consumption Trends:\n"
for week, value in weekly_totals.items():
    summary_text += f"Week {week}: {value:.2f} kWh\n"

summary_text += f"\nDetailed CSV summaries saved in the '{output_folder}' folder."

summary_txt_file = os.path.join(output_folder, 'summary.txt')
with open(summary_txt_file, 'w') as f:
    f.write(summary_text)

print(f"Executive summary saved to: {summary_txt_file}")

test_file = os.path.join(output_folder, 'test.txt')
with open(test_file, 'w') as f:
    f.write("Hello, this is a test!")

print(f"Test file saved at: {test_file}")
