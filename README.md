# Airline Loyalty Insights

## Overview
This project analyzes airline loyalty programs using customer flight activity and loyalty history datasets. The goal is to uncover flight trends, loyalty point usage patterns, and identify correlations between customer loyalty and revenue generation.

## Prerequisites
- Python 3.8+
- PySpark
- Libraries:
  - matplotlib
  - seaborn
- Development Tools:
  - Visual Studio Code
  - Git

## Installation & Setup
1. Clone the repository
```bash
git clone https://github.com/abilfarabil/airline-loyalty-insights.git
cd airline_loyalty_insights
```

## Project Structure
```
airline_loyalty_insights/
├── data/
│   ├── calendar.csv
│   ├── customer_flight_activity.csv
│   └── customer_loyalty_history.csv
├── output/
│   ├── graphs/
│   └── results/
└── analysis.py
```

## Features
- Flight trend analysis by month
- Loyalty points collection and usage tracking
- Revenue correlation with loyalty status
- Average flight distance analysis
- Point usage percentage by loyalty status

## Documentation

### Data Processing Pipeline
1. **Data Extraction**
   - Loads CSV files from `data/` directory into PySpark DataFrames
   - Handles customer flight activity and loyalty history data

2. **Data Cleaning**
   - Null value validation
   - Data type verification
   - Duplicate removal
   - Data quality assurance

3. **Data Transformation**
   - Merges flight and loyalty data
   - Calculates redeemed percentage
   - Assigns loyalty status categories
   - Creates analytical features

4. **Analysis & Visualization**
   - Generates monthly flight trends
   - Analyzes points collection and usage
   - Correlates revenue with loyalty status
   - Calculates average flight distances
   - Visualizes point usage by loyalty status

## Technologies Used
- **Python**
- **PySpark**
- **Visualization Libraries**:
  - matplotlib
  - seaborn
- **Data Formats**: CSV, Parquet

## Output Structure
### Graphs (`output/graphs/`)
- Monthly flight trends
- Points collection patterns
- Revenue by loyalty status
- Average flight distances
- Points usage analysis

### Results (`output/results/`)
Generated CSV/Parquet files:
- trend_flights.csv
- points_usage_by_loyalty_status.csv
- [other analysis results]

## Screenshots

### Project Structure
![Project Folder Structure](images/1_Struktur_Proyek_Folder.png)

### Main Script
![analysis.py - Part 1](images/2_Script_Utama_analysis.py.png)
![analysis.py - Part 2](images/3_Script_Utama_analysis.py.png)
![analysis.py - Part 3](images/4_Script_Utama_analysis.py.png)
![analysis.py - Part 4](images/5_Script_Utama_analysis.py.png)
![analysis.py - Part 5](images/6_Script_Utama_analysis.py.png)

### Visualization Results
![Monthly Flight Trends](images/7_trend_flights.png)
![Points Collection](images/8_points_collected.png)
![Revenue by Loyalty Status](images/9_income_by_loyalty_status.png)
![Average Flight Distance](images/10_average_distance.png)
![Points Usage by Loyalty Status](images/11_points_usage_by_loyalty_status.png)

### Final Output
![Final Output Results](images/12_Output_Hasil_Akhir_Results_dan_Graphs.png)

## Contributing
Feel free to fork this repository and submit pull requests for any improvements.
