from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, when, format_number
import matplotlib.pyplot as plt
import seaborn as sns

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Airline Loyalty Insights - Data Cleaning") \
    .getOrCreate()

# Load data dari CSV
calendar_df = spark.read.csv('data/calendar.csv', header=True, inferSchema=True).drop('_c0')
flight_activity_df = spark.read.csv('data/customer_flight_activity.csv', header=True, inferSchema=True).drop('_c0')
loyalty_history_df = spark.read.csv('data/customer_loyalty_history.csv', header=True, inferSchema=True).drop('_c0')

# 3.1: Pembersihan Data
# a. Cek nilai null di setiap DataFrame
print("Checking for null values in each DataFrame:")
calendar_df.show(5)
flight_activity_df.show(5)
loyalty_history_df.show(5)

# Mengisi nilai kosong dengan default values (misal, 0 untuk nilai numerik)
flight_activity_df = flight_activity_df.fillna({
    'total_flights': 0, 
    'distance': 0.0, 
    'points_accumulated': 0, 
    'points_redeemed': 0,
    'dollar_cost_points_redeemed': 0.0
})

# b. Cek duplikasi dan hapus duplikat berdasarkan loyalty_number
print("Checking for duplicate records:")
flight_activity_df = flight_activity_df.dropDuplicates(['loyalty_number'])
loyalty_history_df = loyalty_history_df.dropDuplicates(['loyalty_number'])

# 3.2: Konsistensi Tipe Data
flight_activity_df = flight_activity_df.withColumn("year", col("year").cast("int"))
flight_activity_df = flight_activity_df.withColumn("month", col("month").cast("int"))
flight_activity_df = flight_activity_df.withColumn("total_flights", col("total_flights").cast("int"))
flight_activity_df = flight_activity_df.withColumn("distance", col("distance").cast("float"))
flight_activity_df = flight_activity_df.withColumn("date", concat_ws("-", col("year"), col("month")))

# 4.1: Menggabungkan Data
# Gabungkan flight_activity_df dan loyalty_history_df
combined_df = flight_activity_df.join(loyalty_history_df, on='loyalty_number', how='left')

# 4.2: Membuat Kolom Tambahan
# Contoh: Menghitung persentase poin yang telah ditebus dari poin yang terakumulasi
combined_df = combined_df.withColumn(
    "redeemed_percentage",
    when(col("points_accumulated") > 0, col("points_redeemed") / col("points_accumulated") * 100).otherwise(0)
)

# Contoh: Status Loyalitas
combined_df = combined_df.withColumn(
    "loyalty_status",
    when(col("points_accumulated") > 10000, "Gold")
    .when(col("points_accumulated") > 5000, "Silver")
    .otherwise("Bronze")
)

# Tampilkan hasil akhir setelah penggabungan data
print("Combined DataFrame:")
combined_df.show(5)

# Buat Temporary View
combined_df.createOrReplaceTempView("combined_data")

# 2. Analisis 1: Tren Penerbangan Bulanan
monthly_flights_query = """
SELECT year, month, SUM(total_flights) AS total_monthly_flights
FROM combined_data
GROUP BY year, month
ORDER BY year, month
"""
monthly_flights_df = spark.sql(monthly_flights_query)
# Format angka untuk lebih sedikit desimal
monthly_flights_df = monthly_flights_df.withColumn("total_monthly_flights", format_number(col("total_monthly_flights"), 0))
print("Tren Penerbangan Bulanan:")
monthly_flights_df.show()

# 3. Analisis 2: Penggunaan dan Pengumpulan Poin
points_usage_query = """
SELECT year, month, SUM(points_accumulated) AS total_points_accumulated, 
       SUM(points_redeemed) AS total_points_redeemed
FROM combined_data
GROUP BY year, month
ORDER BY year, month
"""
points_usage_df = spark.sql(points_usage_query)
# Format angka
points_usage_df = points_usage_df.withColumn("total_points_accumulated", format_number(col("total_points_accumulated"), 0))
points_usage_df = points_usage_df.withColumn("total_points_redeemed", format_number(col("total_points_redeemed"), 0))
print("Penggunaan dan Pengumpulan Poin:")
points_usage_df.show()

# 4. Analisis 3: Hubungan Pendapatan dan Status Loyalitas
revenue_loyalty_status_query = """
SELECT loyalty_status, AVG(dollar_cost_points_redeemed) AS average_revenue
FROM combined_data
GROUP BY loyalty_status
ORDER BY average_revenue DESC
"""
revenue_loyalty_status_df = spark.sql(revenue_loyalty_status_query)
# Format angka
revenue_loyalty_status_df = revenue_loyalty_status_df.withColumn("average_revenue", format_number(col("average_revenue"), 2))
print("Hubungan Pendapatan dan Status Loyalitas:")
revenue_loyalty_status_df.show()

# 5. Analisis 4: Rata-rata Jarak Per Penerbangan
average_distance_query = """
SELECT year, month, AVG(distance) AS average_distance
FROM combined_data
GROUP BY year, month
ORDER BY year, month
"""
average_distance_df = spark.sql(average_distance_query)
# Format angka
average_distance_df = average_distance_df.withColumn("average_distance", format_number(col("average_distance"), 2))
print("Rata-rata Jarak Per Penerbangan:")
average_distance_df.show()

# 6. Analisis 5: Persentase Penggunaan Poin per Status Loyalitas
points_usage_percentage_query = """
SELECT loyalty_status, 
       SUM(points_redeemed) / SUM(points_accumulated) * 100 AS redemption_percentage
FROM combined_data
WHERE points_accumulated > 0
GROUP BY loyalty_status
ORDER BY redemption_percentage DESC
"""
points_usage_percentage_df = spark.sql(points_usage_percentage_query)
# Format angka
points_usage_percentage_df = points_usage_percentage_df.withColumn("redemption_percentage", format_number(col("redemption_percentage"), 2))
print("Persentase Penggunaan Poin per Status Loyalitas:")
points_usage_percentage_df.show()

# Visualisasi 1: Tren Penerbangan Bulanan
monthly_flights_pd = monthly_flights_df.toPandas()  # Konversi ke Pandas DataFrame untuk visualisasi
plt.figure(figsize=(10, 6))
sns.lineplot(data=monthly_flights_pd, x='month', y='total_monthly_flights', marker='o', hue='year')
plt.title('Tren Jumlah Penerbangan per Bulan')
plt.xlabel('Bulan')
plt.ylabel('Total Penerbangan')
plt.xticks(rotation=45)
plt.grid(True)
plt.savefig('output/graphs/trend_flights.png')  # Simpan grafik
plt.show()

# Visualisasi 2: Penggunaan dan Pengumpulan Poin
points_usage_pd = points_usage_df.toPandas()  # Konversi ke Pandas DataFrame untuk visualisasi
plt.figure(figsize=(10, 6))
sns.barplot(data=points_usage_pd, x='month', y='total_points_accumulated', hue='year')
plt.title('Total Poin yang Terakumulasi per Bulan')
plt.xlabel('Bulan')
plt.ylabel('Total Poin')
plt.xticks(rotation=45)
plt.savefig('output/graphs/points_collected.png')  # Simpan grafik
plt.show()

# Visualisasi 3: Hubungan Pendapatan dan Status Loyalitas
revenue_loyalty_status_pd = revenue_loyalty_status_df.toPandas()  # Konversi ke Pandas DataFrame
plt.figure(figsize=(10, 6))
sns.barplot(data=revenue_loyalty_status_pd, x='loyalty_status', y='average_revenue')
plt.title('Rata-rata Pendapatan berdasarkan Status Loyalitas')
plt.xlabel('Status Loyalitas')
plt.ylabel('Rata-rata Pendapatan (USD)')
plt.savefig('output/graphs/income_by_loyalty_status.png')  # Simpan grafik
plt.show()

# Visualisasi 4: Rata-rata Jarak Per Penerbangan
average_distance_pd = average_distance_df.toPandas()  # Konversi ke Pandas DataFrame
plt.figure(figsize=(10, 6))
sns.lineplot(data=average_distance_pd, x='month', y='average_distance', marker='o', hue='year')
plt.title('Rata-rata Jarak Penerbangan per Bulan')
plt.xlabel('Bulan')
plt.ylabel('Rata-rata Jarak (km)')
plt.xticks(rotation=45)
plt.grid(True)
plt.savefig('output/graphs/average_distance.png')  # Simpan grafik
plt.show()

# Visualisasi 5: Persentase Penggunaan Poin per Status Loyalitas
points_usage_percentage_pd = points_usage_percentage_df.toPandas()  # Konversi ke Pandas DataFrame
plt.figure(figsize=(8, 8))
plt.pie(points_usage_percentage_pd['redemption_percentage'], labels=points_usage_percentage_pd['loyalty_status'], autopct='%1.1f%%', startangle=90)
plt.title('Persentase Penggunaan Poin berdasarkan Status Loyalitas')
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.savefig('output/graphs/points_usage_by_loyalty_status.png')  # Simpan grafik
plt.show()

import os

# Fungsi untuk mengganti nama file partisi default menjadi nama yang diinginkan
def rename_csv(output_dir, new_filename):
    # Dapatkan nama file partisi default yang dibuat oleh Spark
    for file in os.listdir(output_dir):
        if file.startswith("part") and file.endswith(".csv"):
            # Buat path lengkap dari file partisi dan file baru
            os.rename(os.path.join(output_dir, file), os.path.join(output_dir, new_filename))

# Simpan hasil analisis ke folder sementara
monthly_flights_df.write.mode('overwrite').csv('output/results/trend_flights_output/')
rename_csv('output/results/trend_flights_output', 'trend_flights.csv')

average_distance_df.write.mode('overwrite').csv('output/results/average_distance_output/')
rename_csv('output/results/average_distance_output', 'average_distance.csv')

revenue_loyalty_status_df.write.mode('overwrite').csv('output/results/income_by_loyalty_status_output/')
rename_csv('output/results/income_by_loyalty_status_output', 'income_by_loyalty_status.csv')

points_usage_df.write.mode('overwrite').csv('output/results/points_collected_output/')
rename_csv('output/results/points_collected_output', 'points_collected.csv')

points_usage_percentage_df.write.mode('overwrite').csv('output/results/points_usage_by_loyalty_status_output/')
rename_csv('output/results/points_usage_by_loyalty_status_output', 'points_usage_by_loyalty_status.csv')

# Stop the Spark session
spark.stop()