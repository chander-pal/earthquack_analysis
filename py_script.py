from pyspark.sql import SparkSession
from pyspark.sql.functions import *

jar_path = '/Users/chanderpal/Desktop/Experiments/aidetic/mysql-connector-j-8.2.0.jar'

spark = SparkSession.builder.appName("EarthquakeAnalysis").config("spark.jars", jar_path).getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/test") \
    .option("dbtable", "neic_earthquakes") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("user", "test") \
    .option("password", 'new_password') \
    .load()

df.printSchema()
df = df.withColumn("Date", coalesce(to_timestamp(df["Date"], "MM/dd/yyyy"), to_timestamp(df["Date"], "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))

# How does the Day of a Week affect the number of earthquakes?
df_day_of_week = df.filter(df["Date"].isNotNull()).groupBy(dayofweek("Date").alias("DayOfWeek")).agg(count("*").alias("Count"))
print("How does the Day of a Week affect the number of earthquakes?")
df_day_of_week.orderBy(desc('Count')).show()
df_day_of_week.write.csv("output/day_of_week_analysis")

# What is the relation between Day of the month and Number of earthquakes that happened in a year?
df = df.withColumn("DayOfMonth", dayofmonth("Date"))
df_day_of_month = df.groupBy("DayOfMonth").agg(count("*").alias("Count")).orderBy("DayOfMonth")
print("Day of the month and Number of earthquakes")
df_day_of_month.orderBy(desc('Count')).show()
df_day_of_month.write.csv("output/day_of_month_analysis")

# What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?
df_filtered = df.withColumn("Year", year("Date")).withColumn("Month", month("Date"))
df_filtered = df_filtered.filter((year("Date") >= 1965) & (year("Date") <= 2016))

# Group by year and month, calculate the count, and then calculate the average frequency
df_monthly_frequency = df_filtered.groupBy("Year", "Month").agg(count("*").alias("MonthlyCount"))
df_monthly_frequency_avg = df_monthly_frequency.groupBy("Month").agg(avg("MonthlyCount").alias("AverageFrequency")).orderBy("Month")
print("What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?")
df_monthly_frequency_avg.show()
df_monthly_frequency_avg.write.mode("overwrite").csv("output/monthly_frequency_analysis")

# What is the relation between Year and Number of earthquakes that happened in that year?
df_yearly_count = df.groupBy(year("Date").alias("Year")).agg(count("*").alias("Count")).orderBy("Year")
print('Relation between Year and Number of earthquakes that happened?')
df_yearly_count.orderBy(desc('Count')).show()
df_yearly_count.write.mode("overwrite").csv("output/yearly_count_analysis")

# How has the earthquake magnitude on average been varied over the years?
df_magnitude_variation = df.groupBy(year("Date").alias("Year")).agg(avg("Magnitude").alias("AverageMagnitude")).orderBy("Year")
print('How has the earthquake magnitude on average been varied over the years?')
df_magnitude_variation.show()
df_magnitude_variation.write.mode("overwrite").csv("output/magnitude_variation_analysis")

# How does year impact the standard deviation of the earthquakes?
df_std_deviation = df.groupBy(year("Date").alias("Year")).agg(stddev("Magnitude").alias("StdDevMagnitude"))
print("standard deviation of the earthquakes?")
df_std_deviation.show()
df_std_deviation.write.mode("overwrite").csv("output/std_deviation_analysis")

# Does geographic location have anything to do with earthquakes?
df_location_analysis = df.groupBy("Location_Source").agg(count("*").alias("Count")).orderBy(desc("Count"))
print('Does geographic location have anything to do with earthquakes?')
df_location_analysis.show()
df_location_analysis.write.mode("overwrite").csv("output/location_analysis")

# Where do earthquakes occur very frequently?
df_high_frequency_locations = df.groupBy("Latitude", "Longitude").agg(count("*").alias("Count")).orderBy(desc("Count"))
print("Where do earthquakes occur very frequently?")
df_high_frequency_locations.show()
df_high_frequency_locations.write.mode("overwrite").csv("output/high_frequency_locations")

# What is the relation between Magnitude, Magnitude Type, Status, and Root Mean Square of the earthquakes?
df_magnitude_relation = df.groupBy("Magnitude", "Magnitude_Type", "Status", "Root_Mean_Square").agg(count("*").alias("Count"))
print("elation between Magnitude, Magnitude Type, Status, and Root Mean Square of the earthquakes?")
df_magnitude_relation.show()
df_magnitude_relation.write.mode("overwrite").csv("output/magnitude_relation_analysis")

# Stop Spark session
spark.stop()