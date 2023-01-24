from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, regexp_extract, col, split, cast, corr, avg, sum, collect_set, count, asc
from pyspark.sql.types import *
import pandas as pd
import os
import time
import matplotlib.pyplot as plt
spark = SparkSession.builder.appName('Dataframe').getOrCreate()
t0 = time.time()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("zomato.csv")
print("Schema of the dataset")
df.printSchema()

print("Query 1. Does average rating of restaurants who accepts online_order is greater than the average rating of restaurants who don't accept online order?")
df.select('online_order', 'rate').groupBy('online_order').avg().show()
print("Ans: Yes, From above table we can say that average rating of restaurants who accepts online_order is greater than the average rating of restaurants who don't accept online order.")

print("Query 2. Does average rating of restaurants who accepts online table booking is greater than the average rating of restaurants who don't accept online table booking?")
df.select('book_table', 'rate').groupBy('book_table').avg().show()
print("Query Ans: Yes, From above table, we can say that average rating of restaurants who accepts online table booking is greater than the average rating of restaurants who don't online table booking.")

print("Query 3. What are the top 3 locations in bangalore with highest number of restaurants?")
print("Top 3 locations are given below:")
df.groupBy('location').count().sort(col('count').desc()).select('location').show(3,False)

print("Query 4. Which location has highest number of pubs?")
keywords = 'Pub'
df.filter(col('rest_type').contains('Pub')).groupBy('location').count().sort(col('count').desc()).select('location').show(1,False)

print("Query 5. Which is the cheapest restaurant with rating >= 4.9?")
df.where("rate >= 4.9").sort(col('cost_for_two_people')).select('name').show(1,False)

print('Query 6. Displaying the basic statistics using describe function')
df.describe().show()

print('Query 7. Restaurants in whitefield location with cost less than or equal to 1000 and drawing 10 samples randomly')
seed=5
withReplacement = False
fraction = 0.5
df.where(col("location")=="Whitefield").where(col("cost_for_two_people")<=1000).sample(withReplacement,fraction, seed=None).show(10)

print('Query 8. Cheaper restaurants(cost<800) with ratings greater than equal to 4')
df.where(col("cost_for_two_people")<=800).where(col("rate")>=4).orderBy(asc("cost_for_two_people")).show()

print('Query 9. Number of restaurants in Banashankari location')
df.where(col("location")=="Banashankari").select("name").count()

print('Query 10. Restaurants having both "Online order" & "table booking" facilities')
df.withColumn("both", col("book_table")&col("online_order")).where("both").select("book_table","online_order","both").show()



dfCorr = df.withColumnRenamed("cost_for_two_people","cost")
dfCorr = dfCorr.withColumn("online_order", df.online_order.cast('int')).withColumn("book_table", df.book_table.cast('int'))

#1. is there a correlation between the cost of the restaurant and the ratings
x= dfCorr.corr("cost", "rate")
print("Query 11. The correlation between the cost of the restaurant and ratings is: ", x)

#3. Do restaurants that offer online booking differ from the ones that offer table bookings

df_cov= dfCorr.stat.cov("online_order", "book_table")
print("Query 12. The direction of the relationship between restaurants that have online booking and table booking is ", df_cov)

#4 Collect List for Location 

Online = df.groupby('name', 'location', 'online_order').agg(sum('votes').alias('TotalVotes_Online')).withColumnRenamed('online_order', 'Order')
book = df.groupby('name', 'location', 'book_table').agg(sum('votes').alias('TotalVotes_Book')).withColumnRenamed('book_table', 'Book')
table = Online.join(book, ['name', 'location'],  'inner')

df2 = table.groupBy("Location").agg(collect_set("name").alias("Location_Rest"), count("Location").alias(""))
print("Query 13. This table shows the list of restaurants by their locations ")
df2.show()


df.createOrReplaceTempView("data_sql")

### Retrieve max and min rates for binning

numBins = 20 ### Define amount of bins to have
maxCost = float(spark.sql("SELECT max(cost_for_two_people) FROM data_sql").collect()[0][0])
minCost = float(spark.sql("SELECT min(cost_for_two_people) FROM data_sql").collect()[0][0])

### Find differences for binning

diffCost = maxCost - minCost
diffBin = diffCost / numBins

diffBinCost = diffBin

### Initalize counts
costsIndex = []
counts = []

### Binning of costs for histogram
for i in range(numBins-1):
    cost = minCost + i * diffBin
    nextCost = cost + diffBin
    costsIndex.append(cost)
    counts.append(float(df.filter(col("cost_for_two_people") >= cost).filter(col("cost_for_two_people") < nextCost).count()))

### Likewise but with rates to compare the distributions

maxRate = float(spark.sql("SELECT max(rate) FROM data_sql").collect()[0][0])
minRate = float(spark.sql("SELECT min(rate) FROM data_sql").collect()[0][0])

### Find differences for binning

diffRate = maxRate - minRate
diffBin = diffRate / numBins

### Initalize rate counting
ratesIndex = []
countRates = []

### Binning of costs for histogram
for i in range(numBins-1):
    rate = minRate + i * diffBin
    nextRate = rate + diffBin
    ratesIndex.append(rate)
    countRates.append(float(df.filter(col("rate") >= rate).filter(col("rate") < nextRate).count()))


### Collecting data for scatter graph of the rate versus cost
rates = spark.sql("SELECT rate FROM data_sql").collect()
costs = spark.sql("SELECT cost_for_two_people FROM data_sql").collect()

### Initalize empty rates and cost lists for plotting

ratesList = []
costsList = []

### Append float value from each row collected from spark dataframe

for i in range(len(rates)):
    ratesList.append(float(rates[i][0]))
    costsList.append(float(costs[i][0]))


### Form graphs and print in display    

plt.figure()
plt.scatter(ratesList, costsList)
plt.title("Resturant Rate-Cost Space Comparison")
plt.xlabel("Rates")
plt.ylabel("Costs for Two Persons to Eat")

plt.figure()
plt.bar(ratesIndex, countRates)
plt.xlabel("Rate Ranges")
plt.ylabel("Frequency (# Resturants)")
plt.title("Frequency Distribution of Rates by Resturants")

print(costsIndex)

plt.figure()
plt.bar(costsIndex, counts, width=diffBinCost)
plt.xlabel("Cost Ranges")
plt.ylabel("Frequence (# Resturants)")
plt.title("Frequency Distribution of Costs by Resturants")

t1 = time.time()

print("Time taken = " + str(t1 - t0))



