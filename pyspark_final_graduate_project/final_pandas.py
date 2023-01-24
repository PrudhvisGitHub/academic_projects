import pandas as pd
import os
import time
import matplotlib.pyplot as plt

t0 = time.time()
df = pd.read_csv('zomato.csv')
print("Query 1. Does average rating of restaurants who accepts online_order is greater than the average rating of restaurants who don't accept online order?")
print(df[['online_order','rate']].groupby('online_order').mean().reset_index())
print("Ans: Yes, From above table we can say that average rating of restaurants who accepts online_order is greater than the average rating of restaurants who don't accept online order.")

print("Query 2. Does average rating of restaurants who accepts online table booking is greater than the average rating of restaurants who don't accept online table booking?")
print(df[['book_table','rate']].groupby('book_table').mean().reset_index())
print("Ans: Yes, From above table, we can say that average rating of restaurants who accepts online table booking is greater than the average rating of restaurants who don't online table booking.")

print("Query 3. What are the top 3 locations in bangalore with highest number of restaurants?")
print("Top 3 locations are given below:")
print(df.groupby('location').count().sort_values(by='name',ascending=False).reset_index()['location'][0:3])

print("Query 4. Which location has highest number of pubs?")
print("Ans: "+df[df['rest_type'].str.contains('Pub')].reset_index().groupby('location').count().sort_values(by='index',ascending=False).reset_index()['location'][0])

print("Query 5. Which is the cheapest restaurant with rating >= 4.9?")
print("Ans: " + df[df['rate']>=4.9].reset_index().sort_values(by='cost_for_two_people').reset_index()['name'][0])

#finding the basic statistics using describe function
print('Query 6. Displaying the basic statistics using describe function')
print(df.describe())

#finding the restaurant in whitefield location with cost less than or equal to 1000 and drawing 10 samples randomly
print('Query 7. Restaurants in whitefield location with cost less than or equal to 1000 and drawing 10 samples randomly')
moderate_rate=df[(df['location']=='Whitefield') & (df['cost_for_two_people'] <=1000)]
print(moderate_rate.sample(10))

#finding the cheaper restaurants(cost<=800) with ratings gretaer than equal to 4
print('Query 8. Cheaper restaurants(cost<=800) with ratings gretaer than equal to 4')
budget_friendly=(df[(df['rate']>=4 ) & (df['cost_for_two_people'] <=800)])
print(budget_friendly.sort_values(by='cost_for_two_people', ascending=True))

#How many restaurants are there in Banashankari location
print('Query 9. Number of restaurants in Banashankari location')
a=df[(df['location']=='Banashankari')]
print(len(a))

#restaurants having both "Online order" & "table booking" facilities
print('Query 10. Restaurants having both "Online order" & "table booking" facilities')
ot=df[(df['online_order']) & (df['book_table'])]
print(ot[['name','online_order','book_table']])


# is there a correlation between the cost of the restaurant and the ratings
col1 = "rate"
col2 = "cost_for_two_people"
col3 = "online_order"

corr = df[col1].corr(df[col2], method='pearson')
print("Query 11. The correlation coefficient between cost and ratings is ", corr)

# The covariance of restaurants that offer online orders v book tables 
col3, col4 = "online_order", "book_table"
cleaned_cov = df[col3].cov(df[col4])
print("Query 12. The direction of the relationship between restaurants that have online booking and table booking is", cleaned_cov)

#Restaurant by Location 
cleaned_4 = df.groupby("location").agg(loc_count = ('location', 'count'), loc_rest =('name', 'unique'))
print("Query 13. This table shows the list of restaurants by their locations")
print(cleaned_4)

   
### Retrieve max and min rates for binning

numBins = 20 ### Define amount of bins to have
maxCost = max(df['cost_for_two_people'])
minCost = min(df['cost_for_two_people'])

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
    counts.append(df[(df['cost_for_two_people']>= cost) & (df['cost_for_two_people'] < nextCost)]['cost_for_two_people'].count())
### Likewise but with rates to compare the distributions

maxRate = max(df['rate'])
minRate = min(df['rate'])

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
    countRates.append(df[(df['rate'] >= rate) & (df['rate'] < nextRate)]['rate'].count())


### Collecting data for scatter graph of the rate versus cost
ratesList = df['rate']
costsList = df['cost_for_two_people']

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