# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 12:55:28 2015

@author: karunsiddana
"""
'''
Lab: Reading and Writing Files in Python
'''

'''
PART 1:
Read in drinks.csv
Store the header in a list called 'header'
Store the data in a list of lists called 'data'
Hint: you've already seen this code!
'''

#Read the values of the drinks.csv
f = open('drinks.csv', 'rU')
data_set = f.readlines()
f.close()

#Using list comprehensions read in the file and store it in lines    
data = [line.rstrip().split(',') for line in data_set]
header = data[0]    
data = data[1:]
    

'''
PART 2:
Isolate the beer_servings column in a list of integers called 'beers'
Hint: you can use a list comprehension to do this in one line
Expected output:
    beers == [0, 89, ..., 32, 64]
    len(beers) == 193
'''

beers = [line[1] for line in data]

#[line[1] for line in data if line[0][0] == 'A']

'''
PART 3:
Create separate lists of NA and EU beer servings: 'NA_beers', 'EU_beers'
Hint: you can use a list comprehension with a condition
Expected output:
    NA_beers == [102, 122, ..., 197, 249]
    len(NA_beers) == 23
    EU_beers == [89, 245, ..., 206, 219]
    len(EU_beers) == 45
'''

#Getting all the beers for North America
NA_beers = [line[1] for line in data if line[5] == "NA"]

#Geeting all the beers for Europe
EU_beers = [line[1] for line in data if line[5] == "EU"]

'''
PART 4:
Calculate the average NA and EU beer servings to 2 decimals: 'NA_avg', 'EU_avg'
Hint: don't forget about data types!
Expected output:
    NA_avg == 145.43
    EU_avg == 193.78
'''

#convert strings to ints
NA_beers = [float(item) for item in NA_beers]
EU_beers = [float(item) for item in EU_beers]

#Round the Average to 2 decimal places)
NA_avg = round(sum(NA_beers) / len(NA_beers), 2)

EU_avg = round(sum(EU_beers) / len(EU_beers), 2)

'''
PART 5:
Write a CSV file called 'avg_beer.csv' with two columns and three rows.
The first row is the column headers: 'continent', 'avg_beer'
The second and third rows contain the NA and EU values.
Hint: think about what data structure will make this easy
Expected output (in the actual file):
    continent,avg_beer
    NA,145.43
    EU,193.78
'''
avg_beers_list = [['continent', 'avg_beer'], ['NA', NA_avg], ['EU', EU_avg]]

import csv
with open('avg_beer.csv', 'wb') as f:
    csv.writer(f).writerows(avg_beers_list)
f.close()

'''
Part 6:
Use the requests module to pull in weather data for any city
Hint: you can use Istanbul from the other code file but you can search
for cities at http://openweathermap.org/find

Create a dates list that stores the date of each datapoint as well as
temperature and humidity

You've already seen this code!
'''
import requests # a module for reading the web
api_endpoint = 'http://api.openweathermap.org/data/2.5/forecast/city'
params = {}
params['id'] = '1261481'
params['units'] = 'metric'
params['APPID'] = '80575a3090bddc3ce9f363d40cee36c2'
request = requests.get(api_endpoint, params = params)

# Look at the text
request.text

# parse out the json from this request
data = request.json()

# Let's store the lists as their own variables
weather_data = data['list']

# use a list comprehension to get the dates out of the data
dates = [data_point['dt'] for data_point in weather_data]

from datetime import datetime
dates = [datetime.fromtimestamp(epoch) for epoch in dates]
dates # now in datetime format

#use a list comprehension to get the temperatures out of the data
temperatures = [data_point['main']['temp'] for data_point in weather_data]  

#use a list comprehension to get the humidity out of the data
humidity = [data_point['main']['humidity'] for data_point in weather_data]

'''
Part 7
Create a list of the pressure measurements and plot it against dates
'''
pressure = [data_point['main']['pressure'] for data_point in weather_data]

# Data is awesome, and so are graphs
import matplotlib.pyplot as plt

#plt.plot(dates, pressure)

plt.xlabel("Date")                          # set the x axis label
plt.ylabel("Pressure")                      # set the y axis label
plt.title("Pressure today in New Delhi, India")  # set the title
locs, labels = plt.xticks()                 # get the x tick marks
plt.setp(labels, rotation=70)               # rotate the x ticks marks by 70 degrees
plt.plot(dates, temperatures)               # plot again


'''
Part 8
Make a scatter plot plotting pressure against temperature and humidity
'''

temperatures_normalized = [float(t) / max(temperatures) for t in temperatures]
humidity_normalized = [float(h) / max(humidity) for h in humidity]
pressure_normalized = [float(p) / max(pressure) for p in pressure]

plt.scatter(temperatures_normalized,pressure_normalized, color='r',)
plt.xticks()

plt.legend()
locs, labels = plt.xticks()                 # get the x tick marks
plt.setp(labels, rotation=70)               # rotate the x ticks marks by 70 degrees
plt.plot(dates, humidity_normalized, label='Humidity')
plt.plot(dates, temperatures_normalized, marker='o', linestyle='--', color='r', label='Temperature')

# Let's try a scatter plot!

plt.scatter(pressure, humidity)

'''
BONUS:
Learn csv.DictReader() and use it to redo Parts 1, 2, and 3.
'''
'''
PART 1:
Read in drinks.csv
Store the header in a list called 'header'
Store the data in a list of lists called 'data'
Hint: you've already seen this code!
'''
import csv
with open('drinks.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    header = reader.next()
    csvdata = [row for row in reader]
    
'''
PART 2:
Isolate the beer_servings column in a list of integers called 'beers'
Hint: you can use a list comprehension to do this in one line
Expected output:
    beers == [0, 89, ..., 32, 64]
    len(beers) == 193
'''

beers1 = [beerdata['beer_servings'] for beerdata in csvdata]
len(beers1)

'''
PART 3:
Create separate lists of NA and EU beer servings: 'NA_beers', 'EU_beers'
Hint: you can use a list comprehension with a condition
Expected output:
    NA_beers == [102, 122, ..., 197, 249]
    len(NA_beers) == 23
    EU_beers == [89, 245, ..., 206, 219]
    len(EU_beers) == 45
'''

NA_beers1 = [NA_beers1['beer_servings'] for NA_beers1 in csvdata if NA_beers1['continent'] == 'NA']
EU_beers1 = [EU_beers1['beer_servings'] for EU_beers1 in csvdata if EU_beers1['continent'] == 'EU']

countries = [country['country'] for country in csvdata]
India = [row for row in csvdata if row['country'] == "India"]
Australia = [row for row in csvdata if row['country'] == "Australia"]


