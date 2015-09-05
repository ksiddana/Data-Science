# -*- coding: utf-8 -*-
"""
Created on Sat Aug 29 20:39:03 2015

@author: karunsiddana
"""

import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect('consumer_database.sqlite')

disk_engine = create_engine('sqlite:///consumer_database.sqlite')
df_requests = pd.read_sql_query('SELECT * FROM requests', disk_engine)
df_quotes = pd.read_sql_query('SELECT * FROM quotes', disk_engine)
df_invites = pd.read_sql_query('SELECT * FROM invites', disk_engine)
df_locations = pd.read_sql_query('SELECT * FROM locations', disk_engine)
df_categories = pd.read_sql_query('SELECT * FROM categories', disk_engine)

#Try seeing some samples and how the dataframes are related
df_quotes[df_quotes.invite_id == 4]
df_invites[df_invites.invite_id == 4]
df_requests[df_requests.request_id == 1]
df_locations[df_locations.location_id == 35]
df_categories[df_categories.category_id == 46]

#Now lets see find some trends in Photography
df_requests[df_requests.category_id == 1]
df_invites[df_invites.invite_id == 4]

#plot between request_id = 311
df_invites[df_invites.request_id == 311].groupby('request_id').plot(kind='bar')

#Merge dataframes using inner join on the Location dataframe and the request_dataframe
frame = pd.DataFrame.merge(df_requests, df_locations, left_on='location_id', right_on='location_id')

#MErge Categories and Location Dataframe into one dataframe using category_id as the key
frame1 = pd.DataFrame.merge(frame, df_categories, left_on='category_id', right_on='category_id')

#Merge Invite Dataframe with the Request Dataframe using the request_id as the key do an inner join
frame2 = pd.DataFrame.merge(frame1, df_invites, left_on='request_id', right_on='request_id')

#Merge Quotes dataframe to the Final Dataframe including Requests, Invites and time 
#using the invite_id as the Key for the inner join.
final_df = pd.DataFrame.merge(frame2, df_quotes, left_on='invite_id', right_on='invite_id')
#Index([u'request_id', u'user_id_x', u'category_id', u'location_id', u'creation_time', 
#u'name_x', u'name_y', u'invite_id', u'user_id_y', u'sent_time_x', 
#u'quote_id', u'sent_time_y'], dtype='object')

#Format the Columns in to Dates and Time Columns
import matplotlib.dates as DT

##------------------------------------------------------------------
#Conver the time the Request was created by the User and entered into the database
#final_df['creation_time_test'] = pd.to_datetime(final_df.creation_time, "%Y-%M-%D")
final_df['c_date'] = final_df['creation_time'].map(lambda x: x[:10])
final_df['c_time'] = final_df['creation_time'].map(lambda x: x[11:-7])
#final_df['c_date2num'] = final_df['c_date'].map(lambda x: DT.datestr2num(x))
final_df['c_date2num'] = pd.to_datetime(final_df.c_date)
final_df['c_time2num'] = pd.to_timedelta(final_df.c_time)
#final_df['c_time2num'] = pd.to_datetime(final_df.c_time)

##------------------------------------------------------------------
#Convert the time the Invite was sent to the User to Format Required
final_df['i_date'] = final_df['sent_time_x'].map(lambda x: x[:10])
final_df['i_time'] = final_df['sent_time_x'].map(lambda x: x[11:-7])
#final_df['i_date2num'] = final_df['i_date'].map(lambda x: DT.datestr2num(x))
final_df['i_date2num'] = pd.to_datetime(final_df.i_date)
final_df['i_time2num'] = pd.to_timedelta(final_df.i_time)

##------------------------------------------------------------------
#Conver the time the Quote was sent back to the Database from the user in the Format Required
final_df['q_date'] = final_df['sent_time_y'].map(lambda x: x[:10])
final_df['q_time'] = final_df['sent_time_y'].map(lambda x: x[11:-7])
#final_df['q_date2num'] = final_df['q_date'].map(lambda x: DT.datestr2num(x))
final_df['q_date2num'] = pd.to_datetime(final_df.q_date)
final_df['q_time2num'] = pd.to_timedelta(final_df.q_time)

# final_df.drop(['creation_time', 'sent_time_x', 'sent_time_y', 'c_date','c_time', 
#'i_date','i_time' ,'q_date', 'q_time'], axis=1, inplace=True)

#This is the relationship that we are looking for
final_df[['i_time', 'q_time']]
final_df[['i_date2num','q_date2num','i_time2num', 'q_time2num']]

##------------------------------------------------------------------
#Plot some Line Graphs
colors = ['b','g','r']
plt.plot(final_df.q_date2num, final_df.category_id )
plt.plot_date(final_df.i_date2num, final_df.category_id)

plt.scatter(final_df.i_time2num.head(50), color='red')
plt.scatter(final_df.q_time2num.head(50), color='green')
plt.plot(final_df.i_time2num.head(50), final_df.q_time2num.head(50), 'bo')
final_df.groupby('location_id').i_date2num.hist(sharex=True)

import matplotlib.pyplot as plt
colors = ['b','g','r','c','m']
plt.bar(final_df.i_date2num, final_df.q_date2num, align = 'center', color = colors)
plt_obj = final_df.set_index('i_date2num').plot()

from datetime import datetime
import numpy as np


def convert_to_12(x):
    if x > 12

y1 = final_df.i_time2num[final_df.category_id == 1]
y2 = final_df.q_time2num[final_df.category_id == 1]

x1 = final_df.i_time2num[final_df.category_id == 1]
x2 = final_df.q_time2num[final_df.category_id == 1]

#dt = np.datetime64(final_df.c_time2num) - np.datetime64(final_df.c_time2num)

final_df['ans'] = final_df.i_time2num.apply(lambda x: x  / np.timedelta64(1,'m')).astype('int64') % (24*60)
y = (y2-y1).astype('timedelta64[h]')

difference = x2 - x1

difference.hist()

plt.plot(difference)
plt.show()

plt.axvline(x, y1, y2)

plt.plot(y1)
plt.plot(y2, color = 'red')
plt.plot(y)

######
#WITH RAMESH
#####

final_df['sent_time_x'] = pd.to_datetime(final_df['sent_time_x'])
final_df['sent_time_y'] = pd.to_datetime(final_df['sent_time_y'])

def get_minutes(row):
    return (row['sent_time_y'] - row['sent_time_x']).total_seconds()/60
    
final_df['sent_minutes'] = final_df.apply(get_minutes, axis=1)
final_df['sent_minutes'].hist()
final_df['sent_minutes'].hist(bins=50)
final_df.groupby('category_id').sent_minutes.mean()
final_df.groupby('category_id').sent_minutes.mean().sort_index('sent_minutes', ascending=False).plot(kind='barh')
final_df.groupby(['category_id']).sent_minutes.mean().sort_index(by='sent_minutes', ascending=False).plot(kind='barh')
final_df.groupby(['category_id']).sent_minutes.mean().order( ascending=False).plot(kind='barh')
final_df.groupby(['category_id']).sent_minutes.mean().order( ascending=False)[:10].plot(kind='barh')
final_df.boxplot('sent_minutes', by='category_id')
final_df[final_df['category_id'] == 95]
final_df[final_df['category_id'] ==111]
final_df[final_df['category_id'] == 111].boxplot('sent_minutes')
final_df.name_y.value_counts()
final_df.groupby(['category_id', 'name_y']).sent_minutes.mean()
final_df.groupby(['name_y', 'name_x']).sent_minutes.mean()
final_df.groupby(['name_y', 'name_x']).agg({'sent_minutes': np.mean})
final_df.groupby(['name_y', 'name_x']).agg({'sent_minutes': 'mean', 'name_x': 'count'})

from datetime import timedelta

times = ['00:02:20','00:4:40']

def average(times): 
  print(str(timedelta(seconds=sum(map(lambda f: int(f[0])*3600 + int(f[1])*60 + int(f[2]), map(lambda f: f.split(':'), times)))/len(times))))

average(times)

'''
fig = plt.figure()
ax = plt.subplot(111)
plt.plot(final_df.i_time2num,'o-')
plt.plot(final_df.q_time2num,'o-',label='E98')
plt.legend(loc=4)
plt.ylabel('Price (Euro/Litre)')
plt.xlabel('Year')
plt.grid()
plt.show()
'''

import statsmodels.api as sm
#Trying to plot the Quote Time and the Invite DateTime on the same Axis and see what the difference is
#Can I plot a histogram instead? What will give me the best insight to my data?
#Bar Graph?






'''
c = conn.cursor()
c.execute('SELECT SQLITE_VERSION()')
version = c.fetchone()

print "SQLite Version is %s" %version

c.execute('SELECT * from categories')
c.fetchone()

c.execute('SELECT * from invites')
c.fetchone()

c.execute('SELECT * from locations')
c.fetchone()

c.execute('SELECT * from quotes')
c.fetchone()

c.execute('SELECT * from requests')
c.fetchone()

c.execute('SELECT * from users')
c.fetchone()

users = pd.read_sql_table('requests', conn)
'''
