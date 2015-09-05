# -*- coding: utf-8 -*-
"""
Created on Sat Aug 29 20:39:03 2015

@author: karunsiddana
"""

import sqlite3
from sqlalchemy import create_engine
import pandas as pd

conn = sqlite3.connect('consumer_database.sqlite')

disk_engine = create_engine('sqlite:///consumer_database.sqlite')
df_requests = pd.read_sql_query('SELECT * FROM requests', disk_engine)
df_quotes = pd.read_sql_query('SELECT * FROM quotes', disk_engine)
df_invites = pd.read_sql_query('SELECT * FROM invites', disk_engine)
df_locations = pd.read_sql_query('SELECT * FROM locations', disk_engine)
df_categories = pd.read_sql_query('SELECT * FROM categories', disk_engine)
df_users = pd.read_sql_query('SELECT * FROM users', disk_engine)

#Try seeing some samples and how the dataframes are related
df_quotes[df_quotes.invite_id == 4]
df_invites[df_invites.invite_id == 4]
df_requests[df_requests.request_id == 1]
df_locations[df_locations.location_id == 35]
df_categories[df_categories.category_id == 46]

#Now lets see find some trends in Photography
df_requests[df_requests.category_id == 1]
df_invites[df_invites.invite_id == 4]

##------------------------------------------------------------------
#Combine the tables using the Merge function in Pandas
##------------------------------------------------------------------     

#Merge dataframes using inner join on the Location dataframe and the request_dataframe
frame = pd.DataFrame.merge(df_requests, df_locations, left_on='location_id', right_on='location_id')

#MErge Categories and Location Dataframe into one dataframe using category_id as the key
frame1 = pd.DataFrame.merge(frame, df_categories, left_on='category_id', right_on='category_id')

#Merge Invite Dataframe with the Request Dataframe using the request_id as the key do an inner join
frame2 = pd.DataFrame.merge(frame1, df_invites, left_on='request_id', right_on='request_id')

#Merge Invite Dataframe with the Request Dataframe using the request_id as the key do an inner join
frame3 = pd.DataFrame.merge(frame2, df_users, left_on='user_id_y', right_on='user_id')

#Merge Quotes dataframe to the Final Dataframe including Requests, Invites and time 
#using the invite_id as the Key for the inner join.
final_df = pd.DataFrame.merge(frame3, df_quotes, left_on='invite_id', right_on='invite_id')

#Rename the columns since the inner join, created names with variables name_x, name_y, sent_time_x, sent_time_y
final_df.rename(columns={'sent_time_x':'sent_time_invite', 
                         'sent_time_y':'sent_time_quote',
                         'name_x':'location_name', 
                         'name_y':'category_name',
                         'user_id_x':'customer_id'}, inplace=True)

##-----------------------------------------------------------------------------------
#Convert the time the Request was created by the User and entered into the database
#final_df['creation_time_test'] = pd.to_datetime(final_df.creation_time, "%Y-%M-%D")
##-----------------------------------------------------------------------------------            

#Sort the data by creation_time
final_df.sort_index(axis=0, by='creation_time').head()

#convert the invite sent_time & quote sent_time to datetime format
final_df['sent_time_invite'] = pd.to_datetime(final_df['sent_time_invite'])
final_df['sent_time_quote'] = pd.to_datetime(final_df['sent_time_quote'])

#define a function get_minutes to convert the hours and minutes into total minutes
#All Plot are determined in minutes
def get_minutes(row):
    return (row['sent_time_quote'] - row['sent_time_invite']).total_seconds()/60
  
#Apply the get_minutes function to all the rows of the dataframe which we will use to plot the data
final_df['time_taken'] = final_df.apply(get_minutes, axis=1)

#----------------------------------------------------------------------
# Now we can start visualizing and start making some sense of the data
# Let us interpret the dataset
#----------------------------------------------------------------------

#Lets plot some basic statistics of the data
final_df.time_taken.describe()

#lets see the distribution of the data
final_df['time_taken'].hist()
final_df['time_taken'].hist(bins=50)

#Group by category_id and plot the mean
final_df.groupby('category_id').time_taken.mean()
final_df.groupby('category_id').time_taken.mean().plot()

#Group by location and Category and plot the time taken to Invite to Quote rate
final_df.groupby(['location_name']).time_taken.mean().order(ascending=False)[:10].plot(kind='barh')
final_df.groupby(['location_name']).time_taken.mean().order(ascending=True)[:10].plot(kind='barh')
final_df.groupby(['category_name']).time_taken.mean().order(ascending=False)[:10].plot(kind='barh')
final_df.groupby(['category_name']).time_taken.mean().order(ascending=True)[:10].plot(kind='barh')

#Box plots are useful in seeing the mean and seeing how far we are from the mean
#In this case we plot box plots for Guitar Teaching (34), Math Tutoring 
df_categories[df_categories.name == 'Guitar Teaching']
final_df[final_df['category_id'] == 34]
final_df[final_df['category_id'] == 34].boxplot('time_taken')

df_categories[df_categories.name == 'Math Tutoring']
final_df[final_df['category_id'] == 95]
final_df[final_df['category_id'] == 95].boxplot('time_taken')

final_df[final_df['category_id'] == 95]
final_df[final_df['category_id'] == 111]
final_df[final_df['category_id'] == 111].boxplot('time_taken')

final_df.groupby('location_name').boxplot('time_taken')

#This shows us the number of quotes per categories, popular demands made by customers.
final_df.category_name.value_counts()
final_df.groupby(['category_name', 'location_name']).time_taken.mean()

final_df.groupby('category_name').time_taken.mean()[:10].plot(kind='bar')
final_df.groupby('category_name').time_taken.mean().plot(kind='hist')

final_df.groupby(['category_name', 'location_name']).time_taken.mean()[:10].plot(kind='bar')
final_df.groupby(['category_name', 'location_name']).agg({'time_taken': 'mean'})
final_df.groupby(['category_name', 'location_name']).agg({'time_taken': 'mean', 'location_name': 'count'}).plot(kind='barh')

