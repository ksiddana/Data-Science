# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 21:29:55 2015

@author: karunsiddana
"""

'''
CLASS: Model evaluation procedures
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#import the glass data and add the row with the header with all the attributes to the file.
glass_data = pd.read_csv('glass.data')

#see glass data
glass_data
#See all the glass data with Sodium
glass_data.Na
glass_data.columns #print all the columns from the file
glass_data.info()

#Plot all the data to see what kind of charts we are able to get
#
#   !!!! Don't recommend however plotting this data
glass_data.plot(kind='bar')

#replace column 11 name with the actual type of glass
glass_data.groupby('glass_type').mean()
glass_type_name = { 1:'building_windows_float_processed', \
2:'building_windows_non_float_processed', \
3:'vehicle_windows_float_processed', 4:'vehicle_windows_non_float_processed', \
5:'containers', 6:'tableware', 7:'headlamps'}

glass_type_name[6]

def define_window_type(glass_type_index):
    glass_type_name = { 1:'building_windows_float_processed', \
    2:'building_windows_non_float_processed', \
    3:'vehicle_windows_float_processed', 4:'vehicle_windows_non_float_processed', \
    5:'containers', 6:'tableware', 7:'headlamps'}
    
    if glass_type_index == 1:
        return glass_type_name[1]
    elif glass_type_index == 2:
        return glass_type_name[2]
    elif glass_type_index == 3:
        return glass_type_name[3]
    elif glass_type_index == 4:
        return glass_type_name[4]
    elif glass_type_index == 5:
        return glass_type_name[5]
    elif glass_type_index == 6:
        return glass_type_name[6]    
    else:
        return glass_type_name[7]

#Store a New Column glass_type_name in the dataFrame and map the define window
#type to each glass_type row using the map function. Since the glass_type is of type
#DataFrame.Series doing a simple for loop does not work on the Column.
glass_data['glass_type_name'] = glass_data.glass_type.map(define_window_type) 

#for i in glass_data:    
#    if glass_data['glass_type_name'] == 1:
#        glass_data['glass_type_name'] = glass_type_name[1]
#    elif glass_data.glass_type == 2:
#        glass_data['glass_type_name'] = glass_type_name[2]
#    elif glass_data.glass_type == 3:
#        glass_data['glass_type_name'] = glass_type_name[3]
#    elif glass_data.glass_type == 4:
#        glass_data['glass_type_name'] = glass_type_name[4]
#    elif glass_data.glass_type == 5:
#        glass_data['glass_type_name'] = glass_type_name[5]
#    elif glass_data.glass_type == 6:
#        glass_data['glass_type_name'] = glass_type_name[6]
#    else:
#        glass_data['glass_type_name'] = glass_type_name[7]

#Check the Column glass_type_name and use Value Counts compare this with the Column 11
#The counts should be the same
glass_data.glass_type_name.value_counts()
glass_data.glass_type.value_counts()
glass_data[['glass_type', 'Na']].plot()
glass_data.groupby('glass_type_name').mean()

#Lets try to make sense of the data, lets plot some data now
#Explore the data visually
glass_data['glass_type'].groupby('glass_type').plot()
glass_data[['glass_type_name', 'Na']].groupby('glass_type_name').hist(sharex=True)
glass_data[['glass_type_name', 'Na']].groupby('glass_type_name').mean().plot(kind='bar')
glass_data[['glass_type_name', 'Al']].groupby('glass_type_name').mean().plot(kind='bar')
glass_data[['glass_type_name', 'Iron']].groupby('glass_type_name').mean().plot(kind='bar')
glass_data[['glass_type_name', 'RI']].groupby('glass_type_name').mean().plot(kind='bar')

glass_data[glass_data.glass_type == 6].plot()
glass_data.Na.plot() #see a line graph of Sodium for the datapoints

#def color_glass_type(glass_type_index):
#    if glass_type_index == 1:
#        return 

#plotting the Sodium against Refractive Index
glass_data.plot(x='Al', y='RI', kind='scatter')


#Visual Plot of Barium is only seen in Headlamps
glass_data.boxplot(column='Ba', by='glass_type_name', rot=90)

#Visual plot of Calcium shows that tableware and containers have a high Calcium 
#content of glass
glass_data.boxplot(column='Ca', by='glass_type_name', rot=90)

#Plotting Box plots of Iron shows that Iron is primarily found in building and vehicle
#windows
glass_data.boxplot(column='Iron', by='glass_type_name', rot=90)
glass_data.drop('Id', axis = 1).boxplot(by='glass_type_name', figsize = (10,10))

##Perform KNN algorithms
# read in the glass data


X, y = glass_data.drop(['Id', 'RI', 'Iron', 'Si', 'glass_type_name', 'glass_type'],  axis = 1), glass_data['glass_type']
#X, y = glass_data.drop('Id','glass_type', 'glass_type_name', axis = 1), glass_data['glass_type_name']

X.shape
y.shape

X.head()

# predict y with KNN
from sklearn.neighbors import KNeighborsClassifier  # import class

knn = KNeighborsClassifier(n_neighbors=1)           # instantiate the estimator

knn.fit(X, y)                                       # fit with data

knn.predict([3, 1.25, 0.5, 7, 0, 0])   # predict for a new observation
knn.score(X, y)         #Oh look !!! we get a 100% because we trained it and scored on
#same training set.


## CROSS-VALIDATION

# check CV score for K=1
from sklearn.cross_validation import cross_val_score
knn = KNeighborsClassifier(n_neighbors=5)
scores = cross_val_score(knn, X, y, cv=5, scoring='accuracy')

scores              # It ran a KNN 5 times!
# We are looking at the accuracy for each of the 5 splits

# search for an optimal value of K
k_range = range(1, 30, 2)
scores = []
for k in k_range:
    knn = KNeighborsClassifier(n_neighbors=k)
    scores.append(np.mean(cross_val_score(knn, X, y, cv=5, scoring='accuracy')))
scores

np.mean(scores)     # Average them together

# plot the K values (x-axis) versus the 5-fold CV score (y-axis)
#As you can see your data is terrible and you only have 60% accuracy, so lets try to 
#rid of the data that is not relevant, it doesn't distinguish the glass properly.
plt.figure()
plt.plot(k_range, scores)



        