# -*- coding: utf-8 -*-
"""
Created on Sat Aug  8 14:08:10 2015

@author: karunsiddana
"""

# read diabetes_data_ncsu data into a DataFrame
import pandas as pd
import numpy as np
diabetes_ncsu_data = pd.read_csv('diabetes_ncsu_raw.csv')
diabetes_ncsu_data.columns

#AGE SEX BMI BP S1 S2 S3 S4 S5 S6 Y
#age sex bmi map tc ldl hdl tch ltg glu y
#Index([u'chol', u'stab.glu', u'hdl', u'ratio', u'glyhb', u'location', 
#u'age', u'gender', u'height', u'weight', u'frame', u'bp.1s', u'bp.1d', 
#u'bp.2s', u'bp.2d', u'waist', u'hip', u'time.ppn'], dtype='object')

diabetes_ncsu_data.drop(['Y'], axis=1, inplace=True)
diabetes_ncsu_data.rename(columns={'AGE':'age', 'SEX': 'gender','S1':'chol', 'S2':'ldl','S3':'hdl'}, inplace=True)


import seaborn as sns
import matplotlib.pyplot as plt
# scatter matrix in Seaborn
plt.scatter(diabetes_ncsu_data.hdl, diabetes_ncsu_data.chol, c='red')
sns.pairplot(diabetes_ncsu_data)
sns.heatmap(diabetes_ncsu_data.corr())                           

#-----------------------------------
# K-Means Clustering Modeling 
#-----------------------------------
from sklearn.cluster import KMeans
#diabetes_ncsu_data.drop(['bp.2s', 'bp.2d', 'location', 'bp.1s', 'bp.1d', 'gender'], axis=1, inplace=True)
#diabetes_ncsu_data.drop(['gender'], axis=1, inplace=True)
#diabetes_ncsu_data.drop(['frame'], axis=1, inplace=True)
#diabetes_ncsu_data.dropna(inplace=True)


columns = ['BMI', 'BP', 'chol', 'ldl', 'hdl', 'S4', 'S5', 'S6']
columns = ['chol', 'ldl', 'hdl', 'S4', 'S5', 'S6']
subsetted_data = diabetes_ncsu_data[columns]
#from sklearn.preprocessing import StandardScaler
#scale = StandardScaler()

#diabetes_scaled = pd.DataFrame(scale.fit_transform(subsetted_data.values), columns = columns)

knn = KMeans(n_clusters=4, init='random')
#knn.fit(diabetes_data)
knn.fit(subsetted_data)
y_kmeans = knn.predict(subsetted_data) #red,yellow,green
subsetted_data['cluster'] = y_kmeans


knn.cluster_centers_

#The colors Array has to be equal to the amount of Clusters K=4, 4 Colors
colors = np.array(['#FF0054','#FBD039','#23C2BC','#4023C2']) #red,yellow,green
#colors = np.array(['#FF0054','#FBD039', '#23C2BC']) #red,yellow,green
plt.figure()
plt.scatter(subsetted_data['chol'], subsetted_data['S4'], c=colors[y_kmeans], s=50)


from pandas.tools.plotting import parallel_coordinates
colors=('#FF0054', '#FBD039', '#23C2BC', '#4023C2' )
parallel_coordinates(data=subsetted_data, class_column='cluster', 
                     colors=('#FF0054', '#FBD039', '#23C2BC', '#4023C2' ))

#Plot y_kmeans data correlation
colors = np.array(['#FF0054','#FBD039','#23C2BC','#4023C2']) #red,yellow,green
diabetes_ncsu_data['y_kmeans'] = colors[y_kmeans]
sns.pairplot(diabetes_ncsu_data.dropna(), hue='y_kmeans')


#---------------------------
# Decision Tree Classifier
#---------------------------


#The Response needs to be a bunch of 1's and O's then delete result and glyhb
response = diabetes_ncsu_data['y_kmeans']
del diabetes_ncsu_data['y_kmeans']

from sklearn.cross_validation import train_test_split
# Now, split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(diabetes_ncsu_data, response, random_state=1)

from sklearn import tree
# Create a decision tree classifier instance (start out with a small tree for interpretability)
ctree = tree.DecisionTreeClassifier(random_state=1, max_depth=3)

# Fit the decision tree classifier
ctree.fit(X_train, y_train)

# Create a feature vector
features = X_train.columns.tolist()

features

# How to interpret the diagram?
ctree.classes_

# Which features are the most important?
ctree.feature_importances_

# Clean up the output
pd.DataFrame(zip(features, ctree.feature_importances_)).sort_index(by=1, ascending=False)

# Make predictions on the test set
preds = ctree.predict(X_test)

from sklearn import metrics

# Calculate accuracy
metrics.accuracy_score(y_test, preds)

'''

FINE-TUNING THE TREE

'''
from sklearn.grid_search import GridSearchCV

from sklearn.cross_validation import cross_val_score
# check CV score for max depth = 3
ctree = tree.DecisionTreeClassifier(max_depth=5)
np.mean(cross_val_score(ctree, diabetes_ncsu_data, response, cv=5, scoring='roc_auc'))

# check CV score for max depth = 10
ctree = tree.DecisionTreeClassifier(max_depth=10)
np.mean(cross_val_score(ctree, diabetes_ncsu_data, response, cv=10, scoring='roc_auc'))



# Conduct a grid search for the best tree depth
ctree = tree.DecisionTreeClassifier(random_state=1)
depth_range = range(1, 15)
param_grid = dict(max_depth=depth_range)
grid = GridSearchCV(ctree, param_grid, cv=5, scoring='roc_auc')
grid.fit(diabetes_ncsu_data, response)


# Check out the scores of the grid search
grid_mean_scores = [result[1] for result in grid.grid_scores_]

import matplotlib.pyplot as plt
# Plot the results of the grid search
plt.figure()
plt.plot(depth_range, grid_mean_scores)
plt.hold(True)
plt.grid(True)
plt.plot(grid.best_params_['max_depth'], grid.best_score_, 'ro', markersize=12, markeredgewidth=1.5,
         markerfacecolor='None', markeredgecolor='r')
         
         
         
         