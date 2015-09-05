# -*- coding: utf-8 -*-
"""
Created on Sun Jul  5 13:01:04 2015

@author: karunsiddana
"""

# read diabetes_data_vanderbilt.csv data into a DataFrame
import pandas as pd
import numpy as np
diabetes_data = pd.read_csv('diabetes_vanderbilt_clean.csv', index_col=0)
diabetes_data.columns
#Index([u'chol', u'stab.glu', u'hdl', u'ratio', u'glyhb', u'location', 
#u'age', u'gender', u'height', u'weight', u'frame', u'bp.1s', u'bp.1d', 
#u'bp.2s', u'bp.2d', u'waist', u'hip', u'time.ppn'], dtype='object')

diabetes_data.gender.value_counts()
#female    234
#male      169

diabetes_data.age.value_counts().sum()
diabetes_data.isnull().sum()


#------------------------------------------------
# Visualize Data using histograms
#------------------------------------------------

def age_groups(n):
    if ( n >= 19 and n < 45):
        return "20-44"
    elif (n >= 45 and n < 60):
        return "45-60"
    elif (n > 60):
        return "above 60"
    
diabetes_data['age_groups'] = diabetes_data.age.apply(age_groups)
diabetes_data.groupby('age_groups').age.describe()
diabetes_data.age_groups.isnull().sum()
#Output: 227 people don't have ages filled in

diabetes_data[['age_groups', 'age']]
diabetes_data.groupby('gender').age.hist(sharex=True)
diabetes_data.groupby('gender').age.count().plot(kind='bar')
diabetes_data.groupby('age_groups').age.count().plot(kind='bar')
diabetes_data.groupby('age_groups').age.mean().plot(kind='bar')
diabetes_data.groupby('age_groups').age.hist()
diabetes_data.info()


#----------------------------------------------------
# Visualize Data, Plot graphs, Heatmaps, Pairplots
#----------------------------------------------------

import seaborn as sns
import matplotlib.pyplot as plt
# scatter matrix in Seaborn
plt.figure()
plt.scatter(x=diabetes_data.hdl, y=diabetes_data.chol, c='red')
plt.xlabel('hdl')
plt.ylabel('chol')
sns.pairplot(diabetes_data)

del diabetes_data['time.ppn']
del diabetes_data['age_groups']
sns.heatmap(diabetes_data.corr())


sns.set()
sns.pairplot(diabetes_data, hue="result")

x = diabetes_data.weight
x = np.random.normal(size=100)
sns.distplot(x);

sns.lmplot(x='hdl', y='glyhb', data=diabetes_data, ci=None)
sns.lmplot(x='stab.glu', y='glyhb', data=diabetes_data, ci=None)
sns.lmplot(x='weight', y='glyhb', data=diabetes_data, ci=None)
sns.lmplot(x='waist', y='stab.glu', data=diabetes_data, ci=None)
sns.pairplot(diabetes_data, x_vars=['stab.glu','bp.1s','weight'], y_vars='glyhb', size=6, aspect=0.7, kind='reg', hue='result')

#Kernel desnsity plots from 
#http://web.stanford.edu/~mwaskom/software/seaborn/tutorial/distributions.html
sns.jointplot(x="weight", y="glyhb", data=diabetes_data, kind="kde")

#dropping the string columns temporarily before we use them to analyze data
'''
diabetes_data.drop(['gender','location', 'frame'],axis=1)
diabetes_data['gender'] = diabetes_data.gender.map({"male":1, "female":0})

diabetes_data.drop(['bp.2s', 'bp.2d', 'location', 'bp.1s', 'bp.1d'], axis=1, inplace=True)
diabetes_data.drop(['frame'], axis=1, inplace=True)



#find the missing data from the datasets and try filling them out

y.fillna(5.5, inplace=True)
'''

#-----------------------------------
# Logistic Regeression Model
#-----------------------------------
features = ['stab.glu', 'weight', 'bp.1s']
x = diabetes_data[features]
x
y = diabetes_data['glyhb']
y

y_category = np.where(y > 6.5, 1, 0)

# cross-validation train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y_category, random_state=1)

X_train.shape
X_test.shape
y_train.shape
y_test.shape

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])

#look at the coefficients to get the equation for the line, 
#but then how do you plot the line?
print logreg.intercept_
print logreg.coef_

#assorted_pred = logreg.predict(X_train)
#assorted_pred[:20]

# add predicted class to DataFrame
#y['assorted_pred_class'] = assorted_pred_class

# sort DataFrame by glyhb
#y.sort('glyhb', inplace=True)

# make predictions on testing set and calculate accuracy
y_pred_class = logreg.predict(X_test)
from sklearn import metrics
print metrics.accuracy_score(y_test, y_pred_class)

# scatter plot that includes the regression line
#plt.scatter(X_train, y_pred_class)

from sklearn import metrics
preds = logreg.predict(X_test)
matrix = metrics.confusion_matrix(y_test, preds)
print matrix

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()

# compare AUC using cross-validation
from sklearn.cross_validation import cross_val_score
cross_val_score(logreg, X_test, y_pred_class, cv=10, scoring='roc_auc').mean()

##Output
##[[  112  0]
## [  10  11]]
##-------------------------------------------

sensitivity = float(matrix[1][1]) / (matrix[1][0] + matrix[1][1])
specificity = float(matrix[0][0]) / (matrix[0][1] + matrix[0][0])
accuracy = (float(matrix[0][0]) + matrix[1][1]) / ((matrix[1][0] + matrix[1][1])+(matrix[0][1] + matrix[0][0]))

print sensitivity # Output = 0.52380952381
print specificity # Output = 1.0
print accuracy    # Output = 0.924812030075


#-----------------------------------
# Linear Regeression Model
#-----------------------------------

features = ['stab.glu', 'weight', 'bp.1s']
x = diabetes_data[features]
x
y = diabetes_data['glyhb']
y

X_train, X_test, y_train, y_test = train_test_split(x, y, random_state=1)


from sklearn.linear_model import LinearRegression
linreg = LinearRegression()
linreg.fit(X_train, y_train)

# look at the coefficients to get the equation for the line, but then how do you plot the line?
print linreg.intercept_
print linreg.coef_
zip(features, linreg.coef_)

#predict the values of an Out-of Sample test Data based on X_test
y_pred = linreg.predict(X_test)

# MAE is the same as before
# Calculate the error between the predicted values of y_pred and y_test
print metrics.mean_absolute_error(y_test, y_pred)

# RMSE is larger than before
# Calculate the error between the predicted values of y_pred and y_test
print np.sqrt(metrics.mean_squared_error(y_test, y_pred))

from sklearn.cross_validation import cross_val_score
scores = cross_val_score(linreg, X_test, y_test, cv=5, scoring='mean_squared_error')
print scores

#make predictions for all values of X, and then plot those predictions connected by a line
#plot the figures below together to get an understanding of the X_test data against y_pred
plt.scatter(X_test.weight, y_pred)
plt.scatter(X_test['bp.1s'], y_pred, color='red')
plt.scatter(X_test['stab.glu'], y_pred, color='green')
plt.ylabel('glyhb')
plt.xlabel('weight and stab.glu')
plt.legend()
#plt.plot(X_test, y_pred, color='red')

#------------------------------------------
# LassoLars Least Angle Regression Model
# http://scikit-learn.org/stable/auto_examples/linear_model/plot_lasso_model_selection.html#example-linear-model-plot-lasso-model-selection-py
#------------------------------------------

features = ['stab.glu', 'weight', 'age', 'bp.1s', 'waist', 'hip']
x = diabetes_data[features]
x
y = diabetes_data['glyhb']
y

X_train, X_test, y_train, y_test = train_test_split(x, y, random_state=1)
from sklearn.linear_model import LassoLarsCV
model = LassoLarsCV(cv=20).fit(X_train, y_train)
#t_lasso_lars_cv = time.time() - t1

# Display results
m_log_alphas = -np.log10(model.cv_alphas_)

plt.figure()
plt.plot(m_log_alphas, model.cv_mse_path_, ':')
plt.plot(m_log_alphas, model.cv_mse_path_.mean(axis=-1), 'k',
         label='Average across the folds', linewidth=2)
plt.axvline(-np.log10(model.alpha_), linestyle='--', color='k')
plt.ylabel('glyhb')
plt.xlabel('weight and stab.glu')
plt.legend()


#-----------------------------------
# Stats Linear Regeression Model
#-----------------------------------

diabetes_data['avg_glu'] = diabetes_data['stab.glu']
diabetes_data['bp_1s'] = diabetes_data['bp.1s']
diabetes_data['bp_1d'] = diabetes_data['bp.1d']

import statsmodels.formula.api as smf
# create a fitted model
lm = smf.ols(formula='glyhb ~ avg_glu + bp_1s + bp_1d + chol + weight + age + waist + hip + gender + hdl + ratio', data=diabetes_data).fit()
lm.rsquared
# Output = 
#-------------------------------------------

y_pred_stats = linreg.predict(x)
metrics.r2_score(y, y_pred_stats)
# Output = 
#-------------------------------------------

# print the p-values for the model coefficients
lm.pvalues
#--------------------------------------------
# based on the low p_values we should not elimate any of the variables as 
# they are related to rating of the business

#--------------------------------------------

# scatter matrix in Pandas
pd.scatter_matrix(x, figsize=(12, 10))


#-----------------------------------
# KNN Classification Model
#-----------------------------------
features = ['stab.glu', 'weight', 'gender', 'age']
#features = ['stab.glu']

x = diabetes_data[features]
x
y = np.where(diabetes_data.glyhb > 6.5, 1, 0)
y

from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y, random_state=1)

# steps 2 and 3: calculate test set error for K=1
from sklearn.neighbors import KNeighborsClassifier
knn = KNeighborsClassifier(n_neighbors=3)
knn.fit(X_train, y_train)      # Note that I fit to the training
scores = cross_val_score(knn, X_test, y_test, cv=5, scoring='accuracy')
print scores

# automatic grid search for an optimal value of K
from sklearn.grid_search import GridSearchCV
knn = KNeighborsClassifier()
k_range = range(1, 10, 1)
param_grid = dict(n_neighbors=k_range)
grid = GridSearchCV(knn, param_grid, cv=5, scoring='accuracy')
grid.fit(X_train, y_train)

# check the results of the grid search
grid.grid_scores_
grid_mean_scores = [result[1] for result in grid.grid_scores_]

# plot the results
plt.figure()
plt.plot(k_range, grid_mean_scores)

grid.best_score_     # shows us the best score
grid.best_params_    # shows us the optimal parameters
grid.best_estimator_ # this is the actual model


#-----------------------------------
# K-Means Clustering Modeling 
#-----------------------------------
from sklearn.cluster import KMeans
#diabetes_data.drop(['bp.2s', 'bp.2d', 'location', 'bp.1s', 'bp.1d', 'gender'], axis=1, inplace=True)
#diabetes_data.drop(['gender'], axis=1, inplace=True)
#diabetes_data.drop(['frame'], axis=1, inplace=True)
#diabetes_data.dropna(inplace=True)


columns = ['weight', 'stab.glu', 'bp.1s', 'glyhb', 'waist', 'hip']
subsetted_data = diabetes_data[columns][diabetes_data.age_groups == 1]
#from sklearn.preprocessing import StandardScaler
#scale = StandardScaler()

#diabetes_scaled = pd.DataFrame(scale.fit_transform(subsetted_data.values), columns = columns)

knn = KMeans(n_clusters=3, init='random')
#knn.fit(diabetes_data)
knn.fit(subsetted_data)
y_kmeans = knn.predict(subsetted_data) #red,yellow,green
subsetted_data['cluster'] = y_kmeans


knn.cluster_centers_

#The colors Array has to be equal to the amount of Clusters K=4, 4 Colors
colors = np.array(['#FF0054','#FBD039','#23C2BC','#4023C2']) #red,yellow,green
#colors = np.array(['#FF0054','#FBD039', '#23C2BC']) #red,yellow,green
plt.figure()
plt.scatter(subsetted_data['weight'], subsetted_data['glyhb'], c=colors[y_kmeans], s=50)
plt.ylabel('glyhb')
plt.xlabel('weight')
#plt.scatter(subsetted_data[:,0],subsetted_data[:,1],c=y_kmeans, s=50)
plt.legend()
plt.show()


from pandas.tools.plotting import parallel_coordinates
colors=('#FF0054', '#FBD039', '#23C2BC', '#4023C2' )
parallel_coordinates(data=subsetted_data, class_column='cluster', 
                     colors=('#FF0054', '#FBD039', '#23C2BC', '#4023C2' ))

#Plot y_kmeans data correlation
colors = np.array(['#FF0054','#FBD039','#23C2BC','#4023C2']) #red,yellow,green
diabetes_data['y_kmeans'] = colors[y_kmeans]
sns.pairplot(diabetes_data.dropna(), hue='y_kmeans')


#-------------------------------------------------------------
# K-Means Clustering Modeling with the Data being Normalized
#-------------------------------------------------------------

from sklearn.cluster import KMeans
columns = ['weight', 'stab.glu', 'glyhb', 'chol', 'bp.1s', 'bp.1d']
subsetted_data = diabetes_data[columns]

from sklearn.preprocessing import StandardScaler
scale = StandardScaler()

diabetes_scaled = pd.DataFrame(scale.fit_transform(subsetted_data.values), columns = columns)

knn = KMeans(n_clusters=3, init='random')
#knn.fit(diabetes_data)
knn.fit(diabetes_scaled)
y_kmeans = knn.predict(diabetes_scaled) #red,yellow,green
subsetted_data['cluster'] = y_kmeans


knn.cluster_centers_

#The colors Array has to be equal to the amount of Clusters K=4, 4 Colors
colors = np.array(['#FF0054','#FBD039','#23C2BC','#4023C2']) #red,yellow,green
#   colors = np.array(['#FF0054','#FBD039','#23C2BC']) #red,yellow,green
plt.figure()
plt.scatter(diabetes_scaled['glyhb'], diabetes_scaled['weight'], c=colors[y_kmeans], s=50)
plt.legend()

plt.hist(diabetes_scaled[:, 0])

plt.hist(diabetes_data.chol.values)


#---------------------------
# Decision Tree Classifier
#---------------------------

#The Response needs to be a bunch of 1's and O's then delete result and glyhb
response = diabetes_data['result']
del diabetes_data['result']
del diabetes_data['glyhb']
del diabetes_data['age_groups']
del diabetes_data['time.ppn']

from sklearn.cross_validation import train_test_split
# Now, split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(diabetes_data, response, random_state=1)

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
np.mean(cross_val_score(ctree, diabetes_data, response, cv=5, scoring='roc_auc'))

# check CV score for max depth = 10
ctree = tree.DecisionTreeClassifier(max_depth=10)
np.mean(cross_val_score(ctree, diabetes_data, response, cv=10, scoring='roc_auc'))



# Conduct a grid search for the best tree depth
ctree = tree.DecisionTreeClassifier(random_state=1)
depth_range = range(1, 15)
param_grid = dict(max_depth=depth_range)
grid = GridSearchCV(ctree, param_grid, cv=5, scoring='roc_auc')
grid.fit(diabetes_data, response)


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

#---------------------------
# Support Vector Matrices
#---------------------------         

features = ['stab.glu', 'weight', 'gender', 'age']
#features = ['stab.glu']

x = diabetes_data[features]
x
y = np.where(diabetes_data.glyhb > 6.5, 1, 0)
y

from sklearn.cross_validation import train_test_split
# Now, split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(diabetes_data, y, random_state=1)


from sklearn import svm

# Let's try a SVM
clf = svm.SVC()
clf.fit(X_train,y_train)

cross_val_score(clf, X_test, y_test, cv=5, scoring='accuracy').mean()


