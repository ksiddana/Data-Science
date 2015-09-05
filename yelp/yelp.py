# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 17:26:46 2015

@author: karunsiddana
"""

##### Part 1 #####

import pandas as pd
import numpy as np

# 1. read in the yelp dataset
#yelp = pd.read_csv('hw/optional/yelp.csv', index_col=0)
yelp = pd.read_csv('hw/data/yelp.csv')

# 2. Perform a linear regression using 
# "stars" as your response and 
# "cool", "useful", and "funny" as predictors

features = ['cool', 'useful', 'funny']
x = yelp[features]
y = yelp['stars']

#Exploring the data. Lets do some plots.
import seaborn as sns
import matplotlib.pyplot as plt
# scatter matrix in Seaborn
plt.scatter(yelp.cool, yelp.stars, c='red')
sns.lmplot(x='cool', y='stars', data=yelp, ci=None)
sns.lmplot(x='useful', y='stars', data=yelp, ci=None)

# scatter matrix in Pandas
#pd.scatter_matrix(yelp, figsize=(12, 10))

# display correlation matrix in Seaborn using a heatmap
sns.heatmap(yelp.corr())


# cross-validation train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y, random_state=1)

from sklearn.linear_model import LinearRegression
linreg = LinearRegression()
linreg.fit(X_train, y_train)
zip(features, linreg.coef_)

##-------------------------------------------
##Output
#[('cool', 0.2634577531380673),
# ('useful', -0.14715363152044192),
# ('funny', -0.12829260460713657)]
##-------------------------------------------

# look at the coefficients to get the equation for the line, but then how do you plot the line?
print linreg.intercept_
print linreg.coef_

#predict the values of an Out-of Sample test Data based on X_test
y_pred = linreg.predict(X_test)

# 3. Show your MAE, R_Squared and RMSE
# Calculate the error between the predicted values of y_pred and y_test
from sklearn import metrics
# **Mean Squared Error** (MSE)
print metrics.mean_squared_error(y_test, y_pred)
##Output = 1.40254405522
##-------------------------------------------

# MAE (Mean Absolute Error)
print metrics.mean_absolute_error(y_test, y_pred)
##Output = 0.947100490773
##-------------------------------------------

# RMSE (Root Mean Square Error)
# Calculate the error between the predicted values of y_pred and y_test
print np.sqrt(metrics.mean_squared_error(y_test, y_pred))
##Output = 0.044272856242001724
##-------------------------------------------

# 4. Use statsmodels to show your pvalues
# for each of the three predictors
# Using a .05 confidence level, 
# Should we eliminate any of the three?
import statsmodels.formula.api as smf
# create a fitted model
lm = smf.ols(formula='stars ~ cool + useful + funny', data=yelp).fit()
lm.rsquared
##Output = 0.044272856242001724
##-------------------------------------------

y_pred = linreg.predict(x)
metrics.r2_score(y, y_pred)
##Output = 0.044131682236617564
##-------------------------------------------

# print the p-values for the model coefficients
lm.pvalues
##-------------------------------------------
# based on the low p_values we should not elimate any of the variables as 
# they are related to rating of the business
#Intercept    0.000000e+00
#cool         2.988197e-90
#useful       1.206207e-39
#funny        1.850674e-43
##-------------------------------------------

# 5. Create a new column called "good_rating"
# this could column should be True iff stars is 4 or 5
# and False iff stars is below 4

yelp['good_rating'] = (yelp.stars == 4) | (yelp.stars == 5)
#yelp['good_rating'] = (yelp['stars']==4)|(yelp['stars']==5)

yelp['good_rating'].value_counts() 
#True     6863
#False    3137

#Validate the good ratings value
yelp['stars'].value_counts()
#for line in yelp['stars']:
#    line = (line == 4 or line == 5)
#    print line


# 6. Perform a Logistic Regression using 
# "good_rating" as your response and the same
# three predictors

features = ['cool', 'useful', 'funny']
x = yelp[features]
y = yelp['good_rating']

# cross-validation train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y, random_state=1)

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])

##-------------------------------------------
##Output
#[('cool', 0.61047114002981107),
# ('useful', -0.21385426531353399),
# ('funny', -0.36367174499327759)]
##-------------------------------------------

#look at the coefficients to get the equation for the line
print logreg.intercept_
print logreg.coef_

# 7. Show your Accuracy, Sensitivity, Specificity
# and Confusion Matrix

# make predictions on testing set and calculate accuracy
y_pred_class = logreg.predict(X_test)
from sklearn import metrics
print metrics.accuracy_score(y_test, y_pred_class)

##Output = 0.6916
##-------------------------------------------

from sklearn import metrics
preds = logreg.predict(X_test)
matrix = metrics.confusion_matrix(y_test, preds)
print matrix

##Output
##[[  51  733]
## [  38 1678]]
##-------------------------------------------

sensitivity = float(matrix[1][1]) / (matrix[1][0] + matrix[1][1])
specificity = float(matrix[0][0]) / (matrix[0][1] + matrix[0][0])
accuracy = (float(matrix[0][0]) + matrix[1][1]) / ((matrix[1][0] + matrix[1][1])+(matrix[0][1] + matrix[0][0]))

print sensitivity # Output = 0.9778
print specificity # Output = 0.0650
print accuracy    # Output = 0.6916

# 8. Perform one NEW operation of your 
# choosing to try to boost your metrics!

#plt.plot(y_test, y_pred_class)
#sns.pairplot(yelp)
#sns.heatmap(yelp.corr())
#sns.lmplot(x='cool', y='stars', data=yelp, ci=None)
#sns.lmplot(x='useful', y='stars', data=yelp, ci=None)
#sns.pairplot(yelp, x_vars=['cool', 'useful', 'funny' ], y_vars=['stars'], size=4.5, aspect=0.7, kind='reg')
#plt.plot(yelp.cool, yelp.stars, 'r+')
#plt.scatter(yelp.cool, yelp.stars, c='g')

# sort DataFrame by cool
#yelp.sort('cool', inplace=True)
#assorted_pred_class = logreg.predict_proba(x)[:, 0]
#plt.scatter(yelp.cool, assorted_pred_class)
#plt.plot(yelp.cool, assorted_pred_class, color='red')


yelp['good_rating'].value_counts() 
yelp['good_rating'] = (yelp.text.str.contains("good" or "nice")) | (yelp.stars > 3)
yelp['good_rating'].value_counts()

features = ['cool', 'useful', 'funny',]
x = yelp[features]
y = yelp['good_rating']

# cross-validation train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.40, random_state=1)

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])

##-------------------------------------------
##Output
#[('cool', 0.54274233809217531),
# ('useful', -0.14854209225164489),
# ('funny', -0.30645205083707844)]
##-------------------------------------------

# make predictions on testing set and calculate accuracy
y_pred_class = logreg.predict(X_test)
from sklearn import metrics
print metrics.accuracy_score(y_test, y_pred_class)

from sklearn import metrics
preds = logreg.predict(X_test)
matrix = metrics.confusion_matrix(y_test, preds)
print matrix

sensitivity = float(matrix[1][1]) / (matrix[1][0] + matrix[1][1])
specificity = float(matrix[0][0]) / (matrix[0][1] + matrix[0][0])
accuracy = (float(matrix[0][0]) + matrix[1][1]) / ((matrix[1][0] + matrix[1][1])+(matrix[0][1] + matrix[0][0]))

print sensitivity # Output = 0.9981
print specificity # Output = 0.0097
print accuracy    # Output = 0.821




##### Part 2 ######

# 1. Read in the titanic data set.

titanic_data = pd.read_csv('hw/data/titanic.csv', index_col=0)
titanic_data.columns

# 4. Create a new column called "wife" that is True
# if the name of the person contains Mrs.
# AND their SibSp is at least 1

titanic_data['wife'] = (titanic_data.Name.str.contains("Mrs.")) & (titanic_data.SibSp >= 1)

# 5. What is the average age of a male and
# the average age of a female on board?
avg_male_age = titanic_data[(titanic_data.Sex == "male")]['Age'].mean()

# 5. Fill in missing MALE age values with the
# average age of the remaining MALE ages
titanic_data.Age[titanic_data.Sex == 'male'].fillna(avg_male_age, inplace=True)

# 6. Fill in missing FEMALE age values with the
# average age of the remaining FEMALE ages
avg_female_age = titanic_data[(titanic_data.Sex == "female")]['Age'].mean()
titanic_data.Age[titanic_data.Sex == 'female'].fillna(avg_female_age, inplace=True)

#####****Error*******
titanic_data.Age.fillna(avg_female_age, inplace=True)

# 7. Perform a Logistic Regression using
# Survived as your response and age, wife
# as predictors
features = ["Age", "wife"]
x = titanic_data[features]
y = titanic_data['Survived']

# understanding train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y,test_size=.33, random_state=1)

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])

##-------------------------------------------
##Output
#[('Age', -0.018195662965542814), 
# ('wife', 2.1859439178635998)]
##-------------------------------------------

# 8. Show Accuracy, Sensitivity, Specificity and 
# Confusion matrix

from sklearn import metrics
preds = logreg.predict(X_test)
matrix = metrics.confusion_matrix(y_test, preds)
print matrix

##-------------------------------------------
##Output
#[[165   9]
# [104  17]]
##-------------------------------------------

sensitivity = float(matrix[1][1]) / (matrix[1][0] + matrix[1][1])
specificity = float(matrix[0][0]) / (matrix[0][1] + matrix[0][0])
accuracy = (float(matrix[0][0]) + matrix[1][1]) / ((matrix[1][0] + matrix[1][1])+(matrix[0][1] + matrix[0][0]))

print sensitivity # Output = 0.140495867769
print specificity # Output = 0.948275862069
print accuracy    # Output = 0.616949152542

# 9. now use ANY of your variables as predictors
# Still using survived as a response to boost metrics!

features = ['Age', 'wife', 'Pclass', 'Parch']
x = titanic_data[features]
y = titanic_data['Survived']

# understanding train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y,test_size=.33, random_state=1)

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])

##-------------------------------------------
##Output
#[('Age', -0.033199147637000624),
# ('wife', 1.9355083684220087),
# ('Pclass', -0.95067051800796609),
# ('Parch', 0.20596509348624045)]
##-------------------------------------------


# 10. Show Accuracy, Sensitivity, Specificity

from sklearn import metrics
preds = logreg.predict(X_test)
matrix = metrics.confusion_matrix(y_test, preds)
print matrix

##-------------------------------------------
##Output
#[[155  19]
# [ 66  55]]
##-------------------------------------------

sensitivity = float(matrix[1][1]) / (matrix[1][0] + matrix[1][1])
specificity = float(matrix[0][0]) / (matrix[0][1] + matrix[0][0])
accuracy = (float(matrix[0][0]) + matrix[1][1]) / ((matrix[1][0] + matrix[1][1])+(matrix[0][1] + matrix[0][0]))

print sensitivity # Output = 0.454
print specificity # Output = 0.885
print accuracy    # Output = 0.711

# REMEMBER TO USE
# TRAIN TEST SPLIT AND CROSS VALIDATION
# FOR ALL METRIC EVALUATION!!!!

