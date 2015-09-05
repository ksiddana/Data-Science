# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 20:34:48 2015

@author: karunsiddana
"""

import pandas as pd
import numpy as np

titanic_data = pd.read_csv('titanic.csv', index_col=0)
titanic_data.columns
features = ["Pclass", "Parch"]
x = titanic_data[features]
y = titanic_data['Survived']

titanic_data.Pclass.value_counts()

import seaborn as sns
import matplotlib.pyplot as plt

# scatter plot using Matplotlib
plt.scatter(x.Parch, y)
plt.plot(x.Age, y, color='red')
sns.lmplot(x='Age', y='Survived', data=titanic_data, ci=None)

# scatter matrix in Seaborn
sns.pairplot(titanic_data)

## TEST SET APPROACH

# understanding train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y,test_size=.33, random_state=1)

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])

#look at the coefficients to get the equation for the line, 
#but then how do you plot the line?
print logreg.intercept_
print logreg.coef_


assorted_pred = logreg.predict(X_train)
assorted_pred[:20]

# transform predictions to 1 or 0
assorted_pred_class = np.where(assorted_pred >= 0.5, 1, 0)
assorted_pred_class

# scatter plot that includes the regression line
plt.scatter(X_train, y_train)
plt.plot(X_train, assorted_pred_class, color='red')

# TASK 5: make predictions on testing set and calculate accuracy
y_pred_class = logreg.predict(X_test)
from sklearn import metrics
print metrics.accuracy_score(y_test, y_pred_class)

# TASK 6 (BONUS): add Age as a feature and calculate testing accuracy
titanic_data.Age.isnull().value_counts()
titanic_data.Age.fillna(titanic_data.Age.mean(), inplace=True)
features = ['Pclass', 'Parch', 'Age']

X = titanic_data[features]
# scatter plot using Matplotlib
plt.scatter(X.Age, y)
plt.plot(X.Age, y, color='red')
sns.lmplot(x='Age', y='Survived', data=titanic_data, ci=None)


X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])
y_pred_class = logreg.predict(X_test)
print metrics.accuracy_score(y_test, y_pred_class)

