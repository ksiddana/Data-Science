
# coding: utf-8

# In[3]:

# read diabetes_vanderbilt.csv data into a DataFrame
import pandas as pd
import numpy as np
diabetes_data = pd.read_csv('diabetes_vanderbilt.csv', index_col=0)


# In[4]:

diabetes_data.head()


# In[5]:

diabetes_data.columns


# In[6]:

diabetes_data.gender.value_counts()


# In[7]:

import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().magic(u'matplotlib inline')
# scatter matrix in Seaborn
sns.lmplot(x='hdl', y='glyhb', data=diabetes_data, ci=None)


# In[8]:

sns.lmplot(x='stab.glu', y='glyhb', data=diabetes_data, ci=None)


# In[9]:

#Kernel desnsity plots from 
#http://web.stanford.edu/~mwaskom/software/seaborn/tutorial/distributions.html
sns.jointplot(x="glyhb", y="stab.glu", data=diabetes_data, kind="kde")


# In[10]:

features = ['chol', 'stab.glu', 'hdl']
x = diabetes_data[features]
x.head(5)


# In[11]:

y = diabetes_data['glyhb']
y.head(5)


# In[18]:

#find the missing data from the predictors and fill them out with the average mean of cholestrol level
import pandas as pd
import numpy as np
x.chol.isnull()
x.chol.fillna(x.chol.mean(), inplace=True)
x.chol.head()


# In[19]:

#find the missing data from the predictors and fill them out with the 40 for the value of a good hdl cholestrol level.
x.hdl.isnull()
x.hdl.fillna(40, inplace=True)


# In[20]:

#the output is NaN for 13 datapoints, therefore we need to fill in the data
y.isnull().value_counts()


# In[21]:

#fill the missing data with the mean of 5.5 which is the average glyhb for this set of data
y.fillna(5.5, inplace=True)


# In[22]:

import seaborn as sns
import matplotlib.pyplot as plt
sns.pairplot(diabetes_data, x_vars=['chol','stab.glu','hdl'], y_vars=['glyhb'], size=4.5, aspect=0.7, kind='reg')


# In[23]:

y_category = np.where(y > 7, 1, 0)


# In[24]:

# cross-validation train_test_split
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y_category, test_size=.33, random_state=1)


# ##Logistic Regeression Model

# In[25]:

from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
zip(features, logreg.coef_[0])


# In[26]:

#look at the coefficients to get the equation for the line
print logreg.intercept_
print logreg.coef_


# * The negative value of Total cholestrol against Glycosolated Hemoglobin shows that there is a very negligible correlation between the predictor and the response.
# * Similarly High cholestrol level also has a negative value indicating the relationship between the two is negligible and thus not a good indicator for the predicting the value of Glycosolated Hemoglobin
# * However as you can see there is positive correlation between Blood Glucose Level and Glycosolated Hemoglobin

# In[27]:

# make predictions on testing set and calculate accuracy
y_pred_class = logreg.predict(X_test)
from sklearn import metrics
print metrics.accuracy_score(y_test, y_pred_class)


# #### 92.48% accuracy from a Logistic regression

# ##Linear Regeression Model

# In[28]:

X_train, X_test, y_train, y_test = train_test_split(x, y,test_size=.33, random_state=1)


# In[29]:

from sklearn.linear_model import LinearRegression
linreg = LinearRegression()
linreg.fit(X_train, y_train)


# In[30]:

# look at the coefficients to get the equation for the line
print linreg.intercept_
print linreg.coef_


# In[31]:

#predict the values of an Out-of Sample test Data based on X_test
y_pred = linreg.predict(X_test)


# In[32]:

# MAE is the same as before
# Calculate the error between the predicted values of y_pred and y_test
print metrics.mean_absolute_error(y_test, y_pred)


# In[33]:

# RMSE is larger than before
# Calculate the error between the predicted values of y_pred and y_test
print np.sqrt(metrics.mean_squared_error(y_test, y_pred))


# In[34]:

from sklearn.cross_validation import cross_val_score
scores = cross_val_score(linreg, x, y, cv=5)
print scores


# In[ ]:



