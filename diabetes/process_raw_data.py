# -*- coding: utf-8 -*-
"""
Created on Sat Aug  8 16:42:56 2015

@author: karunsiddana

References:
http://www.cdc.gov/diabetes/pubs/pdf/diabetesreportcard.pdf
http://www4.stat.ncsu.edu/~boos/var.select/diabetes.html
http://cs-people.bu.edu/dgs/courses/cs105/hall_of_fame/awm.html
http://www.cdc.gov/diabetes/pubs/pdf/diabetesreportcard.pdf
"""

import pandas as pd
diabetes_data = pd.read_csv('diabetes_vanderbilt_raw.csv', index_col=0)
diabetes_data.columns
#Index([u'chol', u'stab.glu', u'hdl', u'ratio', u'glyhb', u'location', 
#u'age', u'gender', u'height', u'weight', u'frame', u'bp.1s', u'bp.1d', 
#u'bp.2s', u'bp.2d', u'waist', u'hip', u'time.ppn'], dtype='object')

diabetes_data.isnull().sum()
diabetes_data[diabetes_data.glyhb.isnull()]
diabetes_data[diabetes_data.weight.isnull()]
diabetes_data[(diabetes_data.glyhb > 6) & (diabetes_data.glyhb < 10)]

#After analyzing the data I assumed the weight of ther person was 180 pounds using
#BMI calculator online
diabetes_data.weight.fillna(180, inplace=True)

#filling in the missing values for glyhb is not easy. First I used the data
#to come up with a equation that tightly correlated with the outcome and the predictors
# I found weight and stab.glu to be the most important factors
# Linear Regression Model Equation
def calculate_glyhb(x, z):
    return 0.028*x + 0.0022*z + 2.12676

diabetes_data.glyhb.fillna(calculate_glyhb(diabetes_data['stab.glu'],
                                           diabetes_data['weight']), inplace=True)

'''
Reference:
http://professional.diabetes.org/glucosecalculator.aspx
relationship between eAG and A1C:
28.7 X A1C – 46.7 = eAG

A1C	eAG	
%	mg/dl	mmol/l	
6	126	7.0	
6.5	140	7.8	
7	154	8.6	
7.5	169	9.4	
8	183	10.1	
8.5	197	10.9	
9	212	11.8	
9.5	226	12.6	
10	240	13.4

'''

diabetes_data.gender.value_counts()
#female    234
#male      169

diabetes_data.age.value_counts().sum()

diabetes_data.info()

#find the missing data from the datasets and try filling them out
diabetes_data.chol.isnull()
diabetes_data.chol.fillna(diabetes_data.chol.mean(), inplace=True)
diabetes_data.hdl.isnull()
diabetes_data.hdl.fillna(40, inplace=True)
diabetes_data.isnull().sum()
diabetes_data.glyhb.isnull()
diabetes_data.ratio.fillna(diabetes_data.ratio.mean(), inplace=True)
diabetes_data['bp.1s'].fillna(diabetes_data['bp.1s'].mean(), inplace=True)
diabetes_data['bp.1d'].fillna(diabetes_data['bp.1d'].mean(), inplace=True)
diabetes_data.waist.fillna(diabetes_data.waist.mean(), inplace=True)
diabetes_data.hip.fillna(diabetes_data.hip.mean(), inplace=True)
diabetes_data.height.fillna(diabetes_data.height.mean(), inplace=True)
diabetes_data['time.ppn'].fillna(diabetes_data['time.ppn'].mean(), inplace=True)

# Add a column for age groups
def age_groups(n):
    if ( n >= 19 and n < 45):
        return 1
    elif (n >= 45 and n <= 60):
        return 2
    elif (n > 60):
        return 3
    
diabetes_data['age_groups'] = diabetes_data.age.apply(age_groups)
diabetes_data.groupby('age_groups').age.describe()
diabetes_data.age_groups.isnull().sum()

diabetes_data[diabetes_data.age_groups.isnull()]

# Add a Column for Output based on the Reports. A Glycosolated Hemoglobin level
# of more than 6.5, results in the person at the risk of type 2 diabetes
def diabetes_output(n):
    if n > 6.5:
        return 1
    else:
        return 0

diabetes_data['result'] = diabetes_data.glyhb.apply(diabetes_output)  

diabetes_data['gender'] = diabetes_data.gender.map({"male":1, "female":0})  

#droppping irrelevant data
#diabetes_data.drop(['bp.2s', 'bp.2d', 'location', 'bp.1s', 'bp.1d'], axis=1, inplace=True)
diabetes_data.drop(['frame', 'location','bp.2s', 'bp.2d'], axis=1, inplace=True)

#generate the clean CSV file
diabetes_data.isnull().sum()  

diabetes_data.to_csv('diabetes_vanderbilt_clean.csv')


