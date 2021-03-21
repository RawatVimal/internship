import pandas as pd
from pandas import DataFrame

import requests

top_db_count = 10   # user can change the count to get that many top databases

#===========================getting database names ======================================

df_list = pd.read_html('https://db-engines.com/en/ranking')  # this parses all the tables in webpages to a list

df_list = df_list[3]

df_list = df_list.iloc[3:top_db_count+3,3]

df=df_list.to_frame()

df.columns =['Database Names']

df.reset_index(drop=True, inplace=True)

df['Database Names'] = df['Database Names'].str.replace('Detailed vendor-provided information available', '')

df = df.T
df.columns = df.iloc[0]
df = df[1:]
df.insert(loc=0, column='Features', value='')
df.columns = df.columns.str.strip()


#================================== getting feature names ================================


df_list_features_names = pd.read_html('https://db-engines.com/en/system/Oracle')

df_list_features_names = df_list_features_names[3]

df_list_features_names= df_list_features_names.iloc[2:35,0]

df_list_features_names=df_list_features_names.to_frame()

df_list_features_names.columns =['Features']
df_list_features_names.reset_index(drop=True, inplace=True)

df['Features'] = df_list_features_names['Features']


#======================================== Saving features of each database ======================


count = 1

while count != top_db_count+1:
    db_name = df.columns[count]
    db_name = db_name.replace(" ", "+")
    df_list_features_values = pd.read_html('https://db-engines.com/en/system/'+db_name)
    df_list_features_values = df_list_features_values[3]
    df_list_features_values = df_list_features_values.iloc[2:35, 1]
    df_list_features_values = df_list_features_values.to_frame()
    df_list_features_values.columns = [df.columns[count]]
    df_list_features_values.reset_index(drop=True, inplace=True)
    df[df.columns[count]] = df_list_features_values[df.columns[count]]
    count = count + 1


df.to_csv('databaseFeatures.csv')

