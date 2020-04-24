import pandas as pd
import numpy as np
import re
import datascrp
import datascrp_1
import sqlite3

print('Filtering of Data Started....')
df = pd.read_csv('data_actresses.csv')

df = df[['Famous Name: ','Date of Birth: ','Nickname','Gender','Birth Place','Hometown','Height','Weight','Eyes Color','Hair Color','Body Shape',
'School','College','University']]
df.rename(columns={'Famous Name: ' : 'Name',
	               'Date of Birth: ' : 'Date_of_Birth',
	               'Birth Place' : 'Birth_Place',
	               'Eyes Color' : 'Eyes_Colour',
	               'Hair Color' : 'Hair_Colour',
	               'Body Shape' : 'Body_Shape'}, inplace = True)

df['temp'] = df['Height'].str.extract(r'\((\d?\d.\d?\d)').apply(pd.to_numeric)
df.at[23,'temp'] = 165
df['temp1'] = np.where(df['temp'] > 2, df['temp'], df['temp'] * 100)
df = df.drop(['temp','Height'], axis = 1)
df.rename(columns={'temp1' : 'Height_in_cm'}, inplace = True)

#Now cleaning column of weight
df['temp'] = df['Weight'].str.extract(r'(..)').apply(pd.to_numeric)
df = df.drop(['Weight'], axis = 1)
df.rename(columns = {'temp' : 'Weight_in_kg'}, inplace = True)

df1 = df
print('....\n....\n....')

#Now cleaning data for actor's

df = pd.read_csv('data_actors.csv')
df = df[['Famous Name: ','Date of Birth: ','Nickname','Gender','Birth Place','Hometown','Height','Weight','Eyes Color','Hair Color','Body Shape',
'School','College','University']]
df.rename(columns={'Famous Name: ' : 'Name',
	               'Date of Birth: ' : 'Date_of_Birth',
	               'Birth Place' : 'Birth_Place',
	               'Eyes Color' : 'Eyes_Colour',
	               'Hair Color' : 'Hair_Colour',
	               'Body Shape' : 'Body_Shape'}, inplace = True)

df['temp'] = df['Height'].str.extract(r'\((\d?\d.\d?\d)').apply(pd.to_numeric)
df.at[4,'temp'] = 1.83
df['temp1'] = np.where(df['temp'] > 2, df['temp'], df['temp'] * 100)
df = df.drop(['temp','Height'], axis = 1)
df.rename(columns={'temp1' : 'Height_in_cm'}, inplace = True)

#Now cleaning column of weight
df['temp'] = df['Weight'].str.extract(r'(..)').apply(pd.to_numeric)
df = df.drop(['Weight'], axis = 1)
df.rename(columns = {'temp' : 'Weight_in_kg'}, inplace = True)

df2 = df.append(df1, ignore_index = True)
df2.to_csv('mix.csv')

print('Filtering of Data Done')

cnx = sqlite3.connect('info.db')
df2.to_sql(name = 'info', con = cnx, if_exists = 'replace', index = False)
print('Created Database')
print('TASK COMPLETED')
