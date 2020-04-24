import pandas as pd
import numpy as np
import re
import datascrp
import datascrp_1
import image_scrap
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

cnx = sqlite3.connect('Information.db')
df2.to_sql(name = 'DATA', con = cnx, if_exists = 'replace', index = False)

#Now dealing with the images
#Images will be written in a separate table but in the same database named info.db

cursor = cnx.cursor()
cursor.execute("DROP TABLE IF EXISTS IMAGES")

sql ='''CREATE TABLE IMAGES(
   NAME TEXT NOT NULL,
   IMAGE BLOB NOT NULL
)'''
cursor.execute(sql)
print('Image Table Creation Started')
cnx.commit()
cnx.close()

def convertToBinaryData(filename) :
	with open(filename, 'rb') as file :
		blobData = file.read()
	return blobData

def insertBLOB(name, photo) :
	conn = sqlite3.connect('Information.db')
	cursor = conn.cursor()
	sql_blob = """ INSERT INTO IMAGES (NAME, IMAGE) VALUES (?, ?)"""
	pic = convertToBinaryData(photo)

	data_tuple = (name, pic)
	cursor.execute(sql_blob, data_tuple)
	conn.commit()
	cursor.close()
	conn.close()

# Now using above functions to insert images into the database

s1 = df2['Name']
names = s1.tolist()
print('Inserting the Scrapped Images into the Database')

for i in range(len(names)) :
	n = names[i]
	path_of_image = 'image' + str(i) + '.jpg'
	insertBLOB(n, path_of_image)

print('Created Database')
print('Database name is information and consists two tables')
print('1. DATA')
print('2. IMAGES')
print('TASK COMPLETED')
