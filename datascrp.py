from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import pandas as pd
import numpy as np
import url1

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url1 = url1.url

df = pd.DataFrame()
print('Data Scrapping Initiated for Bollywood Actresses.....')

for p in range(len(url1)) :

	url = url1[p]
	print('Retriving data from URL:', url,'.......')
	html = urlopen(url, timeout = 10, context=ctx).read()
	soup = BeautifulSoup(html, "html.parser")
	dic= {}
	temp_disc = {}
	k = list()
	val = list()

	gdp_table = soup.find_all("table", attrs={"style": "width:100%; background: linear-gradient(to right, #ffffff 10%,#F2FFF1 20%,#F2FFF1 20%,#ffffff 100%);"})[0]
	gdp_table_data = gdp_table.find_all("tr")
	for td in gdp_table_data[0].find_all("td")[1] :
	    val.append(td)
	for td in gdp_table_data[0].find_all("td")[0] :
		k.append(td)
	for td in gdp_table_data[1].find_all("td")[0] :
		k.append(td)
	for td in gdp_table_data[1].find_all("td")[1].find("span1") :
		val.append(td.strip())
	temp_dic = dict(zip(k, val))
	dic.update(temp_dic)
	temp_dic.clear()
	val.clear()
	k.clear()

	gdp_table = soup.find_all("table", attrs={"id" : "customers"})[0]
	gdp_table_data = gdp_table.find_all("tr")
	for i in range(6) :
		for td in gdp_table_data[i].find("td", attrs={"style": "font-size: larger;"}) :
			val.append(td)
	for i in range(6) :
		for td in gdp_table_data[i].find("td", attrs={"width": "25%"}) :
			k.append(td)
	temp_dic = dict(zip(k, val))
	dic.update(temp_dic)
	temp_dic.clear()
	k.clear()
	val.clear()

	gdp_table = soup.find_all("table", attrs={"id": "customers"})[1]
	gdp_table_data = gdp_table.find_all("tr")
	for i in range(13) :
		for td in gdp_table_data[i].find("td", attrs={"style": "font-size: larger;"}) :
			val.append(td)
	for i in range(13) :
		for td in gdp_table_data[i].find("td", attrs={"width": "25%"}) :
			k.append(td)
	temp_dic = dict(zip(k, val))
	dic.update(temp_dic)
	temp_dic.clear()
	k.clear()
	val.clear()

	gdp_table = soup.find_all("table", attrs={"id" : "customers"})[2]
	gdp_table_data = gdp_table.find_all("tr")
	for i in range(3) :
		for td in gdp_table_data[i].find("td", attrs={"style": "font-size: larger;"}) :
			val.append(td)
	for i in range(3) :
		for td in gdp_table_data[i].find("td", attrs={"width": "25%"}) :
			k.append(td)
	temp_dic = dict(zip(k, val))
	dic.update(temp_dic)
	temp_dic.clear()
	k.clear()
	val.clear()

	#print(dic)
	if p == 0 :
		df = pd.DataFrame([dic])
	else :
		df = df.append(dic, ignore_index = True)
	print("Iteration number completed:",p + 1)

df.to_csv('data_actresses.csv', index = False)
print('Data Retriving task completed for Bollywood Actresses')
