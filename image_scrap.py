import requests
import shutil
import URL


urls = URL.url_a
p=0

for url in urls :

	image_url = url
	resp = requests.get(image_url, stream = True)
	hello = 'image' + str(p) + '.jpg'
	p = p + 1
	local_file = open(hello, 'wb')

	resp.raw.decode_content = True
	shutil.copyfileobj(resp.raw, local_file)
	del resp
	print('Actors Pictures Scrapping',p,'out of 24 completed')

print('Actors Pictures Scrapped')

urls = URL.url_b

for url in urls :

	image_url = url
	resp = requests.get(image_url,stream = True)
	hello = 'image' + str(p) + '.jpg'
	p = p + 1
	local_file = open(hello, 'wb')

	resp.raw.decode_content = True
	shutil.copyfileobj(resp.raw, local_file)
	del resp
	print('Actresses Pictures Scrapping', p-24,'out of 25 completed')

print('Actresses Pictures Scrapped')
print('Images Scrapping Task Completed')


# This is the image url.
#image_url = "https://www.dev2qa.com/demo/images/green_button.jpg"
'''image_url= "https://i.bollywoodmantra.com/albums/candids/actresses/disha-patani/disha-patani__1071473.jpg"
# Open the url image, set stream to True, this will return the stream content.
resp = requests.get(image_url, stream=True)

hello = 'Disha Patani.jpg'
# Open a local file with wb ( write binary ) permission.
local_file = open(hello, 'wb')

# Set decode_content value to True, otherwise the downloaded image file's size will be zero.
resp.raw.decode_content = True

# Copy the response stream raw data to local image file.
shutil.copyfileobj(resp.raw, local_file)

# Remove the image url response object.
del resp
#pic_url='https://i.bollywoodmantra.com/albums/candids/actresses/sara-ali-khan/sara-ali-khan__1072148.jpg'''
