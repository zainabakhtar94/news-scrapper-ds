# import requests
# from bs4 import BeautifulSoup

# url='https://www.pakistantoday.com.pk/category/national/'
# response = requests.get(url)

# soup = BeautifulSoup(response.text, 'html.parser')
# headlines = soup.find('body').find_all('h3')
# for x in headlines:
#     print(x.text.strip())

import requests
import re
from bs4 import BeautifulSoup

def my_tag_selector(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "h2" and tag.has_attr("class") and "entry-title" in tag.get("class")

def my_content(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "div" and tag.has_attr("class") and "entry-content" in tag.get("class")

def getTime(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "time" and tag.has_attr("class") and "entry-time" in tag.get("class")

def getLink(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "a" and tag.has_attr("class") and "entry-title-link" in tag.get("class")


for page in range(1,200):
    url = "https://dailytimes.com.pk/pakistan/page/{page}/".format(page =page)
    print("************* url ****************")
    print(url)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    headlines=soup.find_all(my_tag_selector)
    # links_with_text = [a['href'] for a in soup.find_all(my_tag_selector, href=True) if a.text]
    headlines2=soup.find_all(my_content)
    links=soup.find_all(getLink)
    time=soup.find_all(getTime)
    
   
    # for i in range(0, len(headlines)):
        # print(headlines[i].text.strip())
        # print(time[i].text.strip())
        # print(i)
        
    links_with_text = []
    for a in headlines: 
        if a.text:
            a = a.find('a', href = re.compile(r'[/]([a-z]|[A-Z])\w+')).attrs['href']
            print(a)
           
    
    print("************* new Page **************** ")   
        # print(x.text.strip())
    # lists = soup.select("div#simulacion_tabla ul")

#     # for lis in lists:
#     #     title = lis.find('li', class_="col1").text
#     #     location = lis.find('li', class_="col2").text
#     #     province = lis.find('li', class_="col3").text
#     #     info = [title, location, province]
#     #     print(info)