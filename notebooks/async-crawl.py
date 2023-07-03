import asyncio
from aiolimiter import AsyncLimiter
from time import time
from httpx import AsyncClient
from bs4 import BeautifulSoup
import aiohttp
import re
import hashlib
import moment
from datetime import datetime

import pymongo

myclient = pymongo.MongoClient("mongodb://root:password@localhost:27017/")
mydb = myclient["news"]
dailyTimesCollection = mydb["dailytimes_new"]

def getTitle(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "h2" and tag.has_attr("class") and "entry-title" in tag.get("class")

def getContent(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "div" and tag.has_attr("class") and "entry-content" in tag.get("class")

def getTime(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "time" and tag.has_attr("class") and "entry-time" in tag.get("class")

    
    
async def fetch(session, url, throttler):
    async with throttler:
        async with session.get(url) as response:
            return await response.text()

def parseTitle(html):
    soup = BeautifulSoup(html,'html.parser')
    body = soup.find_all(getTitle)
    return body

def parseContent(html):
    soup = BeautifulSoup(html,'html.parser')
    body = soup.find_all(getContent)
    return body

def parseDate(html):
    soup = BeautifulSoup(html,'html.parser')
    body = soup.find_all(getTime)
    return body

async def fetch_and_parse(session, page, throttler):
    _start = time()
    url = "https://dailytimes.com.pk/pakistan/page/{page}/".format(page =page)
    html = await fetch(session, url, throttler)
    titles = parseTitle(html)
    content = parseContent(html)
    date = parseDate(html)
    print(page, " ...page number.")
    print(f"finished scraping in: {time() - _start:.1f} seconds")
    for i in range(0, len(titles)):
        hash_object = hashlib.sha256(str(i).encode('utf-8')+titles[i].text.strip().encode('utf-8'))
        hex_dig = hash_object.hexdigest()
        # print(hex_dig)
        # print(titles[i].text.strip())
        link = titles[i].find('a', href = re.compile(r'[/]([a-z]|[A-Z])\w+')).attrs['href']
        # print(content[i].text.strip())
        timestamp = str(moment.date(date[i].text.strip(), "%m-%d-%Y"))
        # print(timestamp)
        newsObject = {
            "hash" : hex_dig,
            "title" : titles[i].text.strip(),
            "link": link,
            "content" : content[i].text.strip(),
            "timestamp" : timestamp
        }
        # print(newsObject)
        key = {'hash': hex_dig}
        # data = {'key2':'value2', 'key3':'value3'};
        data = {"$set": newsObject}
        result = dailyTimesCollection.update_one(key, data, upsert=True);
        
        print(result.modified_count, " ...documents modified.")
        # dailyTimesCollection.insert_one(newsObject);
    await asyncio.sleep(0.005)   
    return titles

async def scrape_urls(): 
    throttler = AsyncLimiter(max_rate=2, time_period=10) 
    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(connector = connector) as session:
        return await asyncio.gather(
            *(fetch_and_parse(session, page, throttler=throttler) for page in range(100, 400))
        )

if __name__ == "__main__":
    asyncio.run(scrape_urls())
