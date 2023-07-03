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
import csv
from itertools import chain

import pymongo

myclient = pymongo.MongoClient("mongodb://root:password@localhost:27017/")
mydb = myclient["news"]
# dailyTimesCollection = mydb["dailytimes"]
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


def getArticle(tag):
	# We only accept "a" tags with a titlelink class
	return tag.name == "div" and tag.has_attr("class") and "entry-content" in tag.get("class")

    
    
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

def parseArticle(html):
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
    rows = []
    print(f"finished scraping in: {time() - _start:.1f} seconds")
    for i in range(0, len(titles)):
        hash_object = hashlib.sha256(str(i).encode('utf-8')+titles[i].text.strip().encode('utf-8'))
        hex_dig = hash_object.hexdigest()
        # print(hex_dig)
        # print(titles[i].text.strip())
        link = titles[i].find('a', href = re.compile(r'[/]([a-z]|[A-Z])\w+')).attrs['href']
        # print(content[i].text.strip())
        timestamp = str(moment.date(date[i].text.strip(), "%m-%d-%Y"))
        print(link)
        # print(timestamp)
        
        # Get full article
        articleHtml = await fetch(session, link, throttler)
        articlesParagraphs = parseArticle(articleHtml)
        print(articlesParagraphs)
        for p in range(0, len(articlesParagraphs)):
             paragraph = articlesParagraphs[p].text.strip()
             print(paragraph)
        
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
        
        # Build data to write in csv
        row = [titles[i].text.strip(),""]
        if i == 0 and page == 101:
            rows.append(["title","subject"])
        rows.append(row)
        # print(titles[i].text.strip())
    # Write data in csv 
    new_list = list(chain.from_iterable(rows))
    
    with open('oldnews1.csv', 'a', encoding='UTF8', newline='') as f:
        
        writer = csv.writer(f)
        writer.writerows(rows)
    return new_list

async def scrape_urls(): 
    throttler = AsyncLimiter(max_rate=2, time_period=2) 
    async with aiohttp.ClientSession() as session:
        data = await asyncio.gather(
            *(fetch_and_parse(session, page, throttler=throttler) for page in range(1, 3))
        )
        # with open('oldnews.csv', 'a', encoding='UTF8', newline='') as f:
        #     header = ['title', 'class']
            
        #     writer = csv.writer(f)
        #     writer.writerow(header)
        #     writer.writerows(data)
        # print(data)
        return data

if __name__ == "__main__":
    asyncio.run(scrape_urls())
