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
dailyTimesCollection = mydb["dailytimes"]


def getContent(tag):
    # We only accept "a" tags with a titlelink class
    return tag.name == "div" and tag.has_attr("class") and "entry-content" in tag.get("class")


async def fetch(session, url, throttler):
    async with throttler:
        async with session.get(url) as response:
            return await response.text()


def parseContent(html):
    soup = BeautifulSoup(html, 'html.parser')
    body = soup.find_all(getContent)
    return body


async def fetch_and_parse(session, data, throttler):
    _start = time()
    hashCode = data['hash']
    url = data['link']
    html = await fetch(session, url, throttler)
    content = parseContent(html)

    print(f"finished scraping in: {time() - _start:.1f} seconds")
    for i in range(0, len(content)):
        print(url)
        # print("*************************************************")
        # print("*************************************************")
        # print("*************************************************")

        fullArticteContent = re.sub(r"\n", "", content[i].text.strip())
        # print(fullArticteContent)

        articleObject = {
            "article": fullArticteContent,
            "timestamp_article": datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        }

        key = {'hash': hashCode}
        data = {"$set": articleObject}
        result = dailyTimesCollection.update_one(key, data, upsert=True);
        print(result.modified_count, " ...documents modified.")

    await asyncio.sleep(0.001)
    return content


async def scrape_urls():
    throttler = AsyncLimiter(max_rate=10, time_period=20)
    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        return await asyncio.gather(
            *(fetch_and_parse(session, data, throttler=throttler) for data in dailyTimesCollection.find())
        )


if __name__ == "__main__":
    asyncio.run(scrape_urls())
    
    # x = dailyTimesCollection.find()
    # for data in x:
    #     print("******************")
    #     # print(data['article'])
    #     print(data['title'])
