import snscrape.modules.twitter as sntwitter
import csv
maxTweets = 2000

# keyword = 'MinusOneNaManzoor'
#place = '5e02a0f0d91c76d2' #This geo_place string corresponds to İstanbul, Turkey on twitter.

#keyword = 'covid'
#place = '01fbe706f872cb32' This geo_place string corresponds to Washington DC on twitter.

#Open/create a file to append data to
csvFile = open('illust_tweet.csv', 'a', newline='', encoding='utf8')

#Use csv writer
csvWriter = csv.writer(csvFile)
csvWriter.writerow(['id','date','tweet',]) 
# for i,tweet in enumerate(sntwitter.TwitterSearchScraper('children books illustration + since:2022-08-25 until:2022-10-05 -filter:links -filter:replies').get_items()):


for i,tweet in enumerate(sntwitter.TwitterSearchScraper('children books illustration + since:2022-08-25 until:2022-10-05 -filter:replies').get_items()):
        if i > maxTweets :
            break
        print(tweet.user)  
        # csvWriter.writerow([tweet.id, tweet.date, tweet.content])
csvFile.close()