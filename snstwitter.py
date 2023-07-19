import snscrape.modules.twitter as sntwitter
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

df = pd.read_csv(r'D:\Shuai-Jingbo-Pei-yu\Twiter Birdwatch\TweetId_w_AllNote_20230107.csv')
def sntwitter_request(start,thread_index):
    for i in range(start,len(df_company),10):
        user_name = df_company.iloc[i,1]
        gvkey = df_company.iloc[i,2]
        query = "(from:"+ user_name+ ") since:2008-01-01"
        tweets = []
        limit = float('inf')
        # limit = 10

        index = 0
        txt_file_path = r'D:\Shuai-Jingbo-Pei-yu\company twitter information\data\company_tweets_snscrape_txt'+'\\'+str(gvkey)+'_tweet.txt'
        open(txt_file_path,'w')
        for tweet in sntwitter.TwitterSearchScraper(query).get_items():
            index = index +1
            with open(r'D:\Shuai-Jingbo-Pei-yu\company twitter information\data\threading'+'\\'+str(thread_index)+'.txt','w') as file:
                file.write('thread: '+ str(thread_index) +' company: '+str(i) +' '+ str(index))
            # print(vars(tweet))
            # break
            if len(tweets) == limit:
                break
            else:
                tweets.append([tweet.url,tweet.date,tweet.content,tweet.renderedContent,tweet.id,tweet.replyCount,tweet.retweetCount,tweet.likeCount,tweet.quoteCount,tweet.conversationId,tweet.lang,
                tweet.source,tweet.sourceUrl,tweet.sourceLabel,tweet.outlinks,tweet.tcooutlinks,tweet.media,tweet.retweetedTweet,tweet.quotedTweet,tweet.inReplyToTweetId,
                tweet.inReplyToUser,tweet.mentionedUsers,tweet.coordinates,tweet.place,tweet.hashtags,tweet.cashtags,tweet.user.username,tweet.user.id,tweet.user.displayname, tweet.user.description,
                tweet.user.descriptionUrls, tweet.user.created, tweet.user.followersCount, tweet.user.friendsCount, tweet.user.statusesCount, tweet.user.favouritesCount, tweet.user.listedCount, 
                tweet.user.mediaCount, tweet.user.location, tweet.user.protected, tweet.user.linkUrl, tweet.user.profileImageUrl, tweet.user.linkTcourl,tweet.user.profileBannerUrl,tweet.user.label])


        str_columns = "tweet.url,tweet.date,tweet.content,tweet.renderedContent,tweet.id,tweet.replyCount,tweet.retweetCount,tweet.likeCount,tweet.quoteCount,tweet.conversationId,tweet.lang,tweet.source,tweet.sourceUrl,tweet.sourceLabel,tweet.outlinks,tweet.tcooutlinks,tweet.media,tweet.retweetedTweet,tweet.quotedTweet,tweet.inReplyToTweetId,tweet.inReplyToUser,tweet.mentionedUsers,tweet.coordinates,tweet.place,tweet.hashtags,tweet.cashtags,tweet.user.username,tweet.user.id,tweet.user.displayname, tweet.user.description,tweet.user.descriptionUrls, tweet.user.created, tweet.user.followersCount, tweet.user.friendsCount, tweet.user.statusesCount, tweet.user.favouritesCount, tweet.user.listedCount, tweet.user.mediaCount, tweet.user.location, tweet.user.protected, tweet.user.linkUrl, tweet.user.profileImageUrl, tweet.user.linkTcourl,tweet.user.profileBannerUrl,tweet.user.label"        

        df = pd.DataFrame(tweets, columns=str_columns.split(','))
        df.to_csv(r'D:\Shuai-Jingbo-Pei-yu\company twitter information\data\company_tweets_snscrape_csv_2' + '\\' + str(gvkey)+'_tweet.csv',index=False)