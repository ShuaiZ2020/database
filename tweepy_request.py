import pandas as pd
import tweepy
import json
from tqdm import tqdm
import logging
import sys



def get_token_dict(index):
    with open(r'D:\Shuai-Jingbo-Pei-yu\Twitter User Location\token.json') as file:
        api_dict = json.load(file)
        index = 'api'+str(index)
        return api_dict[index]
def authentication(token,api_type):
    if api_type==1:
        if 'Consumer Key' in token:
            consumer_key = token['Consumer Key']
            consumer_key_secret = token['Consumer Secret']
            access_token = token['Access Token']
            access_token_secret = token['Access Token Secret']
            auth = tweepy.OAuthHandler(consumer_key, consumer_key_secret)
            auth.set_access_token(access_token, access_token_secret)
        else:
            auth = tweepy.OAuth2BearerHandler(token['Bearer Token'])
        
        api = tweepy.API(auth)
        return api
    if api_type == 2:
        client = tweepy.Client(token['Bearer Token'],wait_on_rate_limit=True)
        return client


def request(twitter_id_list,start,end, api_index,output_file):
    logging.basicConfig(filename="twitter request "+str(api_index) +".log",
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a',
                    level=logging.INFO)


    file_path = output_file
    tweet_field = "attachments, author_id, context_annotations, conversation_id, created_at, edit_controls, entities, geo, id, in_reply_to_user_id, lang, public_metrics, possibly_sensitive, referenced_tweets, reply_settings, source, text, withheld"
    # for i in range(len(df)):
    client = authentication(get_token_dict(api_index),2)
    tweet_ids = twitter_id_list
    pd.DataFrame(columns=tweet_field.split(', ')).to_csv(file_path,index=False)
    for j in tqdm(range(start,end,100)):
        
        while_index = 0
        while True:
            while_index = while_index+1
            if while_index > 5:
                logging.error('api ' +str(api_index)+' | ' + str(j) + ' tweets requeste fail')
                break
            try:
                # print(i)
                tweet = client.get_tweets(tweet_ids[j:j+100],tweet_fields = tweet_field.split(', '))
                for i in range(len(tweet.data)):
                    
                        
                    pd.DataFrame([[tweet.data[i].attachments, tweet.data[i].author_id, tweet.data[i].context_annotations, tweet.data[i].conversation_id, tweet.data[i].created_at, tweet.data[i].edit_controls, tweet.data[i].entities, 
                    tweet.data[i].geo, tweet.data[i].id, tweet.data[i].in_reply_to_user_id, tweet.data[i].lang, tweet.data[i].public_metrics, tweet.data[i].possibly_sensitive, tweet.data[i].referenced_tweets, tweet.data[i].reply_settings, 
                    tweet.data[i].source, tweet.data[i].text, tweet.data[i].withheld]]).to_csv(file_path, mode='a',index=False,header=False)
                    
                logging.info('api ' +str(api_index)+' | ' + str(j) + ' tweets has requested')        
                break
            except Exception as e1:
                
                continue
def single_tread():
    input_file_path = sys.argv[1]
    start_index = int(sys.argv[2])
    end_index = int(sys.argv[3])
    twitter_api_index = sys.argv[4]
    output_file_name = sys.argv[5]
    df = pd.read_csv(input_file_path)
    twitter_id_list = df['tweet_id'].to_list()
    if (len(sys.argv)!=6):
        print('bad arguments')
    else:   
        print('Amount of twitter id is '+str(len(twitter_id_list)))
        # tweet = request(twitter_id_list,29900,7)
        tweet = request(twitter_id_list,int(start_index),end_index,twitter_api_index,output_file_name)

single_tread