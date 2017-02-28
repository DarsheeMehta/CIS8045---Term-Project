#Importing all required libraries for text analytics
import pymongo
import nltk
from pymongo import MongoClient
import pandas as pd
import csv
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.util import ngrams

#connecting to mongoDB for data
con=MongoClient()
airlineDB=con.Airline
reviews=airlineDB.reviews
reviewData=pd.DataFrame(list(reviews.find()))
posResult=pd.DataFrame(list(reviews.find({'recommended':1})))
negResult=pd.DataFrame(list(reviews.find({'recommended':0})))

#Sampling done here is optional and can be removed if needed
posSample=posResult[:5000]
negSample=negResult[:5000]
stemmer = SnowballStemmer("english")

def getWordList(text, word_proc = lambda x:x):
    '''
    this function takes each review and breaks them into words and then 
    further refines these list of words by removing stopwords,applying stemming 
    and creating ngrams
    '''
    word_list=[]
    for sent in sent_tokenize(text):
        for word in word_tokenize(sent):
            word_list.append(word)    
    vocabulary = [word for word in word_list if len(word) > 2]    
    vocabulary = [word for word in vocabulary 
                  if not word in stopwords.words('english')]    
    vocabulary = [stemmer.stem(word) for word in vocabulary]
    ngramsize=2
    if ngramsize>1:
        vocabulary=[word for word in ngrams(
               vocabulary,ngramsize)]
        
    
    return vocabulary
    
def word_feats(words):   
    if words != words:
        words = ""    
    vocabulary = getWordList(words, lambda x:x.lower())    
    return dict([(word,True) for word in vocabulary])#creating list of words in format suitable for classifier

pos_feat=[]
neg_feat=[]    
pos_feat=[(word_feats(f),'1')
            for f in posSample["reviewcontent"]]
neg_feat=[(word_feats(f),'0')
            for f in negSample["reviewcontent"]]

#creating test and train data set
train=pos_feat[:4000]+neg_feat[:4000]

test=pos_feat[4000:]+neg_feat[4000:]
#using Naive Bayes classifier for creating model
from nltk.classify import NaiveBayesClassifier
classifier=NaiveBayesClassifier.train(train)

import collections
refsets=collections.defaultdict(set)
testsets=collections.defaultdict(set)
result=[]
for i,(feats,label) in enumerate(test):
    refsets[label].add(i)
    observed=classifier.classify(feats)#using trained model on test data
    testsets[observed].add(i)
    result.append(observed)  

#calculating various metrics to measure the performance of model

import math
from nltk.metrics import precision
from nltk.metrics import recall

pos_pre=precision(refsets['1'],testsets['1'])
neg_pre=precision(refsets['0'],testsets['0'])
pos_rec=recall(refsets['1'],testsets['1'])
neg_rec=recall(refsets['0'],testsets['0'])
gperf=math.sqrt(pos_pre*neg_rec)

print('Positive Precision: ',pos_pre)
print('Negative Precision: ',neg_pre)
print('Positive Recall: ',pos_rec)    
print('Negative Recall: ',neg_rec)
print('G-Performance: ',gperf)

df = pd.DataFrame()
se2=pd.Series(['1']*5000+['0']*5000)
df['Original_recomendation']=se2
se3=pd.Series(['']*4000+result[0:1000]+['']*4000+result[1000:2000])
df['Predicted_Class']=se3
df.to_csv("E:/MongoProject/Results.csv")#it must be an relative path with respect to this file location
