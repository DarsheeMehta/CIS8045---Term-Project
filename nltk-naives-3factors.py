#Importing all required libraries for text analytics
import pymongo
import nltk
from pymongo import MongoClient
import pandas as pd
import csv
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer

#connecting to mongoDB for data

con=MongoClient()
airlineDB=con.Airline
reviews=airlineDB.reviews
reviewData=pd.DataFrame(list(reviews.find()))
posResult=pd.DataFrame(list(reviews.find({'recommended':1})))
negResult=pd.DataFrame(list(reviews.find({'recommended':0})))


#Finding Correlations to get most influencing factors driving recommendation

cabinToR=reviewData['rating_cabinstaff'].corr(reviewData['recommended'])
foodToR=reviewData['rating_foodbeverage'].corr(reviewData['recommended'])
inflightToR=reviewData['rating_inflightEnt'].corr(reviewData['recommended'])
overallToR=reviewData['rating_overall'].corr(reviewData['recommended'])
seatToR=reviewData['rating_seatcomfort'].corr(reviewData['recommended'])
valueToR=reviewData['rating_valuemoney'].corr(reviewData['recommended'])

#Sampling done here is optional and can be removed if needed

posSample=posResult[:5000]
negSample=negResult[:5000]


def Nancheck(content):
    if content != content:
        return "" 
    return content 

def num_feats(words,j): 
    list1 = []   
    list1.append(Nancheck(words["rating_cabinstaff"][j]))
    list1.append(Nancheck(words["rating_overall"][j]))
    list1.append(Nancheck(words["rating_valuemoney"][j]))     
    return dict([(word,True) for word in list1])

#creating test and train data set

pos_feat=[(num_feats(posSample,j),'1')
            for j in range(0,len(posSample))]
neg_feat=[(num_feats(negSample,j),'0')
            for j in range(0,len(negSample))]

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
df.to_csv("E:/MongoProject/Results.csv")#it must be an raltive path with respect to this file location