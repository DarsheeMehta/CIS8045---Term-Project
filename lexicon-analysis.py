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
# We use [General Inquirer](http://www.wjh.harvard.edu/~inquirer/), 
# a free dictionary, to analyze reveiw sentiment.
#Import positive and negative words from General Inquirer Dictionary*
positive=[]
negative=[]
with open('E:\MongoProject\general_inquirer_dict.txt') as fin:
    # Reads the csv file delimited by tab, separates the negative 
    # and positive words and adds them to the list
    reader = csv.DictReader(fin,delimiter='\t')
    for i,line in enumerate(reader):
        if line['Negativ']=='Negativ':
            if line['Entry'].find('#')==-1:
                negative.append(line['Entry'].lower())
            if line['Entry'].find('#')!=-1: #In General Inquirer, some words have multiple senses. Combine all tags for all senses.
                negative.append(line['Entry'].lower()[:line['Entry'].index('#')]) 
        if line['Positiv']=='Positiv':
            if line['Entry'].find('#')==-1:
                positive.append(line['Entry'].lower())
            if line['Entry'].find('#')!=-1: #In General Inquirer, some words have multiple senses. Combine all tags for all senses.
                positive.append(line['Entry'].lower()[:line['Entry'].index('#')])

#Closes the file
fin.close()

pvocabulary=sorted(list(set(positive))) 
nvocabulary=sorted(list(set(negative))) 

#creating relevant columns for analysis
reviewData['pos']=0
reviewData['neg']=0
reviewData['sentiment']=0


# Gets all the words in the reviews after tokenizing sentences and words
# and adding it to the list word_list
def getWordList(text, word_proc = lambda x:x):
    word_list = []
    for sent in sent_tokenize(text):
        for word in word_tokenize(sent):
            word_list.append(word)
    return word_list
  
# Gets all the english stemmer words in the variable stemmer
stemmer = SnowballStemmer("english")

pcount_list = []
ncount_list = []
senti_list = []
sample2=reviewData[:7000]#sampling is optional and can be removed
review_index=0
for text in sample2['reviewcontent']:
    vocabulary = getWordList(text, lambda x:x.lower()) 
    
    # Remove words with a length of 1
    vocabulary = [word for word in vocabulary if len(word) > 1]
                  
    # Remove stopwords
    vocabulary = [word for word in vocabulary 
                  if not word in stopwords.words('english')]

    # Stem words
    vocabulary = [stemmer.stem(word) for word in vocabulary]
    
    # Counts the total occurrance of positive and negative words
    # in the list named vocabulary and appends the number to pcount_list
    # and ncount_list and stores the overall difference of pcount and ncount
    # to get the overall sentiment of each review and stores it to 
    # senti_list
    pcount = 0
    ncount = 0
    for pword in pvocabulary:
        pcount += vocabulary.count(pword)
    for nword in nvocabulary:
        ncount += vocabulary.count(nword)
    
    pcount_list.append(pcount)
    ncount_list.append(ncount)
    senti_list.append(pcount-ncount)
    
    # Adds the sentiment count to the corresponding row and column
    sample2.loc[review_index, 'pos'] = pcount
    sample2.loc[review_index, 'neg'] = ncount
    sample2.loc[review_index, 'sentiment'] = pcount - ncount        
    review_index += 1 #Increases the row index

#Applying OLS regression 
import statsmodels.formula.api as sm
result1 = sm.ols(formula="recommended~sentiment", data = sample2).fit()
print(result1.summary())
result2 = sm.ols(formula="rating_overall~sentiment", data = sample2).fit()
print(result2.summary())

#Applying logistic regression
import statsmodels.api as sm2
logit = sm2.Logit(sample2['recommended'], sample2['sentiment'])
result = logit.fit()
print(result.summary())