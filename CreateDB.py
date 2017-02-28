import pandas as pd
import csv
import math

#py2neo 3.1.2
import py2neo
from py2neo import *

py2neo.authenticate("localhost:7474", "neo4j", "termproject")

graph = Graph("http://localhost:7474/db/data/")

'''
#graph.delete_all()

#1 Creating database in Neo4j from .csv file
# Reading records from the csv file
review = pd.read_csv('C:/Users/darsh/OneDrive/Documents/MSIS/Spring Semester/MM1/Unstructured Data Management/Term Project/AirlineSentimentResults.csv', encoding='latin-1')
#Using only the first 10000 reviews for analysis
review = review[:10000]

# Creating a set of unique author and airline names
setAuthor = set(review['authorname'])
setAirline = set(review['airlinename'])

# Insert Author Nodes
for i in setAuthor:
    i = Node("Author", name=i)
    graph.create(i)
    
# Insert Airline Nodes
for j in setAirline:
    j = Node("Airline", name=j)
    graph.create(j)

# Create relationships between Author and Airline and give values to its parameters
statement = "MATCH (a {name:{A}}), (b {name:{B}}) \
                    CREATE (a)-[:RATED {authorcountry:{C}, \
                                        aircraft:{D}, \
                                        route:{E}, \
                                        travellertype:{F}, \
                                        cabin:{G}, \
                                        rating_cabinstaff:{H}, \
                                        rating_foodbeverage:{I}, \
                                        rating_inflightEnt:{J}, \
                                        rating_overall:{K}, \
                                        rating_seatcomfort:{L}, \
                                        rating_valuemoney:{M}, \
                                        recommended:{N}, \
                                        reviewcontent:{O}, \
                                        reviewdate:{P}, \
                                        sentiment:{Q}}]->(b)"


authorcountry = []
aircraft = []
route = []
travellertype = []
cabin = []
rating_cabinstaff = []
rating_foodbeverage = []
rating_inflightEnt = []
rating_overall = []
rating_seatcomfort = []
rating_valuemoney = []
recommended = []
sentiment = []

for k in range(0, len(review)):
   
    if review.loc[k, 'authorcountry'] != review.loc[k, 'authorcountry']:
        authorcountry.append('NoValue')
    else:
        authorcountry.append(review.loc[k, 'authorcountry'])
    
    if review.loc[k, 'aircraft'] != review.loc[k, 'aircraft']:
        aircraft.append('NoValue')
    else:
        aircraft.append(review.loc[k, 'aircraft'])

    if review.loc[k, 'route'] != review.loc[k, 'route']:
        route.append('NoValue')
    else:
        route.append(review.loc[k, 'route'])

    if review.loc[k, 'travellertype'] != review.loc[k, 'travellertype']:
        travellertype.append('NoValue')
    else:
        travellertype.append(review.loc[k, 'travellertype'])

    if review.loc[k, 'cabin'] != review.loc[k, 'cabin']:
        cabin.append('NoValue')
    else:
        cabin.append(review.loc[k, 'cabin'])
       
    if (math.isnan(review.loc[k, 'rating_cabinstaff'])):
        rating_cabinstaff.append('NoValue')
    else:
        rating_cabinstaff.append(str(review.loc[k, 'rating_cabinstaff']))

    if (math.isnan(review.loc[k, 'rating_foodbeverage'])):
        rating_foodbeverage.append('NoValue')
    else:
        rating_foodbeverage.append(str(review.loc[k, 'rating_foodbeverage']))

    if (math.isnan(review.loc[k, 'rating_inflightEnt'])):
        rating_inflightEnt.append('NoValue')
    else:
        rating_inflightEnt.append(str(review.loc[k, 'rating_inflightEnt']))

    if (math.isnan(review.loc[k, 'rating_overall'])):
        rating_overall.append('NoValue')
    else:
        rating_overall.append(str(review.loc[k, 'rating_overall']))

    if (math.isnan(review.loc[k, 'rating_seatcomfort'])):
        rating_seatcomfort.append('NoValue')
    else:
        rating_seatcomfort.append(str(review.loc[k, 'rating_seatcomfort']))

    if (math.isnan(review.loc[k, 'rating_valuemoney'])):
        rating_valuemoney.append('NoValue')
    else:
        rating_valuemoney.append(str(review.loc[k, 'rating_valuemoney']))

    if (math.isnan(review.loc[k, 'recommended'])):
        recommended.append('NoValue')
    elif review.loc[k, 'recommended'] == 1:
        recommended.append('Yes')
    elif review.loc[k, 'recommended'] == 0:
        recommended.append('No')
        
    sentiment.append(str(review.loc[k, 'sentiment']))

tx = graph.begin()
for k in range(0, len(review)):
    tx.run(statement, {"A": review.loc[k,'authorname'], 
                          "B": review.loc[k,'airlinename'],
                          "C": authorcountry[k], 
                          "D": aircraft[k], 
                          "E": route[k],
                          "F": travellertype[k],
                          "G": cabin[k],
                          "H": rating_cabinstaff[k],
                          "I": rating_foodbeverage[k],
                          "J": rating_inflightEnt[k],
                          "K": rating_overall[k],
                          "L": rating_seatcomfort[k],
                          "M": rating_valuemoney[k],
                          "N": recommended[k],
                          "O": review.loc[k,'reviewcontent'],
                          "P": review.loc[k,'reviewdate'],
                          "Q": sentiment[k]})

tx.commit()
'''

'''
#2 Comparison between direct competitors
results = graph.run("""MATCH (ai1: Airline)<-[r1:RATED]-(au: Author)-[r2:RATED]->(ai2: Airline) 
                    WHERE NOT (ai1 = ai2)
                    WITH ai1, ai2, au, r1, r2, 
                    CASE toInt(r1.sentiment) > toInt(r2.sentiment)
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as BetterAirline,
                    CASE toInt(r1.rating_cabinstaff) > toInt(r2.rating_cabinstaff) 
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as BetterCabinStaff,
                    CASE toInt(r1.rating_foodbeverage) > toInt(r2.rating_foodbeverage)
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as BetterFoodBeverage,
                    CASE toInt(r1.rating_inflightEnt) > toInt(r2.rating_inflightEnt) 
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as BetterInFlightEntertainment,
                    CASE toInt(r1.rating_seatcomfort) > toInt(r2.rating_seatcomfort)
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as BetterSeatComfort,
                    CASE toInt(r1.rating_valuemoney) > toInt(r2.rating_valuemoney) 
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as BetterValueForMoney,
                    CASE toInt(r1.recommended) > toInt(r2.recommended)
                    WHEN True THEN ai1.name 
                    ELSE ai2.name END as RecommendedAirline
                    Return au.name, ai1.name, r1.sentiment, 
                    ai2.name, r2.sentiment, BetterAirline, 
                    RecommendedAirline, BetterCabinStaff, BetterFoodBeverage, 
                    BetterInFlightEntertainment, BetterSeatComfort, BetterValueForMoney""")

for result in results:
    print(result)
    print()
'''

'''
#4 Analyzing raters with with common interests
results = graph.run("""MATCH (au1:Author)-[r1:RATED]->(ai:Airline)<-[r2:RATED]-(au2:Author) 
                    WHERE NOT (au1 = au2)    
                    WITH count(ai) as SimilarityIndex, au1, au2 
                    ORDER BY SimilarityIndex DESC LIMIT 20
                    CREATE UNIQUE (au1)-[d:SimilarityIndex]->(au2)
                    SET d.count=SimilarityIndex
                    RETURN d.count as SimilarityIndex, au1.name, au2.name""")
for result in results:
    print(result)
'''

'''
#4 (Optional) Analyzing raters with with common interests

review = pd.read_csv('C:/Users/darsh/OneDrive/Documents/MSIS/Spring Semester/MM1/Unstructured Data Management/Term Project/AirlineSentimentResults.csv', encoding='latin-1')
review = review[:10000]

setAuthor = set(review['authorname'])

statement = "MATCH (au: Author)-[r:RATED]->(ai: Airline {name:{A}})<-[r2:RATED]-(au2: Author) \
                    WITH count(r) as NumberOfTravels, count(r2) as NumberOfTravels2, ai, au, au2 \
                    WHERE NumberOfTravels > 5 AND NOT (au.name = au2.name)\
                    RETURN DISTINCT(ai.name) as Airline, au.name as Author, \
                    au2.name as Author2, NumberOfTravels, NumberOfTravels2 \
                    ORDER BY NumberOfTravels DESC LIMIT 1"
 

tx = graph.begin()
for k in setAirline:
    result = tx.run(statement, {"A": k})
    print(result.data())
    print()
'''

