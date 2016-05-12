from pyspark.mllib.recommendation import *
import random
from pyspark import SparkContext

sc = SparkContext( 'local', 'pyspark')

def canonical(a):
    if aliasDict.has_key(a[1]):
        return (a[0], aliasDict.get(a[1]), a[2])
    else:
        return a
    
path_AA = "s3://aws-logs-778814615222-us-east-1/audio_data/artist_alias.txt"
path_AD = "s3://aws-logs-778814615222-us-east-1/audio_data/artist_data.txt"
path_UAD = "s3://aws-logs-778814615222-us-east-1/audio_data/user_artist_data.txt"

#read txt files
artistAlias = sc.textFile(path_AA)
aliasDict = artistAlias.map(lambda x: (x.split("\t")[0], (x.split("\t")[1]))).collectAsMap()

artistData = sc.textFile(path_AD)

userArtistData = sc.textFile(path_UAD).map(lambda x : (x.split(" ")[0], x.split(" ")[1], x.split(" ")[2]))
userArtistData = userArtistData.map(canonical)

#RandomSplit the data, so everytime the result may be a little bit different
userArtistData = userArtistData.map(lambda x: (int(x[0]), int(x[1]), int(x[2])))
trainData, validationData, testData = userArtistData.randomSplit([0.4, 0.4, 0.2], 13)

#Use the trainDate to train the model, parameters are as same as ch3
#This step will take 10~15 minutes
Model = ALS.trainImplicit(trainData, 10, 5, 0.01)

#Input UserId here
UserId = 2093760

#Recommend 10 artists 
recommendations = list(Model.call("recommendProducts", UserId, 10))

#Put 10 artists IDs into a list
Sequence = []
for i in range(10):
    print recommendations[i].product  #print numbers
    Sequence.append(str(recommendations[i].product))
Ans = ['0' for i in range(10)]
print ''

#Find the corresponding name of artists
artistDataText = artistData.collect()

for line in artistDataText:
    line1 = line.strip()
    a = line.split("\t")
    if a[0] in Sequence:
        Ans[Sequence.index(a[0])] = a[1]

for i in Ans:
    ans = i.strip()
    print ans   #print names

