#1
news = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('hdfs://master:9000/user/HW2/News_Final.csv')
title_forall = news.select("Title","Topic","PublishDate").rdd.map(list)
headline_forall = news.select("Headline","Topic","PublishDate").rdd.map(list)
from operator import add

#title_output

title_output = title_forall.map(lambda x:x[0])
title_output = title_output.map(lambda x:(x.split(' ')))
title_output = title_output.flatMap(lambda x:x)
title_output = title_output.map(lambda x:(x,1))
title_output = title_output.reduceByKey(add)
title_output = title_output.sortBy(lambda x:x[1],False).collect()

#title_pertopic

title_pertopic = title_forall.map(lambda x:[x[0],x[1]])
title_pertopic = title_pertopic.map(lambda x:(x[0].split(' ') ,x[1]))
title_pertopic = title_pertopic.flatMap(lambda x:[(element, x[1]) for element in x[0]])
title_pertopic = title_pertopic.map(lambda x:(x,1))
from operator import add
title_pertopic = title_pertopic.reduceByKey(add)
title_pertopic = title_pertopic.sortBy(lambda x: x[1], False).collect()

#title_perday

title_perday = title_forall.map(lambda x:[x[0],x[2].split(' ')[0]])
title_perday = title_perday.map(lambda x:(x[0].split(' '),x[1]))
title_perday = title_perday.flatMap(lambda x:[(element, x[1])for element in x[0]])
title_perday = title_perday.map(lambda x:(x,1))
title_perday = title_perday.reduceByKey(add)
title_perday = title_perday.sortBy(lambda x:x[1], False).collect()

#headline_output

headline_output = headline_forall.map(lambda x:x[0])
headline_output = headline_output.filter(lambda x:type(x) == str)
headline_output = headline_output.map(lambda x:(x.split(' ')))
headline_output = headline_output.flatMap(lambda x:x)
headline_output = headline_output.map(lambda x:(x,1))
headline_output = headline_output.reduceByKey(add)
headline_output = headline_output.sortBy(lambda x:x[1],False).collect()

#headline_pertopic

headline_pertopic = headline_forall.map(lambda x:[x[0],x[1]])
headline_pertopic = headline_pertopic.filter(lambda x:type(x[0]) == str)
headline_pertopic = headline_pertopic.map(lambda x:(x[0].split(' '),x[1]))
headline_pertopic = headline_pertopic.flatMap(lambda x:[(element, x[1])for element in x[0]])
headline_pertopic = headline_pertopic.map(lambda x:(x,1))
headline_pertopic = headline_pertopic.reduceByKey(add)
headline_pertopic = headline_pertopic.sortBy(lambda x:x[1],False).collect()

#headline_perdate

headline_perdate = headline_forall.map(lambda x:[x[0],x[2]])
headline_perdate = headline_perdate.filter(lambda x:type(x[0]) == str)
headline_perdate = headline_perdate.map(lambda x:(x[0].split(' '),x[1].split(' ')[0]))
headline_perdate = headline_perdate.flatMap(lambda x:[(element,x[1])for element in x[0]])
headline_perdate = headline_perdate.map(lambda x:(x,1))
headline_perdate = headline_perdate.reduceByKey(add)
headline_perdate = headline_perdate.sortBy(lambda x:x[1],False).collect()

#2

source = 'hdfs://master:9000/user/HW2/'
fileList=['Facebook_Economy', 'Facebook_Microsoft', 'Facebook_Obama', 'Facebook_Palestine',\
          'GooglePlus_Economy', 'GooglePlus_Microsoft', 'GooglePlus_Obama', 'GooglePlus_Palestine',\
          'LinkedIn_Economy', 'LinkedIn_Microsoft', 'LinkedIn_Obama', 'LinkedIn_Palestine']

header_per_hour=['IDLink'] + ['TS'+str((count+1)*3) for count in range(48)]
header_per_day=['IDLink'] + ['TS'+str((count+1)*72) for count in range(2)]
Facebook = 'Facebook'
GooglePlus = 'GooglePlus'
LinkedIn = 'LinkedIn'
Facebook_hour = open('Facebook_hour.csv','w')
Facebook_day = open('Facebook_day.csv','w')
GooglePlus_hour = open('GooglePlus_hour.csv','w')
GooglePlus_day = open('GooglePlus_day.csv','w')
LinkedIn_hour = open('LinkedIn_hour.csv','w')
LinkedIn_day = open('LinkedIn_day.csv','w')
for i in fileList:
	feedback = sqlContext.read.format('csv').options(header = 'true').options(inferschema = 'true').load(source + i + '.csv')
	avg_perhour = feedback.select(header_per_hour).rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:])).reduceByKey(add).map(lambda x:(x[0], x[1]/48)).sortByKey().map(lambda x:('ID'+str(x[0]), x[1])).collect()
	avg_perday = feedback.select(header_per_day).rdd.map(list).flatMap(lambda x:((x[0], element) for element in x[1:])).reduceByKey(add).map(lambda x:(x[0], x[1]/2)).sortByKey().map(lambda x:('ID'+str(x[0]), x[1])).collect()
	if i.split('_')[0] == Facebook:
		Facebook_hour = open('Facebook_hour.csv','w')
		Facebook_day = open('Facebook_day.csv','w')
		for j in avg_perhour:
			k = ':'.join([str(l)for l in j])
			Facebook_hour.write(k + '\n')
		for j in avg_perday:
			k = ':'.join([str(l)for l in j])
			Facebook_day.write(k + '\n')
		Facebook_hour.close()
		Facebook_day.close()
	if i.split('_')[0] == GooglePlus:
		GooglePlus_hour = open('GooglePlus_hour.csv','w')
		GooglePlus_day = open('GooglePlus_day.csv','w')
		for j in avg_perhour:
			k = ':'.join([str(l)for l in j])
			GooglePlus_hour.write(k + '\n')
		for j in avg_perday:
			k = ':'.join([str(l)for l in j])
			GooglePlus_day.write(k + '\n')
		GooglePlus_hour.close()
		GooglePlus_day.close()
	if i.split('_')[0] == LinkedIn:
		LinkedIn_hour = open('LinkedIn_hour.csv','w')
		LinkedIn_day = open('LinkedIn_day.csv','w')
		for j in avg_perhour:
			k = ':'.join([str(l)for l in j])
			LinkedIn_hour.write(k + '\n')
		for j in avg_perday:
			k = ':'.join([str(l)for l in j])
			LinkedIn_day.write(k + '\n')
		LinkedIn_hour.close()
		LinkedIn_day.close()

#3
news = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('hdfs://master:9000/user/HW2/News_Final.csv')
from operator import add
topic = news.select("Topic","SentimentTitle","SentimentHeadline").rdd.map(list)
topics = ['economy','obama','microsoft','palestine']
topic_sentimenttitle = topic.filter(lambda x:x[0] in topics).map(lambda x:(x[0],float(x[1])))
topic_sentimenttitle = topic_sentimenttitle.reduceByKey(add).collect()
topic_sentimentheadline = topic.filter(lambda x:x[0] in topics).map(lambda x:(x[0],float(x[1])))
topic_sentimentheadline = topic_sentimentheadline.reduceByKey(add).collect()
total_topic = topic.filter(lambda x:x[0] in topics).map(lambda x:(x[0],1))
total_topic = total_topic.reduceByKey(add).collect()

for i in range(4):
	topic_sentimenttitle = topic_sentimenttitle + [(topics[i] , topic_sentimenttitle[i][1] / total_topic[i][1])]
	topic_sentimentheadline = topic_sentimentheadline + [(topics[i] , topic_sentimentheadline[i][1] / total_topic[i][1])]

#4
