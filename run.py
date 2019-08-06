'''
To run on Fladoop cluster

spark-submit --master yarn-client --queue si601w16 --num-executors 2 --executor-memory 1g --executor-cores 2 si601_w16_hw5_syhliu.py
'''
import simplejson as json
from pyspark import SparkContext
sc = SparkContext(appName="CityCount")

input_file_business = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_business.json")
input_file_review = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_review.json")

def cat_star(data):
  cat_star_list = []
  city = data.get('city', None)
  stars = data.get('stars', None)
  neighbor = data.get('neighborhoods', None)
  review_count = data.get('review_count', None)
  if city:
    if not neighbor:
      neighbor =  ['Unknown']
    for n in neighbor:
        cat_star_list.append(((city,n),(review_count, stars,1)))
      
  return cat_star_list

# map is going to call the whole thing key and values
# mapValue is going to call only the value part

cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_star) \
                      .reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1], x[2]+y[2])) \
                      .mapValues(lambda x: (x[2], x[0], x[1]/x[2])) \
                      .map(lambda x: (x[0][0],x[0][1],x[1][0],x[1][1],x[1][2]))\
                      .sortBy(lambda x:(x[0]), ascending = True)



cat_stars.map(lambda x:(x[0] + '\t' + x[1] + '\t' + str(x[2]) + '\t' +str(x[3]) + '\t' + str(x[4]))) \
         .saveAsTextFile("hw5_output")
