from pyspark import SparkContext, SparkConf
sc = SparkContext()
movies = sc.textFile('/dataset/ml-latest-small/movies.csv')
result = movies.take(10)
for r in result:
    print(r)
moviesData = movies.filter(lambda s: not s.startswith('mov'))
c = moviesData.count()
print('The movies count is :', c)

ratings = sc.textFile('/dataset/ml-latest-small/ratings.csv')
result = ratings.take(10)
for r in result:
    print(r)
ratingsData = ratings.filter(lambda s: not s.startswith('user'))
c = ratingsData.count()
print('The ratings count is :', c)

moviesData_id = moviesData.map(lambda s: s.split(',')).map(lambda s: (int(s[0]), str(s[1])))
print(moviesData_id.first())

ratingsData_id = ratingsData.map(lambda s: s.split(',')).map(lambda s: (int(s[1]), float(s[2])))
print(ratingsData_id.first())

top10 = moviesData_id.join(ratingsData_id)
print(top10.take(10))

top10 = moviesData_id.join(ratingsData_id).map(lambda s: ((s[0], s[1][0]), s[1][1]))
print(top10.take(10))

top10 = moviesData_id.join(ratingsData_id).map(lambda s: ((s[0], s[1][0]), s[1][1])).groupByKey().mapValues(lambda s: (sum(s) / len(s), len(s)))
print(top10.take(10))

top10 = moviesData_id.join(ratingsData_id).map(lambda s: ((s[0], s[1][0]), s[1][1])).groupByKey().mapValues(lambda s: (sum(s) / len(s), len(s))).filter(lambda s: s[1][1] > 100).sortBy(lambda s: s[1], False)
print(top10.take(10))