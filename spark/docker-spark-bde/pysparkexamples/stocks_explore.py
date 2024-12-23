from pyspark import SparkContext, SparkConf

def toCSV(data):
    return ','.join(data)

sc = SparkContext()
stocks = sc.textFile('/dataset/stocks.csv')
print(stocks.first())
stocks.cache()

stocksFiltered = stocks.filter(lambda s: not s.startswith('symbol'))
stocksFilteredData = stocksFiltered.map(lambda s: s.split(','))
stocksFilteredData.cache()

result = stocksFilteredData.filter(lambda s: s[1].split(' ')[-1] == '2003')
result.cache()
print('The Count is: ')
print(result.count())

print('The Data is: ')
print(result.take(10))

result.map(toCSV).saveAsTextFile('/result2.csv')