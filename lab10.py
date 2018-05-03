import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession


def filterBike(pId, lines):
    import csv
    for row in csv.reader(lines):
        if (row[6] == 'Greenwich Ave & 8 Ave' and row[3].startswith('2015-02-01')):
            yield (row[3][:19])


def filterTaxi(pId, lines):
    if pId == 0:
        next(lines)

    import csv
    import pyproj
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    gLoc = proj(-74.00263761, 40.73901691)
    sqm = 1320 ** 2
    for row in csv.reader(lines):
        try:
            dropoff = proj(float(row[5]), float(row[4]))
        except:
            continue
        distance = (dropoff[0] - gLoc[0]) ** 2 + (dropoff[1] - gLoc[1]) ** 2
        if distance < sqm:
            yield row[1][:19]




if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    sc = SparkContext()

    spark = SparkSession(sc)

    taxi = sc.textFile('yellow.csv.gz')
    bike = sc.textFile('citibike.csv')

    gbike = bike.mapPartitionsWithIndex(filterBike).cache()
    gTaxi = taxi.mapPartitionsWithIndex(filterTaxi).cache()

    lBikes = gbike.collect()
    lTaxis = gTaxi.collect()

    import datetime

    count = 0
    for b in lBikes:
        # Convert the datetime string to a datetime object
        bt = datetime.datetime.strptime(b, '%Y-%m-%d %H:%M:%S')
        for t in lTaxis:
            # Convert the datetime string to a datetime object
            tt = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')

            diff = (bt - tt).total_seconds()

            if diff > 0 and diff < 600:
                count += 1
                break

    print(count)