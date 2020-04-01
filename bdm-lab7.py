from pyspark import SparkContext


bike = sc.textFile('/tmp/bdm/citibike.csv')
taxi = sc.textFile('/tmp/bdm/yellow.csv.gz')

bikeStation = (-74.00263761, 40.73901691)

def filterBike(records):
    for record in records:
        fields = record.split(',')
        if (fields[6]=='Greenwich Ave & 8 Ave' and 
            fields[3].startswith('2015-02-01')):
            yield (fields[3][:19], 1)


def filterTaxi(pid, lines):
    if pid==0:
        next(lines)
    import pyproj
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    station = proj(-74.00263761, 40.73901691)
    squared_radius = 1320**2
    for trip in lines:
            fields = trip.split(',')
            if 'NULL' in fields[4:6]: continue # ignore trips without locations
            dropoff = proj(fields[5], fields[4])
            squared_distance = (dropoff[0]-station[0])**2 + (dropoff[1]-station[1])**2
            if (fields[1].startswith('2015-02-01') and
                squared_distance <= squared_radius):
                yield (fields[1][:19], 0)

def connectTrips(_, records):
    import datetime
    lastTaxiTime = None
    count = 0
    for dt,mode in records:
        t = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        if mode==1:
            if lastTaxiTime!=None:
                diff = (t-lastTaxiTime).total_seconds()
                if diff>=0 and diff<=600:
                    count += 1
        else:
            lastTaxiTime = t
    yield(count)



if __name__=='__main__':
    sc = SparkContext()
    bike = sc.textFile('/tmp/bdm/citibike.csv')
    taxi = sc.textFile('/tmp/bdm/yellow.csv.gz')
    matchedBike = bike.mapPartitions(filterBike)
    bikeStation = (-74.00263761, 40.73901691)
    matchedTaxi = taxi.mapPartitionsWithIndex(filterTaxi)

    allTrips = (matchedBike+matchedTaxi).sortByKey().cache()

    counts = allTrips.mapPartitionsWithIndex(connectTrips).reduce(lambda x,y: x+y)
    print(counts)

