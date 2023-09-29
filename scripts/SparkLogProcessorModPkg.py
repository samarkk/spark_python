import os
import sys
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from sys import argv
import re
import datetime
from pyspark.sql import Row
import matplotlib.pyplot as plt
from pkgdemo.logparse import parseApacheLogLine



script, fileLoc = argv
spark = SparkSession.builder.appName('SparkLogProcessorModPkg') .config('spark.warehouse.dir','/apps/hive/warehouse') .enableHiveSupport().getOrCreate()
spark.version
sc = spark.sparkContext
sc.setLogLevel('ERROR')
sc.addFile('hdfs://localhost:8020/user/cloudera/pkgdemo', recursive=True)



APACHE_ACCESS_LOG_PATTERN ='^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)'

def parseLogs():
    """ Read and parse log file """
   
    parsed_logs = (sc
                   .textFile(fileLoc)
                   .map(parseApacheLogLine)
                   .cache())
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())
    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print('Number of invalid logline: %d' % failed_logs.count())
        for line in failed_logs.take(20):
            print('Invalid logline: %s' % line)
    print('Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs

parsed_logs, access_logs, failed_logs = parseLogs()

assert(failed_logs.count() == 0)
assert(parsed_logs.count() == 1043177)
assert(access_logs.count() == parsed_logs.count())

content_sizes = access_logs.map(lambda log: log.content_size).cache()
print('Content Size Avg: %i, Min: %i, Max: %s' % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()))

responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .cache())
responseCodeToCountList = responseCodeToCount.collect()
print( 'Found %d response codes' % len(responseCodeToCountList))
print( 'Response Code Counts: %s' % responseCodeToCountList)

assert len(responseCodeToCountList) == 7
assert sorted(responseCodeToCountList) == [(200, 940847), (302, 16244), (304, 79824), (403, 58), (404, 6185), (500, 2), (501, 17)]

labels = responseCodeToCount.map(lambda x : x[0]).collect()
print(labels)

count = access_logs.count()
fracs = responseCodeToCount.map(lambda x : (float(x[1]) / count)).collect()
print(fracs)


def pie_pct_format(value):
    """ Determine the appropriate format string for the pie chart percentage label
    Args:
        value: value of the pie slice
    Returns:
        str: formated string label; if the slice is too small to fit, returns an empty string for label
    """
    return '' if value < 7 else '%.0f%%' % value
fig = plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
colors = ['yellowgreen', 'lightskyblue', 'gold', 'purple', 'lightcoral', 'yellow', 'black']
explode = (0.05, 0.05, 0.1, 0, 0, 0, 0)
patches, texts, autotexts = plt.pie(fracs, labels=labels, colors=colors,
                                    explode=explode, autopct=pie_pct_format,
                                    shadow=False,  startangle=125)
for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')  # If the slice is small to fit, don't show a text label
plt.legend(labels, loc=(0.80, -0.1), shadow=True)
plt.show(block=False)
plt.pause(5)
plt.close()

hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))
hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)
hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
hostsPick20 = (hostMoreThan10
               .map(lambda s: s[0])
               .take(20))
print('Any 20 hosts that have accessed more then 10 times: %s' % hostsPick20)

endpoints = (access_logs
             .map(lambda log: (log.endpoint, 1))
             .reduceByKey(lambda a, b : a + b)
             .cache())
ends = endpoints.map(lambda x: x[0]).collect()
counts = endpoints.map(lambda x: x[1]).collect()

fig = plt.figure(figsize=(12, 10), facecolor='white', edgecolor='white')
plt.axis([0, len(ends), 0, max(counts)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Endpoints')
plt.ylabel('Number of Hits')
plt.title('Endpoints Number of Hits')
plt.plot(counts)
plt.show(block=False)
plt.pause(5)
plt.close()

endpointCounts = (access_logs
                  .map(lambda log: (log.endpoint, 1))
                  .reduceByKey(lambda a, b : a + b))
topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])
print('Top Ten Endpoints: %s' % topEndpoints)
assert topEndpoints == [(u'/images/NASA-logosmall.gif', 59737), (u'/images/KSC-logosmall.gif', 50452), (u'/images/MOSAIC-logosmall.gif', 43890), (u'/images/USA-logosmall.gif', 43664), (u'/images/WORLD-logosmall.gif', 43277), (u'/images/ksclogo-medium.gif', 41336), (u'/ksc.html', 28582), (u'/history/apollo/images/apollo-logo1.gif', 26778), (u'/images/launch-logo.gif', 24755), (u'/', 20292)], 'incorrect Top Ten Endpoints'

not200_or_304 = access_logs.filter(lambda x: x.response_code != (200 or 304))
endpointCountPairTuple = not200_or_304.map(lambda x:(x[3],1))
endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b: a+ b)
topTenErrURLs = endpointSum.takeOrdered(10, lambda x: -1 * x[1])

print('Top Ten failed URLs: %s' % topTenErrURLs)

assert endpointSum.count() == 7689, 'incorrect count for endpointSum'
assert topTenErrURLs == [(u'/images/NASA-logosmall.gif', 8761), (u'/images/KSC-logosmall.gif', 7236), 
                         (u'/images/MOSAIC-logosmall.gif', 5197), (u'/images/USA-logosmall.gif', 5157), 
                         (u'/images/WORLD-logosmall.gif', 5020), (u'/images/ksclogo-medium.gif', 4728),
                         (u'/history/apollo/images/apollo-logo1.gif', 2907), 
                         (u'/images/launch-logo.gif', 2811), (u'/', 2199), 
                         (u'/images/ksclogosmall.gif', 1622)], 'incorrect Top Ten failed URLs (topTenErrURLs)'


hosts = access_logs.map(lambda log: (log.host))
uniqueHosts = hosts.distinct()
uniqueHostCount = uniqueHosts.count()

print('Unique hosts: %d' % uniqueHostCount)
assert uniqueHostCount == 54507, 'incorrect uniqueHostCount'

dayHostCount = access_logs.map(lambda x: (x.date_time.day, x.host))
dailyHosts = dayHostCount.groupByKey().sortByKey().map(lambda x: (x[0], len(set(x[1])))).cache()
dailyHostsList = dailyHosts.take(30)

print('Unique hosts per day: %s' % dailyHostsList)

assert dailyHosts.count() == 21, 'incorrect dailyHosts.count()'
assert dailyHostsList == [(1, 2582), (3, 3222), (4, 4190), (5, 2502), (6, 2537), (7, 4106), (8, 4406), (9, 4317), (10, 4523), (11, 4346), (12, 2864), (13, 2650), (14, 4454), (15, 4214), (16, 4340), (17, 4385), (18, 4168), (19, 2550), (20, 2560), (21, 4134), (22, 4456)], 'incorrect dailyHostsList'
assert dailyHosts.is_cached, 'incorrect dailyHosts.is_cached'

daysWithHosts = dailyHosts.map( lambda x : x[0]).collect()
hosts = dailyHosts.map( lambda x : x[1]).collect()

test_days =[x for x in  range(1, 23) if x != 2]

assert daysWithHosts == test_days, 'incorrect days'
assert hosts == [2582, 3222, 4190, 2502, 2537, 4106, 4406, 4317, 4523, 4346, 2864, 2650, 4454, 4214, 4340, 4385, 4168, 2550, 2560, 4134, 4456], 'incorrect hosts'

fig = plt.figure(figsize=(12, 6), facecolor='white', edgecolor='white')
plt.axis([min(daysWithHosts), max(daysWithHosts), 0, max(hosts)+500])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Day')
plt.ylabel('Hosts')
plt.plot(daysWithHosts, hosts)
plt.show(block=False)
plt.pause(5)
plt.close()

dayAndHostTuple =  access_logs.map(lambda s : (s.date_time.day,s.host))
groupedByDay = dayAndHostTuple.groupByKey()
sortedByDay = groupedByDay.sortByKey().map(lambda x:(x[0], int(len(x[1])/len(set(x[1])))))
avgDailyReqPerHost = (sortedByDay
                      .cache())
avgDailyReqPerHostList = avgDailyReqPerHost.take(30)

print('Average number of daily requests per Hosts is %s' % avgDailyReqPerHostList)

assert avgDailyReqPerHostList == [(1, 13), (3, 12), (4, 14), (5, 12), (6, 12), (7, 13), (8, 13), (9, 14), (10, 13), (11, 14), (12, 13), (13, 13), (14, 13), (15, 13), (16, 13), (17, 13), (18, 13), (19, 12), (20, 12), (21, 13), (22, 12)], 'incorrect avgDailyReqPerHostList'

assert avgDailyReqPerHost.is_cached, 'incorrect avgDailyReqPerHost.is_cache'

badRecords = (access_logs
              .filter(lambda s: s.response_code == 404).cache())

print('Found %d 404 URLs' % badRecords.count())

assert badRecords.count() == 6185, 'incorrect badRecords.count()'
assert badRecords.is_cached, 'incorrect badRecords.is_cached'

badEndpoints = badRecords.map(lambda s:s.endpoint)
badUniqueEndpoints = badEndpoints.distinct()
badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)

print('404 URLS: %s' % badUniqueEndpointsPick40)

badEndpointsCountPairTuple = badRecords.map(lambda s:(s.endpoint,1))
badEndpointsSum = badEndpointsCountPairTuple.reduceByKey(lambda a,b: a + b)
badEndpointsTop20 = badEndpointsSum.takeOrdered(20,lambda s: -1 * s[1])

print('Top Twenty 404 URLs: %s' % badEndpointsTop20)

assert badEndpointsTop20 == [(u'/pub/winvn/readme.txt', 633), (u'/pub/winvn/release.txt', 494), (u'/shuttle/missions/STS-69/mission-STS-69.html', 431), (u'/images/nasa-logo.gif', 319), (u'/elv/DELTA/uncons.htm', 178), (u'/shuttle/missions/sts-68/ksc-upclose.gif', 156), (u'/history/apollo/sa-1/sa-1-patch-small.gif', 146), (u'/images/crawlerway-logo.gif', 120), (u'/://spacelink.msfc.nasa.gov', 117), (u'/history/apollo/pad-abort-test-1/pad-abort-test-1-patch-small.gif', 100), (u'/history/apollo/a-001/a-001-patch-small.gif', 97), (u'/images/Nasa-logo.gif', 85), (u'/shuttle/resources/orbiters/atlantis.gif', 64), (u'/history/apollo/images/little-joe.jpg', 62), (u'/images/lf-logo.gif', 59), (u'/shuttle/resources/orbiters/discovery.gif', 56), (u'/shuttle/resources/orbiters/challenger.gif', 54), (u'/robots.txt', 53), (u'/elv/new01.gif>', 43), (u'/history/apollo/pad-abort-test-2/pad-abort-test-2-patch-small.gif', 38)], 'incorrect badEndpointsTop20'

errHostsCountPairTuple = badRecords.map(lambda s: (s.host,1))
errHostsSum = errHostsCountPairTuple.reduceByKey(lambda a,b : a + b)
errHostsTop25 = errHostsSum.takeOrdered(25, lambda x: -1 * x[1])

print('Top 25 hosts that generated errors: %s' % errHostsTop25)

assert len(errHostsTop25) == 25, 'length of errHostsTop25 is not 25'
assert len(set(errHostsTop25) - set([(u'maz3.maz.net', 39), (u'piweba3y.prodigy.com', 39), (u'gate.barr.com', 38), (u'm38-370-9.mit.edu', 37), (u'ts8-1.westwood.ts.ucla.edu', 37), (u'nexus.mlckew.edu.au', 37), (u'204.62.245.32', 33), (u'163.206.104.34', 27), (u'spica.sci.isas.ac.jp', 27), (u'www-d4.proxy.aol.com', 26), (u'www-c4.proxy.aol.com', 25), (u'203.13.168.24', 25), (u'203.13.168.17', 25), (u'internet-gw.watson.ibm.com', 24), (u'scooter.pa-x.dec.com', 23), (u'crl5.crl.com', 23), (u'piweba5y.prodigy.com', 23), (u'onramp2-9.onr.com', 22), (u'slip145-189.ut.nl.ibm.net', 22), (u'198.40.25.102.sap2.artic.edu', 21), (u'gn2.getnet.com', 20), (u'msp1-16.nas.mr.net', 20), (u'isou24.vilspa.esa.es', 19), (u'dial055.mbnet.mb.ca', 19), (u'tigger.nashscene.com', 19)])) == 0, 'incorrect errHostsTop25'

errDateCountPairTuple = badRecords.map(lambda x:(x.date_time.day,1))
errDateSum = errDateCountPairTuple.reduceByKey( lambda a,b: a + b)
errDateSorted = (errDateSum.sortByKey()
                 .cache())
errByDate = errDateSorted.collect()

print('404 Errors by day: %s' % errByDate)

assert errByDate == [(1, 243), (3, 303), (4, 346), (5, 234), (6, 372), (7, 532), (8, 381), (9, 279), (10, 314), (11, 263), (12, 195), (13, 216), (14, 287), (15, 326), (16, 258), (17, 269), (18, 255), (19, 207), (20, 312), (21, 305), (22, 288)], 'incorrect errByDate'
assert errDateSorted.is_cached, 'incorrect errDateSorted.is_cached'
daysWithErrors404 = errDateSorted.map(lambda x: x[0]).collect()
errors404ByDay = errDateSorted.map(lambda x: x[1]).collect()
assert daysWithErrors404 == [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22], 'incorrect daysWithErrors404'
assert errors404ByDay == [243, 303, 346, 234, 372, 532, 381, 279, 314, 263, 195, 216, 287, 326, 258, 269, 255, 207, 312, 305, 288], 'incorrect errors404ByDay'

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, max(daysWithErrors404), 0, max(errors404ByDay)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Day')
plt.ylabel('404 Errors')
plt.plot(daysWithErrors404, errors404ByDay)
plt.show(block=False)
plt.pause(5)
plt.close()

hourCountPairTuple = badRecords.map(lambda x:(x.date_time.hour,1))
hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a,b : a + b)
hourRecordsSorted = (hourRecordsSum
                     .sortByKey().cache())
errHourList = hourRecordsSorted.collect()

print('Top hours for 404 requests: %s' % errHourList)

assert errHourList == [(0, 175), (1, 171), (2, 422), (3, 272), (4, 102), (5, 95), (6, 93), (7, 122), (8, 199), (9, 185), (10, 329), (11, 263), (12, 438), (13, 397), (14, 318), (15, 347), (16, 373), (17, 330), (18, 268), (19, 269), (20, 270), (21, 241), (22, 234), (23, 272)], 'incorrect errHourList'
assert hourRecordsSorted.is_cached, 'incorrect hourRecordsSorted.is_cached'
hoursWithErrors404 = hourRecordsSorted.map(lambda x: x[0]).collect()
errors404ByHours = hourRecordsSorted.map(lambda x:x[1]).collect()
assert hoursWithErrors404 == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23], 'incorrect hoursWithErrors404'
assert errors404ByHours == [175, 171, 422, 272, 102, 95, 93, 122, 199, 185, 329, 263, 438, 397, 318, 347, 373, 330, 268, 269, 270, 241, 234, 272], 'incorrect errors404ByHours'

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, max(hoursWithErrors404), 0, max(errors404ByHours)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Hour')
plt.ylabel('404 Errors')
plt.plot(hoursWithErrors404, errors404ByHours)
plt.show(block=False)
plt.pause(5)
plt.close()

