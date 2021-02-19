from pyspark.sql import SparkSession
from sys import argv
import re
import datetime
from pyspark.sql import Row
from pyspark.sql.functions import  *
import pandas as pd
import time

spark = SparkSession.builder.appName('SparkLogProcessor') \
    .enableHiveSupport().getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

sc = spark.sparkContext
sc.setLogLevel('ERROR')

module, fileloc, wait_before_exit = argv.copy()
print(f'module is {module} and fileloc is {fileloc} and wait_before_exit is {wait_before_exit}')
logFileRDD = sc.textFile(fileloc)
print("No of logs " , logFileRDD.count())
logFileRDD.take(2)

# make provisions for processing the logs
# use a regex to split the line into the nine groups and map it to a sql row of nine columns
month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = int(0)
    else:
        size = int(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

# set the log pattern to be used
APACHE_ACCESS_LOG_PATTERN ='^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)'

# read the log file and use the mapped tuple second part to get a count of successfully parsed
# and failed logs

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(fileloc)
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

    print('Read %d lines, successfully parsed %d lines, failed to parse %d lines' %
          (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()

access_logs_df = access_logs.toDF()
access_logs_df.show()

@pandas_udf("int")
def sum_udf(v: pd.Series) -> int:
    return v.sum()

access_logs_df.groupby('host').\
    agg(sum_udf(col('content_size')).alias('total_content_size')).\
    show()

access_logs_df.groupby('host'). \
    agg(sum_udf(col('content_size')).alias('total_content_size')). \
    orderBy(desc('total_content_size')).show()

access_logs_df.groupby('response_code', 'protocol'). \
    agg(sum_udf(col('content_size')).alias('total_content_size')). \
    orderBy(desc('total_content_size')).show()

time.sleep(int(wait_before_exit))