# Sessionizer
 Sessionize the AWS Elastic Load Balancer log to enable the analytics at session level
 
 
 
 

***:-  Result set Per Quetions  -:***

######################################################### 

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics) :

output --> 'paypayAcc/sessionizer'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

2. Determine the average session time:

output --> 'paypayAcc/session_time_per_ip_per_session'

output --> 'paypayAcc/avgSessionTimePerIp'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session:

output --> 'paypayAcc/hitCnt_per_Disturl_per_session'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

4. Find the most engaged users, ie the IPs with the longest session times:

output --> 'paypayAcc/longestSessionTimePerIp'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Codebase illustration :

Java Project name : pigUDF

Java class name for junit cases for udf  : package name =  pigUDFTest.SessionizerTest.java  

Java class name for sessionizer udf  : package name =  pigUDF.SessionizerAcc.java

Necessary jar files for pig & Java dependencies :  direcory name = jars

Pig file name : paypay_sessionizerAcc.pig

## To Run The Script, Do the following preparation:
hadoop fs -mkdir -p hdfs:///paypay/
hadoop fs -put 2015_07_22_mktplace_shop_web_log_sample.log.gz hdfs:///paypay/

# Modify the Variable in Pig script for hdfs file path of WebLog:
LOG_FILE_HDFS_PATH='hdfs:///paypay/2015_07_22_mktplace_shop_web_log_sample.log.gz'

# run in local mode:
nohup pig -x local -f paypay_sessionizerAcc.pig > paypay_sessionizerAcc_local_log 2>&1 &

# run in tez cluster mode:
nohup pig -x tez -f paypay_sessionizerAcc.pig > paypay_sessionizerAcc_cluster_log 2>&1 &


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Log File generated for 10,000 records only --> paypay_sessionizerAcc_cluster_10000RecordsOnly_log
Log File generated for all records  --> paypay_sessionizerAcc_cluster_log

# Note: directory -> paypayAcc, only contains the sample output generated for 10,000 records due to github file size issue.



