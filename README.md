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

java class name for junit cases for udf  : package name =  pigUDFTest.SessionizerTest.java  

java class name for sessionizer udf  : package name =  pigUDF.SessionizerAcc.java

necessary jar files for pig & Java dependencies :  direcory name = jars

pig file name : paypay_sessionizerAcc.pig

> run in local mode:
nohup pig -useHCatalog -x local -f paypay_sessionizerAcc.pig > paypay_sessionizerAcc_local_log 2>&1 &

> run in tez cluster mode:
nohup pig -useHCatalog -x tez -f paypay_sessionizerAcc.pig > paypay_sessionizerAcc_cluster_log 2>&1 &


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
