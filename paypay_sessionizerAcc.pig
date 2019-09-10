SET DEFAULT_PARALLEL 10; 
-- # local  mode:
-- nohup pig -useHCatalog -x local -f paypay.pig > local_log 2>&1 &
--or
-- # Cluster tez mode:
--nohup pig -useHCatalog -x tez -f paypay.pig > tez_log 2>&1 &
----------------------------------------------------------------------------
-- # Make availble file on hdfs in cluster mode
 --fs -mkdir -p hdfs://rl-hadoop-gcp-m/paypay/
 --fs rm -r hdfs://rl-hadoop-gcp-m/paypay/2015_07_22_mktplace_shop_web_log_sample.log.gz
 --fs -copyFromLocal -f 2015_07_22_mktplace_shop_web_log_sample.log.gz hdfs://rl-hadoop-gcp-m/paypay/

----------------------------------------------------------------------------

--REGISTER '/home/revan.wabale/paypay/jars/*.jar';
REGISTER './jars/*.jar';
%declare SESSION_TIME  15m -- 15 minute into millis

%declare LOG_FILE_HDFS_PATH 'hdfs:///paypay/2015_07_22_mktplace_shop_web_log_sample.log.gz'
-- Define function for use.
--define Sessionizer pigUDF.Sessionizer(); -- 15 minute considered for session window
define Sessionizer pigUDF.SessionizerAcc('$SESSION_TIME'); -- 15 minute considered for session window
--REGISTER /usr/lib/pig/piggybank.jar;
--REGISTER /usr/lib/pig/lib/joda-time-2.9.3.jar;

DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

-- data = load './2015_07_22_mktplace_shop_web_log_sample.log.gz' using TextLoader();
--data = load '/home/revan.wabale/paypay/2015_07_22_mktplace_shop_web_log_sample.log.gz' using TextLoader();
data = load '$LOG_FILE_HDFS_PATH' using TextLoader();

--parse the text into target categories per  aws balancer schema:
--timstamp is presented in ISO format:

-- REGEX Refered FROM Aws Blog: https://aws.amazon.com/blogs/aws/access-logs-for-elastic-load-balancers/
aws_parsed_data = foreach data generate FLATTEN(REGEX_EXTRACT_ALL($0,
	'([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*):([0-9]*) ([.0-9]*) ([.0-9]*) ([.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-0-9]*) ([-0-9]*) \\"([^ ]*) ([^ ]*) (- |[^ ]*)\\".*' )
) as (Timestamp:chararray,
     ELBName :chararray,
     RequestIP :chararray,
     RequestPort:int,
     BackendIP:chararray,
     BackendPort :int,
     RequestProcessingTime : double,
     BackendProcessingTime : double,
     ClientResponseTime : double,
     ELBResponseCode :chararray,
     BackendResponseCode :chararray,
     ReceivedBytes:long,
     SentBytes : long,
     RequestVerb :chararray,
     URL :chararray,
     Protocol :chararray);

-- make sure records are distinct for calculation :
--aws_parsed_data = distinct aws_parsed_data;


--describe aws_parsed_data;
--aws_parsed_data: {Timestamp: bytearray,ELBName: bytearray,RequestIP: bytearray,RequestPort: bytearray,BackendIP: bytearray,BackendPort: bytearray,RequestProcessingTime: bytearray,BackendProcessingTime: bytearray,ClientResponseTime: bytearray,ELBResponseCode: bytearray,BackendResponseCode: bytearray,ReceivedBytes: bytearray,SentBytes: bytearray,RequestVerb: bytearray,URL: bytearray,Protocol: bytearray}

--lim_data = limit data 1 ;
--lim_aws_parsed_data = limit aws_parsed_data 1 ;
--dump lim_data;
--(2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2)
--dump lim1_aws_parsed_data;
-- (2015-07-22T09:00:28.019143Z,marketpalce-shop,123.242.248.130,54635,10.0.6.158,80,2.2E-5,0.026109,2.0E-5,200,200,0,699,GET,https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null,HTTP/1.1)

--To see the distinct ip per request:
--dist_req_ip = foreach ( group aws_parsed_data by RequestIP ) generate flatten(group) as request_ip, COUNT(aws_parsed_data) as per_ip_req_cnt;
--dist_backend_ip = foreach ( group aws_parsed_data by BackendIP ) generate flatten(group) as backend_ip, COUNT(aws_parsed_data) as per_ip_req_cnt;
-- multiple backend ip : confirmed!

-- Convert ISO ts to unix :
aws_parsed_data_tsunix_00 = foreach aws_parsed_data generate Timestamp AS Timestamp:chararray,
     ELBName AS ELBName:chararray,
     RequestIP AS RequestIP:chararray,
     RequestPort AS RequestPort:int,
     BackendIP AS BackendIP:chararray,
     BackendPort AS BackendPort:int,
     RequestProcessingTime AS RequestProcessingTime : double,
     BackendProcessingTime AS BackendProcessingTime : double,
     ClientResponseTime AS ClientResponseTime : double,
     ELBResponseCode AS ELBResponseCode :chararray,
     BackendResponseCode AS BackendResponseCode :chararray,
     ReceivedBytes AS ReceivedBytes:long,
     SentBytes AS SentBytes : long,
     RequestVerb AS RequestVerb :chararray,
     URL AS URL :chararray,
     Protocol AS Protocol :chararray, 
     ISOToUnix(Timestamp) AS unixTime:chararray;
     

-- # Clean Bad Records:
SPLIT aws_parsed_data_tsunix_00 INTO aws_parsed_data_tsunix IF (Timestamp is not null),  badrecords OTHERWISE;
-- test with limit
-- aws_parsed_data_tsunix = limit aws_parsed_data_tsunix 10000;

-- describe aws_parsed_data_tsunix;

--aws_parsed_data_tsunix: {Timestamp: chararray,ELBName: chararray,RequestIP: chararray,RequestPort: int,BackendIP: chararray,BackendPort: int,RequestProcessingTime: double,BackendProcessingTime: double,ClientResponseTime: double,ELBResponseCode: chararray,BackendResponseCode: chararray,ReceivedBytes: long,SentBytes: long,RequestVerb: chararray,URL: chararray,Protocol: chararray,unixTime: chararray}
--quit;
/*
sessions = foreach (group aws_parsed_data_tsunix by RequestIP) {
	morethantwo = filter aws_parsed_data_tsunix by COUNT(aws_parsed_data_tsunix) > 2;
	morethantwo_lim = limit morethantwo 2;
	generate aws_parsed_data_tsunix;
}

rmf sessions
store sessions into 'sessions';

{(2015-07-22T17:42:04.834900Z,marketpalce-shop,1.23.0.6,28165,10.0.4.227,80,2.4E-5,0.010813,2.1E-5,200,200,0,254,GET,https://paytm.com:443/papi/v1/promosearch/product/11148776/offers?parent_id=11148811&price=3399&channel=web&version=2,HTTP/1.1,1437586924834),
(2015-07-22T17:42:04.817634Z,marketpalce-shop,1.23.0.6,28166,10.0.4.225,80,2.4E-5,0.018603,2.2E-5,200,200,0,211,GET,https://paytm.com:443/papi/nps/merchantrating?merchant_id=20238&channel=web&version=2,HTTP/1.1,1437586924817),
(2015-07-22T17:42:04.881551Z,marketpalce-shop,1.23.0.6,28167,10.0.4.244,80,2.4E-5,0.00895,2.1E-5,200,200,0,72,GET,https://paytm.com:443/papi/rr/products/11148776/statistics?channel=web&version=2,HTTP/1.1,1437586924881)}
*/

--UDF called for sessionizer:
sessionizer = foreach (group aws_parsed_data_tsunix by RequestIP PARALLEL 10) {

--minCnt = COUNT(aws_parsed_data_tsunix);
-- morethanOne = filter aws_parsed_data_tsunix by minCnt > 1;
-- {(t1, url1, ),(t2, url1, ),(t3, url2),...etc}  <- bag 
--aws_parsed_data_ts = order aws_parsed_data_tsunix by Timestamp,unixTime DESC;  --necessary for udf to work else exception
aws_parsed_data_ts = order aws_parsed_data_tsunix by Timestamp;  --necessary for udf to work else exception

generate FLATTEN(group) as reqIP, 
--define how many session we want to define based on udf logic:
FLATTEN(Sessionizer(aws_parsed_data_ts)) AS (Timestamp:chararray,
     ELBName :chararray,
     RequestIP :chararray,
     RequestPort:int,
     BackendIP:chararray,
     BackendPort :int,
     RequestProcessingTime : double,
     BackendProcessingTime : double,
     ClientResponseTime : double,
     ELBResponseCode :chararray,
     BackendResponseCode :chararray,
     ReceivedBytes:long,
     SentBytes : long,
     RequestVerb :chararray,
     URL :chararray,
     Protocol :chararray,
     unixTime : chararray,
     sessionId : chararray);
};


--1. calculate sessionize by 15 minute: ok
rmf paypayAcc/sessionizer
store sessionizer into 'paypayAcc/sessionizer' using PigStorage('\t','-schema');


--read specific which are required further:
sessionizer = foreach sessionizer generate 
     RequestIP AS RequestIP:chararray,
     URL AS URL :chararray,
     Timestamp AS Timestamp : chararray,
     (long)unixTime AS unixTime:long,
     sessionId AS sessionId : chararray;

dist = foreach sessionizer generate RequestIP, sessionId;
dist = distinct dist;
ipDistSesions = foreach ( group dist by RequestIP PARALLEL 10 ) {

	generate FLATTEN(group) as RequestIP, COUNT(dist.sessionId) as dist_sessionId;
};


-- ## STORE statements:

rmf paypayAcc/badrecords
store badrecords into 'paypayAcc/badrecords' using PigStorage('\t','-schema');


-- find the ip and number of dist sessions
rmf paypayAcc/ipDistSesions
store ipDistSesions into 'paypayAcc/ipDistSesions' using PigStorage('\t','-schema');


morethantwo = filter ipDistSesions by dist_sessionId > 1 ;
rmf paypayAcc/morethantwo
store morethantwo into 'paypayAcc/morethantwo' using PigStorage('\t','-schema');

-- ##Metric calculation:
--2. calculate session_time : ok 
 session_time_per_ip_per_session = foreach ( group sessionizer by (RequestIP, sessionId)  ) {
 
 generate FLATTEN(group) as (RequestIP, sessionId), 
          MAX(sessionizer.unixTime) as maxunixTime, MIN(sessionizer.unixTime ) as minunixTime;

 };

session_time_per_ip_per_session = foreach session_time_per_ip_per_session generate RequestIP, sessionId, maxunixTime, minunixTime, 
																					maxunixTime - minunixTime as SessionTime;

rmf paypayAcc/session_time_per_ip_per_session
store session_time_per_ip_per_session into 'paypayAcc/session_time_per_ip_per_session' using PigStorage('\t','-schema');

--2. calculate SessionTime per reqIP: ok
avgSessionTimePerIp = foreach (group session_time_per_ip_per_session by RequestIP) {

	dist = distinct session_time_per_ip_per_session.sessionId;

	generate FLATTEN(group) as RequestIP, SUM(session_time_per_ip_per_session.SessionTime)/COUNT(dist) AS avgSessionTimePerIp;
};

rmf paypayAcc/avgSessionTimePerIp
store avgSessionTimePerIp into 'paypayAcc/avgSessionTimePerIp' using PigStorage('\t','-schema');

--3. calculate hitCnt_per_Disturl_per_session: ok.

 hitCnt_per_Disturl_per_session = foreach ( group sessionizer by (RequestIP, sessionId, URL) ) {
 
 hitCnt_per_Disturl_per_session = COUNT( sessionizer );

 generate FLATTEN(group) as (RequestIP, sessionId, URL), hitCnt_per_Disturl_per_session as hitCnt_per_Disturl_per_session;

 };

rmf paypayAcc/hitCnt_per_Disturl_per_session
store hitCnt_per_Disturl_per_session into 'paypayAcc/hitCnt_per_Disturl_per_session' using PigStorage('\t','-schema');

 --4. Find the most engaged users, ie the IPs with the longest session times: ok

 longestSessionTimePerIp = foreach ( group session_time_per_ip_per_session by RequestIP ) {
  generate FLATTEN(group) as RequestIP, MAX(session_time_per_ip_per_session.SessionTime) as longestSessionTimePerIp;
 };

 rmf paypayAcc/longestSessionTimePerIp
 store longestSessionTimePerIp into 'paypayAcc/longestSessionTimePerIp' using PigStorage('\t','-schema');












