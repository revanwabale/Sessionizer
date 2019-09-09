# Sessionizer
 Sessionize the AWS Elastic Load Balancer log to enable the analytics at session level
 
 
 
 

***:-  Result set Per Quetions  -:***

######################################################### 

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics) :

output --> 'pay/sessionizer'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

2. Determine the average session time:

output --> 'pay/session_time_per_ip_per_session'

output --> 'pay/avgSessionTimePerIp'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session:

output --> 'pay/hitCnt_per_Disturl_per_session'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

4. Find the most engaged users, ie the IPs with the longest session times:

output --> 'pay/longestSessionTimePerIp'

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++



