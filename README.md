# O2OPotentialsStreamApp
Kafka Stream app for calculating consumer fields related to O2O Potential (webInCnt, webOutCnt, o2oInCnt, o2oOutCnt)

# Embedded web server
url= http://10.100.1.17:7004  
changeLog: PUT /logger `{"logName": "com.nyble", "logLevel": "warn"}`


# Installation

#### check that actions rmc/rrp kafka connectors are paused and o2o-potentials consumers have lag = 0 

#### check that stream app O2OPotentialsStreamApp.jar is not running

#### get the latest id from actions in database:
`update config_parameters  
 set value = q.id  
 from (select max(id) as id from consumer_actions where system_id = 1) q  
 where key = 'O2O_POTENTIALS_LAST_ACTION_ID_RMC';--lastRmcActionId`  
 `create temp table tmp as select id from consumer_actions ca where system_id = 2;
  update config_parameters  
  set value = q.id  
  from (select max(id) as id from tmp) q  
  where key = 'O2O_POTENTIALS_LAST_ACTION_ID_RRP';--lastRmcActionId  
  drop table tmp;`  
`cd /home/crmsudo/jobs/kafkaClients/scripts && node utility.js --get-script name=o2o-potentials replace=:lastRmcActionId^TODO replace=:lastRrpActionId^TODO`  

#### empty o2o-potentials-counts (source topic) :  
`~/kits/confluent-5.5.1/bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --add-config retention.ms=10`  
--wait  
`~/kits/confluent-5.5.1/bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --delete-config retention.ms`  

#### use reset for o2o-potentials-stream streams app:
`~/kits/confluent-5.5.1/bin/kafka-streams-application-reset --application-id o2o-potentials-stream --input-topics  o2o-potentials-counts --bootstrap-servers 10.100.1.17:9093`

#### delete internal stream app topics (if any remained)  
`~/kits/confluent-5.5.1/bin/kafka-topics --bootstrap-server 10.100.1.17:9093  --delete --topic <everything starting with o2o-potentials-stream-...>`

#### load initial state
`PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse -f "/home/crmsudo/jobs/kafkaClients/scripts/initial-o2o-potentials-calc.sql"&`
`PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse -c "\copy (\
select replace(json_build_object('systemId', system_id, 'consumerId', consumer_id, 'actionType', action_type)::text,' :',':'),\
total from o2o_potentials_start\
) to '/tmp/o2o-potentials-source.csv' delimiter ';'" &`  
`cd /home/crmsudo/jobs/kafkaClients/scripts/kafkaToolsJava`
`./kafkaTools.sh producer --topic o2o-potentials-counts --bootstrap-server 10.100.1.17:9093 --value-serializer Integer --key-serializer String --format key-value --key-value-delimiter ";" --file /tmp/o2o-potentials-source.csv`

#### start streams app

