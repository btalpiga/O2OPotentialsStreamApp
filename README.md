# O2OPotentialsStreamApp
Kafka Stream app for calculating consumer fields related to O2O Potential (webInCnt, webOutCnt, o2oInCnt, o2oOutCnt)

# Embedded web server
url= http://10.100.1.17:7004  
changeLog: PUT /logger `{"logName": "com.nyble", "logLevel": "warn"}`


# Installation

#### check that stream app O2OPotentialsStreamApp.jar is not running

#### get the latest id from actions in database:
`update config_parameters  
 set value = q.id  
 from (select max(id) as id from consumer_actions where system_id = 1) q  
 where key = 'O2O_POTENTIALS_LAST_ACTION_ID_RMC';--lastRmcActionId`  
`update config_parameters  
 set value = q.id  
 from (select max(id) as id from consumer_actions where system_id = 2) q  
 where key = 'O2O_POTENTIALS_LAST_ACTION_ID_RRP';--lastRrpActionId`  
`cd /home/crmsudo/jobs/kafkaClients/scripts && node utility.js name=o2o-potentials replace=:lastRmcActionId^TODO replace=:lastRrpActionId^TODO`  

#### empty o2o-potentials-counts (source topic) :
`cd ~/kits/confluent-5.5.1/`  
`bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --add-config retention.ms=10`  
--wait  
`bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --delete-config retention.ms`  

#### use reset for o2o-potentials-stream streams app:
`./bin/kafka-streams-application-reset --application-id o2o-potentials-stream --input-topics  o2o-potentials-counts --bootstrap-servers 10.100.1.17:9093`

#### delete internal stream app topics (if any remained)  
`./bin/kafka-topics --bootstrap-server 10.100.1.17:9093  --delete --topic <everything starting with o2o-potentials-stream-...>`

#### load initial state
`PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse -f "/home/crmsudo/jobs/kafkaClients/scripts/initial-o2o-potentials-calc.sql"&`

#### start streams app

