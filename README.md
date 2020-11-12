# O2OPotentialsStreamApp
Kafka Stream app for calculating consumer fields related to O2O Potential (webInCnt, webOutCnt, o2oInCnt, o2oOutCnt)

# Embedded web server
url= http://10.100.1.17:7004  
changeLog: PUT /logger `{"logName": "com.nyble", "logLevel": "warn"}`


# Installation

`cd ~/kits/confluent-5.5.1/`  

#### get the latest id from actions in database:
`select max(id) from consumer_actions where system_id = 1; --lastRmcActionId`  
`select max(id) from consumer_actions where system_id = 2; --lastRrpActionId`  
--update ids in the application and build it  
--update ids in the initial-o2o-potentials-calc.sql  
`PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse -f "/home/crmsudo/jobs/kafkaClients/scripts/initial-o2o-potentials-calc.sql"&`

#### check that stream app O2OPotentialsStreamApp.jar is not running

#### empty o2o-potentials-counts (source topic) :
`bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --add-config retention.ms=10`  
--wait  
`bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --delete-config retention.ms`  

#### use reset for o2o-potentials-stream streams app:
`./bin/kafka-streams-application-reset --application-id o2o-potentials-stream --input-topics  o2o-potentials-counts --bootstrap-servers 10.100.1.17:9093`

#### delete internal stream app topics
`./bin/kafka-topics --bootstrap-server 10.100.1.17:9093  --delete --topic <everything starting with potentials-stream-...>`

#### start streams app

