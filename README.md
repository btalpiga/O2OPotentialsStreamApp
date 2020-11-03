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

#### check that kafka connector jdbc_source_o2o_potentials_start does not exist OR pause and delete it
#### reset kafka connector jdbc_source_o2o_potentials_start offset
`./bin/kafka-console-producer --bootstrap-server 10.100.1.17:9093 --topic connect-consumer-action-offsets --property "parse.key=true" --property "key.separator=;"`  
`["jdbc_source_o2o_potentials_start",{"query":"query"}];{"incrementing":0}`

#### empty o2o-potentials-counts (source topic) :
`bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --add-config retention.ms=10`  
--wait
`bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name o2o-potentials-counts --delete-config retention.ms`  

#### use reset for o2o-potentials-stream streams app:
`./bin/kafka-streams-application-reset --application-id o2o-potentials-stream --input-topics  o2o-potentials-counts --bootstrap-servers 10.100.1.17:9093`

#### delete internal stream app topics
`./bin/kafka-topics --bootstrap-server 10.100.1.17:9093  --delete --topic <everything starting with potentials-stream-...>`

#### update consumers table and set all web and o2o in/out counts to 0
`update consumers set payload = payload-'webInCnt'-'o2oInCnt'-'o2oOutCnt';`

#### start streams app

--create kafka connector jdbc_source_o2o_potentials_start and wait until full table is loaded  
--pause and delete kafka connector jdbc_source_o2o_potentials_start  

