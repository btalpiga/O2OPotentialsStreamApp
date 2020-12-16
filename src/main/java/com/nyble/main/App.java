package com.nyble.main;

import com.google.gson.Gson;
import com.nyble.exceptions.RuntimeSqlException;
import com.nyble.topics.Names;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.types.ConsumerActionDescriptor;
import com.nyble.types.ConsumerActionType;
import com.nyble.util.DBUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

@SpringBootApplication(scanBasePackages = {"com.nyble.rest"})
public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static Logger logger = LoggerFactory.getLogger(App.class);
    final static String appName = "o2o-potentials-stream";
    final static Properties streamsConfig = new Properties();
    final static Properties producerProps = new Properties();
    static KafkaProducer<String, Integer> producer;
    static{
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        streamsConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        producerProps.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProps.put("acks", "all");
        producerProps.put("retries", 5);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", IntegerSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producer.flush();
            producer.close();
        }));
    }

    public static void initSourceTopic(List<String> topics) throws ExecutionException, InterruptedException {
        //this values should be updated on redeployment, if actions are reloaded
        int lastRmcActionId;
        int lastRrpActionId;
        final String configName = "O2O_POTENTIALS_LAST_ACTION_ID_%";
        final String lastActIdsQ = String.format("select vals[1]::int as rmc, vals[2]::int as rrp from\n" +
                "(\tselect array_agg(value order by key) as vals \n" +
                "\tfrom config_parameters cp \n" +
                "\twhere key like '%s'\n" +
                ") foo", configName);
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(lastActIdsQ)){
            if(rs.next()){
                lastRmcActionId = rs.getInt("rmc");
                lastRrpActionId = rs.getInt("rrp");
            }else{
                throw new RuntimeException("Last action ids not set");
            }

        } catch (SQLException e) {
            throw new RuntimeSqlException(e.getMessage(), e);
        }

        //
        final String consumerGroupId = "o2o-potentials-source-creator";
        final Gson gson = new Gson();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000*60);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        AdminClient adminClient = AdminClient.create(adminProps);
        Set<String> existingTopics = adminClient.listTopics().names().get();
        for(String checkTopic : topics){
            if(!existingTopics.contains(checkTopic)){
                int numPartitions = 4;
                short replFact = 2;
                NewTopic st = new NewTopic(checkTopic, numPartitions, replFact).configs(new HashMap<>());
                adminClient.createTopics(Collections.singleton(st));
            }
        }

        String sourceTopic = topics.get(0);
        Thread poolActionsThread = new Thread(()->{
            try{
                KafkaConsumer<String, String> kConsumer = new KafkaConsumer<>(consumerProps);
                kConsumer.subscribe(Arrays.asList(Names.CONSUMER_ACTIONS_RMC_TOPIC, Names.CONSUMER_ACTIONS_RRP_TOPIC));
                while(true){
                    ConsumerRecords<String, String> records = kConsumer.poll(Duration.ofSeconds(10));
                    records.forEach(record->{
                        String provenienceTopic = record.topic();
                        int lastAction;
                        if(provenienceTopic.endsWith("rmc")){
                            lastAction = lastRmcActionId;
                        } else if(provenienceTopic.endsWith("rrp")){
                            lastAction = lastRrpActionId;
                        } else {
                            return;
                        }

                        //filter actions
                        ConsumerActionsValue cav;
                        try{
                            cav = (ConsumerActionsValue) TopicObjectsFactory
                                    .fromJson(record.value(), ConsumerActionsValue.class);
                        }catch (Exception exp){
                            logger.error("Last corrupt record key={}, value={}",record.key(), record.value());
                            throw exp;
                        }
                        if(Integer.parseInt(cav.getId()) > lastAction && ActionsDict.filter(cav)){
                            ConsumerActionDescriptor cad = ActionsDict.get(cav.getSystemId(), cav.getActionId());
                            ConsumerActionType cat = new ConsumerActionType(Integer.parseInt(cav.getConsumerId()),
                                    Integer.parseInt(cav.getSystemId()), cad.getType());
                            producer.send(new ProducerRecord<>(sourceTopic, gson.toJson(cat), 1));
                            logger.debug("Create {} topic input",sourceTopic);
                        }
                    });
                }
            }catch (Exception e){
                logger.error(e.getMessage(), e);
                logger.error("Stopping source thread creator O2OPotentials...");
            }
        });
        poolActionsThread.setDaemon(true);
        poolActionsThread.start();
    }

    public static ScheduledExecutorService scheduleBatchUpdate(String intermediateTopic){
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final Runnable task = ()->{
            try {
                logger.info("Start update counts");
                updateCounts(intermediateTopic);
                logger.info("End update counts");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        };
        Duration delay = Duration.between(Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS));
        scheduler.scheduleAtFixedRate(task, delay.toMillis(), Duration.ofHours(1).toMillis(), TimeUnit.MILLISECONDS);
        logger.info("Task will start in: "+delay.toMillis()+" millis");

        Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));
        return scheduler;
    }

    public static void main(String[] args)  {
        ConfigurableApplicationContext restApp = null;
        ExecutorService scheduler = null;
        final String sourceTopic = "o2o-potentials-counts";
        try{
            restApp = SpringApplication.run(App.class,args);
            initSourceTopic(Collections.singletonList(sourceTopic));
            scheduler = scheduleBatchUpdate(sourceTopic);

            final Gson gson = new Gson();
            final StreamsBuilder builder = new StreamsBuilder();

            builder.stream(sourceTopic,Consumed.with(Serdes.String(), Serdes.Integer()))
                    .groupBy((catStr, cnt)->catStr, Grouped.with(Serdes.String(), Serdes.Integer()))
                    .reduce(Integer::sum)
                    .toStream()
                    .map( (catStr, actionCounts) -> {
                        ConsumerActionType cat = gson.fromJson(catStr, ConsumerActionType.class);
                        String key;
                        if(cat.getActionType() == ConsumerActionDescriptor.WEB_IN){
                            key = "webInCnt";
                        }else if(cat.getActionType() == ConsumerActionDescriptor.O2O_IN){
                            key = "o2oInCnt";
                        }else{
                            key = "o2oOutCnt";
                        }
                        ConsumerAttributesKey cak = new ConsumerAttributesKey(cat.getSystemId(), cat.getConsumerId());
                        ConsumerAttributesValue cav = new ConsumerAttributesValue(cat.getSystemId()+"", cat.getConsumerId()+"",
                                key, actionCounts+"", new Date().getTime()+"", new Date().getTime()+"");
                        return KeyValue.pair(cak.toJson(), cav.toJson());
                    })
                    .to(Names.CONSUMER_ATTRIBUTES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

            Topology topology = builder.build();
            logger.debug(topology.describe().toString());
            KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
            streams.cleanUp();
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }catch(Exception e){
            if(restApp != null){restApp.close();}
            if(scheduler != null){scheduler.shutdown();}
            logger.error(e.getMessage(), e);
            logger.error("EXITING");
            System.exit(1);
        }

    }

    public static void updateCounts(String intermediateTopic) throws Exception {
        Gson gson = new Gson();
        Map<ConsumerActionType, Integer> decrements = new HashMap<>();
        final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        final String startDate = getLastDate();
        Calendar now = new GregorianCalendar();
        now.add(Calendar.YEAR, -2);
        now.set(Calendar.MILLISECOND, 0);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.MINUTE, 0);
        final String endDate = sdf.format(now.getTime());

        GregorianCalendar g = new GregorianCalendar();
        g.setTime(sdf.parse(startDate));
        g.add(Calendar.YEAR, 1);
        final String startDateShiftOneY = sdf.format(g.getTime());
        g = new GregorianCalendar();
        g.setTime(sdf.parse(endDate));
        g.add(Calendar.YEAR, 1);
        final String endDateShiftOneY = sdf.format(g.getTime());

        logger.info("Remove actions from {} to {}", startDate, endDate);

        String query = "select system_id, consumer_id, action_type, total from (\n" +
                "\tselect system_id, consumer_id,\n" +
                "\tcase when action_id in (:web_in_act_rmc) and system_id = "+Names.RMC_SYSTEM_ID+" then 1 \n" +
                "\t\t when action_id in (:web_in_act_rrp) and system_id = "+Names.RRP_SYSTEM_ID+" then 1 \n" +
                "\t\t when action_id in (:o2o_in_act_rmc) and system_id = "+Names.RMC_SYSTEM_ID+" then 2 \n" +
                "\t\t else 0\n" +
                "\tend as action_type, count(*) as total\n" +
                "\tfrom consumer_actions \n" +
                "\twhere external_system_date >= '"+startDate+"' and external_system_date < '"+endDate+"'\n" +
                "\tgroup by system_id, consumer_id, action_type\n" +
                "\tunion \n" +
                "\tselect system_id, consumer_id , 3 as action_type, count(*) as total\n" +
                "\tfrom consumer_actions\n" +
                "\twhere external_system_date >= '"+startDateShiftOneY+"' and external_system_date < '"+endDateShiftOneY+"' \n" +
                "\tand ( (action_id in (:o2o_out_act_rmc) and system_id = 1) )\n" +
                "\tgroup by system_id, consumer_id\n" +
                ") foo where action_type > 0";

        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(tokenizeQuery(query))){
            st.setFetchSize(1000);
            while(rs.next()){
                int systemId = rs.getInt(1);
                int consumerId = rs.getInt(2);
                int actionType = rs.getInt(3);
                int cnt = rs.getInt(4);

                ConsumerActionType cat = new ConsumerActionType(consumerId, systemId, actionType);
                if(cnt != 0){
                    Integer actionsCount = decrements.get(cat);
                    if(actionsCount != null){
                        decrements.put(cat, actionsCount+cnt);
                    }else{
                        decrements.put(cat, cnt);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        for(Map.Entry<ConsumerActionType, Integer> e : decrements.entrySet()){
            String keyStr = gson.toJson(e.getKey());
            Integer valStr = e.getValue()*-1;
            logger.debug("Sending {} and {} to {}", keyStr, valStr, intermediateTopic);
            producer.send(new ProducerRecord<>(intermediateTopic, keyStr, valStr));
        }
        updateLastDate(endDate);
    }

    private static String tokenizeQuery(String query) {
        String[] tokensToReplace = new String[] {":web_in_act_rmc", ":web_in_act_rrp", ":o2o_in_act_rmc", ":o2o_out_act_rmc"};
        for(String token : tokensToReplace){
            String repl = "";
            if(token.equals(":web_in_act_rmc")){
                repl = ActionsDict.getActionsList(ConsumerActionDescriptor.WEB_IN, Names.RMC_SYSTEM_ID);
            }
            if(token.equals(":web_in_act_rrp")){
                repl = ActionsDict.getActionsList(ConsumerActionDescriptor.WEB_IN, Names.RRP_SYSTEM_ID);
            }

            if(token.equals(":o2o_in_act_rmc")){
                repl = ActionsDict.getActionsList(ConsumerActionDescriptor.O2O_IN, Names.RMC_SYSTEM_ID);
            }
            if(token.equals(":o2o_in_act_rrp")){
                repl = ActionsDict.getActionsList(ConsumerActionDescriptor.O2O_IN, Names.RRP_SYSTEM_ID);
            }

            if(token.equals(":o2o_out_act_rmc")){
                repl = ActionsDict.getActionsList(ConsumerActionDescriptor.O2O_OUT, Names.RMC_SYSTEM_ID);
            }
            if(token.equals(":o2o_out_act_rrp")){
                repl = ActionsDict.getActionsList(ConsumerActionDescriptor.O2O_OUT, Names.RRP_SYSTEM_ID);
            }

            query = query.replace(token, repl);
        }
        return query;
    }

    final static String potentialsLastDateKey = "O2O_POTENTIALS_DECREMENT_FROM";
    public static String getLastDate() throws Exception {

        final String query = "SELECT value from config_parameters where key = '"+potentialsLastDateKey+"'";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){

            if(rs.next()){
                return rs.getString(1);
            }else{
                throw new Exception("Parameter "+potentialsLastDateKey+" not found");
            }
        }
    }

    public static void updateLastDate(String newDate) throws SQLException {
        final String query = "UPDATE config_parameters set value = '"+newDate+"' where key = '"+potentialsLastDateKey+"'";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement() ){
            logger.info("Updating last date");
            st.executeUpdate(query);
            logger.info("Updated last date");
        }
    }
}
