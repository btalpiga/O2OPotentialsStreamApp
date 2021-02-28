package com.nyble.processing;

import com.nyble.facades.kafkaConsumer.RecordProcessor;
import com.nyble.main.ActionsDict;
import com.nyble.main.App;
import com.nyble.topics.Names;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.types.ConsumerActionDescriptor;
import com.nyble.types.ConsumerActionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ActionProcessor implements RecordProcessor<String, String> {

    private final static Logger logger = LoggerFactory.getLogger(ActionProcessor.class);

    @Override
    public boolean process(ConsumerRecord<String, String> consumerRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean processBatch(ConsumerRecords<String, String> consumerRecords) {
        Map<ConsumerActionType, Integer> increments = new HashMap<>();
        final String now = System.currentTimeMillis()+"";
        consumerRecords.forEach(action->{
            int externalSystemProvenience = action.topic().contains("rmc") ? Names.RMC_SYSTEM_ID : Names.RRP_SYSTEM_ID;
            ConsumerActionsValue actionValue = (ConsumerActionsValue) TopicObjectsFactory.fromJson(action.value(), ConsumerActionsValue.class);
            ConsumerActionType consumerActionType = getO2OPotentialActionType(externalSystemProvenience, actionValue);
            if(consumerActionType != null){
                if(increments.containsKey(consumerActionType)){
                    int currentCnt = increments.get(consumerActionType);
                    increments.put(consumerActionType, currentCnt+1);
                }else{
                    increments.put(consumerActionType, 1);
                }
            }
        });

        for(Map.Entry<ConsumerActionType, Integer> e : increments.entrySet()){
            int actionType = e.getKey().getActionType();
            String propertyName;
            if(actionType == ConsumerActionDescriptor.WEB_IN){
                propertyName = "webInCnt";
            }else if(actionType == ConsumerActionDescriptor.O2O_IN){
                propertyName = "o2oInCnt";
            }else{
                propertyName = "o2oOutCnt";
            }
            String incrementCount = "+"+e.getValue();
            ConsumerAttributesKey cak = new ConsumerAttributesKey(e.getKey().getSystemId(), e.getKey().getConsumerId());
            ConsumerAttributesValue cav = new ConsumerAttributesValue(e.getKey().getSystemId()+"",
                    e.getKey().getConsumerId()+"",
                    propertyName, incrementCount, now, now);
            String keyStr = cak.toJson();
            String valStr = cav.toJson();
            logger.debug("Sending {} and {} to {}", keyStr, valStr, Names.CONSUMER_ATTRIBUTES_TOPIC);
            App.producerManager.getProducer().send(new ProducerRecord<>(Names.CONSUMER_ATTRIBUTES_TOPIC, keyStr, valStr));
        }

        return true;
    }

    private ConsumerActionType getO2OPotentialActionType(int systemId, ConsumerActionsValue actionValue) {
        if( (systemId == Names.RMC_SYSTEM_ID && Integer.parseInt(actionValue.getId()) <= App.lastRmcActionId) ||
                (systemId == Names.RRP_SYSTEM_ID && Integer.parseInt(actionValue.getId()) <= App.lastRrpActionId )){
            return null;
        }

        if(ActionsDict.filter(actionValue)){
            return new ConsumerActionType(Integer.parseInt(actionValue.getConsumerId()),
                    Integer.parseInt(actionValue.getSystemId()),
                    ActionsDict.get(actionValue.getSystemId(), actionValue.getActionId()).getType());
        }else{
            return null;
        }
    }
}
