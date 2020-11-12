package com.nyble.main;

import com.nyble.topics.Names;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.types.ConsumerActionDescriptor;

import java.util.*;
import java.util.stream.Collectors;

public class ActionsDict {
    static Map<String, ConsumerActionDescriptor> actionsDict = new HashMap<>();
    static List<Integer> rmcWebInActions = new ArrayList<>();
    static List<Integer> rmcO2OInActions = new ArrayList<>();
    static List<Integer> rmcO2OOutActions = new ArrayList<>();
    static List<Integer> rrpWebInActions = new ArrayList<>();
    static List<Integer> rrpO2OInActions = new ArrayList<>();
    static List<Integer> rrpO2OOutActions = new ArrayList<>();
    static {

        actionsDict.put("1#1828", new ConsumerActionDescriptor(1828,"","1", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("1#1728", new ConsumerActionDescriptor(1728,"","1", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("1#1814", new ConsumerActionDescriptor(1814,"","1", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("1#1898", new ConsumerActionDescriptor(1898,"","1", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("1#1858", new ConsumerActionDescriptor(1858,"","1", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("1#1776", new ConsumerActionDescriptor(1776,"","1", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("1#1866", new ConsumerActionDescriptor(1866,"","1", ConsumerActionDescriptor.WEB_IN));

        actionsDict.put("1#1729", new ConsumerActionDescriptor(1729,"","1", ConsumerActionDescriptor.O2O_IN));
        actionsDict.put("1#1740", new ConsumerActionDescriptor(1740,"","1", ConsumerActionDescriptor.O2O_IN));
        actionsDict.put("1#1951", new ConsumerActionDescriptor(1951,"","1", ConsumerActionDescriptor.O2O_IN));
        actionsDict.put("1#1964", new ConsumerActionDescriptor(1964,"","1", ConsumerActionDescriptor.O2O_IN));
        actionsDict.put("1#1922", new ConsumerActionDescriptor(1922,"","1", ConsumerActionDescriptor.O2O_IN));

        actionsDict.put("1#1791", new ConsumerActionDescriptor(1791,"","1", ConsumerActionDescriptor.O2O_OUT));
        actionsDict.put("1#1920", new ConsumerActionDescriptor(1920,"","1", ConsumerActionDescriptor.O2O_OUT));

        //----------------------

        actionsDict.put("2#3069", new ConsumerActionDescriptor(3069,"","2", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("2#3064", new ConsumerActionDescriptor(3064,"","2", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("2#1898", new ConsumerActionDescriptor(1898,"","2", ConsumerActionDescriptor.WEB_IN));
        actionsDict.put("2#1775", new ConsumerActionDescriptor(1775, "Entered correct stamp code", "2", ConsumerActionDescriptor.WEB_IN));
    }

    public static boolean contains(String systemId, String actionId){
        return actionsDict.containsKey(systemId+"#"+actionId);
    }

    public static ConsumerActionDescriptor get(String systemId, String actionId){
        return actionsDict.get(systemId+"#"+actionId);
    }

    public static boolean filter(ConsumerActionsValue cav){
        String systemId = cav.getSystemId()+"";
        String actionId = cav.getActionId()+"";
        final long twoYearsDurationMillis = 365L*24*60*60*1000;
        Date actionDate = new Date(Long.parseLong(cav.getExternalSystemDate()));
        Date now = new Date();
        boolean allowedActionDate = actionDate.after(new Date(System.currentTimeMillis() - twoYearsDurationMillis)) &&
                (actionDate.before(now) || actionDate.equals(now));
        return contains(systemId, actionId) && allowedActionDate;
    }

    public static String getActionsList(int actionType, int systemId) {
        List<Integer> collection = null;
        if(systemId == Names.RMC_SYSTEM_ID){
            if(actionType == ConsumerActionDescriptor.WEB_IN){
                collection = rmcWebInActions;
            }
            if(actionType == ConsumerActionDescriptor.O2O_IN){
                collection = rmcO2OInActions;
            }
            if(actionType == ConsumerActionDescriptor.O2O_OUT){
                collection = rmcO2OOutActions;
            }
        }else{
            if(actionType == ConsumerActionDescriptor.WEB_IN){
                collection = rrpWebInActions;
            }
            if(actionType == ConsumerActionDescriptor.O2O_IN){
                collection = rrpO2OInActions;
            }
            if(actionType == ConsumerActionDescriptor.O2O_OUT){
                collection = rrpO2OOutActions;
            }
        }

        if(collection == null){
            throw new NullPointerException("O2O Potentials action type list is null");
        }
        if(collection.isEmpty()){
            for(ConsumerActionDescriptor cad : actionsDict.values()){
                if(cad.getSystemId().equals(systemId+"") && cad.getType()==actionType){
                    collection.add(cad.getId());
                }
            }
        }
        return collection.stream().map(i->i+"").collect(Collectors.joining(","));
    }
}
