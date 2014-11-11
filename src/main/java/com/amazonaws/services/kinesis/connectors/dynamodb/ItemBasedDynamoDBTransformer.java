package com.amazonaws.services.kinesis.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by soloman.weng on 11/11/14.
 */
public abstract class ItemBasedDynamoDBTransformer<T> implements DynamoDBTransformer<T> {

    @Override
    public Map<String, AttributeValue> fromClass(T record) {
        Item item = recordToItem(record);

        return convertToAttributeValueHash(item);
    }

    protected abstract Item recordToItem(T record);

    private static Map<String, AttributeValue> convertToAttributeValueHash(Item item) {
        return doConvertToAttributeValueHash(item.asMap());
    }

    private static Map<String, AttributeValue> doConvertToAttributeValueHash(Map<String, Object> payload) {
        Map<String, AttributeValue> target = new LinkedHashMap<>(payload.size());

        for (Map.Entry<String, Object> e : payload.entrySet()) {
            AttributeValue value = toAttributeValue(e.getValue());
            if (value != null) {
                target.put(e.getKey(), value);
            }
        }
        return target;
    }

    private static AttributeValue toAttributeValue(Object value) {
        if (value instanceof String && !((String)value).isEmpty()) {
            return new AttributeValue().withS((String) value);
        } else if (value instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean) value);
        } else if (value instanceof Number) {
            return new AttributeValue().withN(value.toString());
        } else if (value instanceof Map) {
            Map<String, AttributeValue> mapVal = doConvertToAttributeValueHash((Map<String, Object>) value);
            if (!mapVal.isEmpty()) {
                return new AttributeValue().withM(mapVal);
            }
        } else if (value instanceof Collection) {
            Collection<?> valCollection = (Collection) value;
            Collection<AttributeValue> targetCollection = new ArrayList<>(valCollection.size());
            for (Object val : valCollection) {
                AttributeValue attrVal = toAttributeValue(val);
                if (attrVal != null) {
                    targetCollection.add(toAttributeValue(val));
                }
            }
            return new AttributeValue().withL(targetCollection);
        }
        return null;
    }

}
