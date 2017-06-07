package ml;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.ArrayList;
import java.util.Collections;


/**
 * Created by lhan on 17-6-1.
 */
public class Task2KeySelector implements KeySelector<ItemSet, Integer> {


    @Override
    public Integer getKey(ItemSet o) throws Exception {
        String key = "";
        ArrayList<Integer> items = o.items;

        Collections.sort(items);

        for (Integer item : items) {
            key += item.toString();
            key += " ";
        }
        return new HashCodeBuilder().append(key).toHashCode();
    }
}
