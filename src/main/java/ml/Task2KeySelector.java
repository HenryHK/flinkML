package ml;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;


/**
 * Created by lhan on 17-6-1.
 */
public class Task2KeySelector implements KeySelector<ItemSet, String> {


    @Override
    public String getKey(ItemSet o) throws Exception {
        String key = "";
        ArrayList<Integer> items = o.items;

        Collections.sort(items);

        for (Integer item : items) {
            key += item.toString();
            key += " ";
        }
        return key;
    }
}
