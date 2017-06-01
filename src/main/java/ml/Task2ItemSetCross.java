package ml;

import org.apache.flink.api.common.functions.CrossFunction;

import java.util.ArrayList;

/**
 * Created by lhan on 17-6-1.
 */
public class Task2ItemSetCross implements CrossFunction<ItemSet, ItemSet, ItemSet> {
    @Override
    public ItemSet cross(ItemSet arg0, ItemSet arg1) throws Exception {
        ArrayList<Integer> items = arg0.items;

        // only add new items
        for (Integer item : arg1.items) {
            if (!items.contains(item)) {
                items.add(item);
            }
        }

        // create a new ItemSet
        ItemSet newItemSet = new ItemSet(items);
        // set a temporary number of transactions
        newItemSet.setNumberOfTransactions(arg0.getNumberOfTransactions());

        return newItemSet;
    }
}
