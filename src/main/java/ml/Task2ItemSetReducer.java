package ml;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by lhan on 17-6-1.
 */
public class Task2ItemSetReducer implements ReduceFunction<ItemSet>{
    @Override
    public ItemSet reduce(ItemSet itemSet, ItemSet t1) throws Exception {
        ItemSet item = new ItemSet(itemSet.items);
        item.setNumberOfTransactions(itemSet.getNumberOfTransactions() + t1.getNumberOfTransactions());

        return item;
    }
}
