package ml;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by lhan on 17-6-1.
 */
public class Task2SupportFilter implements FilterFunction<ItemSet> {

    private double numOfTransactions;

    public Task2SupportFilter(double numOfTransactions) {
        this.numOfTransactions = numOfTransactions;

    }

    @Override
    public boolean filter(ItemSet itemSet) throws Exception {
        if (itemSet.getNumberOfTransactions()<numOfTransactions){
            return false;
        }else {
            return true;
        }
    }
}
