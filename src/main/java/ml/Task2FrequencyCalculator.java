package ml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Created by lhan on 17-6-2.
 */
public class Task2FrequencyCalculator extends RichMapFunction<ItemSet, ItemSet>{

    private Collection<Tuple2<Integer, ArrayList<Integer>>> transactions;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.transactions = getRuntimeContext().getBroadcastVariable("transactions");
    }

    @Override
    public ItemSet map(ItemSet itemSet) throws Exception {
        ItemSet out = new ItemSet(itemSet.items);
        int numberOfTransactions = 0;

        for (Tuple2<Integer, ArrayList<Integer>> transaction : this.transactions) {
            if (transaction.f1.containsAll(itemSet.items)) {
                numberOfTransactions++;
            }
        }

        out.setNumberOfTransactions(numberOfTransactions);
        return out;
    }
}
