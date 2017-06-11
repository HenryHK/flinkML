package ml;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by lhan on 17-6-9.
 */
public class Task2CrossGenerator extends RichFlatMapFunction<ItemSet, ItemSet> {

    private Collection<ItemSet> input;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.input = getRuntimeContext().getBroadcastVariable("input");
    }

    @Override
    public void flatMap(ItemSet itemSet, Collector out) throws Exception {

        ArrayList<Integer> items = new ArrayList<>(itemSet.items);
        for (ItemSet itemSet1 : input){

            for(Integer item : itemSet1.items){
                if(!items.contains(item)){
                    items.add(item);
                }
            }

            ItemSet newItemSet = new ItemSet(items);
            out.collect(newItemSet);
        }

    }
}
