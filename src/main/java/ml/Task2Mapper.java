package ml;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by lhan on 17-6-2.
 */
public class Task2Mapper implements MapFunction<Tuple2<String, Integer>, ItemSet>{
    @Override
    public ItemSet map(Tuple2<String, Integer> integerIntegerTuple2) throws Exception {
        return new ItemSet(integerIntegerTuple2.f1);
    }
}
