package ml;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by lhan on 17-5-31.
 */
public class GeneIDFilter implements FilterFunction<Tuple3<String, Integer, Double>> {


    @Override
    public boolean filter(Tuple3<String, Integer, Double> tuple) throws Exception {
        if (tuple.f1==42&&tuple.f2>1250000){
            return true;
        }else{
            return false;
        }
    }
}
