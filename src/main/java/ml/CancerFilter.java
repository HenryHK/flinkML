package ml;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by lhan on 17-5-31.
 */
public class CancerFilter implements FilterFunction<Tuple2<String, String>> {

    @Override
    public boolean filter(Tuple2<String, String> tuple) throws Exception {

        if(tuple.f1.equals("breast-cancer")
                ||tuple.f1.equals("prostate-cancer")
                ||tuple.f1.equals("pancreatic-cancer")
                ||tuple.f1.equals("leukemia")
                ||tuple.f1.equals("lymphoma")){

            return true;

        }else{
            return false;
        }
    }

}
