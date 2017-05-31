package ml;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Created by lhan on 17-6-1.
 */
public class OnePartitioner implements Partitioner<String> {
    @Override
    public int partition(String s, int i) {
        return 0;
    }
}
