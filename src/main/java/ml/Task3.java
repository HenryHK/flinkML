package ml;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.commons.math3.util.Combinations;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by lhan on 17-6-9.
 */
public class Task3 {


    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();

        String inputFile = "hdfs:///user/lhan9852/assignment3/"+params.getRequired("dir")+"/task2";
        String outputDir = "hdfs:///user/lhan9852/assignment3/"+params.getRequired("dir")+"/task3";
        float threshold = Float.parseFloat(params.getRequired("threshold"));

        //import the result of task2
        DataSet<Tuple2<Integer, ArrayList<Integer>>> task2 = env.readTextFile(inputFile)
                .map(line->{
                    String[] nums = line.split("\t");
                    int supports = Integer.parseInt(nums[0]);
                    //Tuple2<Integer, ArrayList<Integer>> tuple = null;
                    ArrayList<Integer> list = new ArrayList<>();
                    for (int i=1; i<nums.length; i++){
                        list.add(Integer.parseInt(nums[i]));
                    }
                    return new Tuple2<Integer, ArrayList<Integer>>(supports, list);
                });

        //get sets with size bigger than 2
        DataSet<Tuple2<Integer, ArrayList<Integer>>> bigSets = task2
                .filter(tuple ->{
                    if(tuple.f1.size()>=2){
                        return true;
                    }else{
                        return false;
                    }
                });
        System.out.println("______________________________________");
        System.out.println(task2.count());
        System.out.println("______________________________________");
        System.out.println(bigSets.count());

        DataSet<Tuple3<ArrayList<Integer>, ArrayList<Integer>, Float>> result =
                bigSets.flatMap(new RichFlatMapFunction<Tuple2<Integer,ArrayList<Integer>>, Tuple3<ArrayList<Integer>, ArrayList<Integer>, Float>>() {

                    private Collection<Tuple2<Integer, ArrayList<Integer>>> task2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.task2 = getRuntimeContext().getBroadcastVariable("task2");
                    }

                    @Override
                    public void flatMap(Tuple2<Integer, ArrayList<Integer>> tuple, Collector<Tuple3<ArrayList<Integer>, ArrayList<Integer>, Float>> out) throws Exception {

                            Combinations combinations = new Combinations(tuple.f1.size(), tuple.f1.size()-1);
                            Iterator<int[]> itr = combinations.iterator();
                            while(itr.hasNext()){
                                ArrayList<Integer> resultList = new ArrayList<>();
                                int[] list = itr.next();
                                for(int i=0; i<list.length; i++){
                                    resultList.add(tuple.f1.get(list[i]));
                                }
                                int freq = 0;
                                for(Tuple2<Integer, ArrayList<Integer>> task2Tuple : task2){
                                    if(task2Tuple.f1.size()==resultList.size()&&task2Tuple.f1.containsAll(resultList)){
                                        freq = task2Tuple.f0;
                                    }
                                }

                                ArrayList<Integer> SR = new ArrayList(tuple.f1);
                                SR.removeAll(resultList);

                                out.collect(new Tuple3<>(resultList, SR, ((float)tuple.f0/(float)freq)));


                            }

                    }
                }).withBroadcastSet(task2, "task2")
                ;
        System.out.println("-------------result-----------------------");
        result.print();

        result = result.filter(arrayListArrayListFloatTuple3 -> arrayListArrayListFloatTuple3.f2>threshold);

        DataSet<String> output = result
                .map(tuple3 -> {
                    StringBuilder str = new StringBuilder();
                    for (int i : tuple3.f0){
                        str.append(i);
                        str.append(" ");
                    }
                    str.deleteCharAt(str.length()-1);
                    str.append("\t");
                    for(int i : tuple3.f1){
                        str.append(i);
                        str.append(" ");
                    }
                    str.deleteCharAt(str.length()-1);
                    str.append("\t");
                    str.append(tuple3.f2);
                    return str.toString();
                });

        output.print();
        output.writeAsText(outputDir);
        env.execute();

    }

}

