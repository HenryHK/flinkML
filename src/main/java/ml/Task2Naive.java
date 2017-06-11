package ml;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import scala.Int;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by lhan on 17-6-1.
 */
public class Task2Naive {

    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();

        int iterations = Integer.parseInt(params.getRequired("set-size"));

        String geoDir = "hdfs:///share/genedata/"+params.getRequired("dir")+"/";
        String outputDir = "hdfs:///user/lhan9852/assignment3/"+params.getRequired("dir")+"/";

        // we get (id, geneids) with
        // all genes having expression_value > 1250000
        DataSet<Tuple2<String, ArrayList<Integer>>> geoData =
                env.readTextFile(geoDir+"GEO.txt")
                        .map(line->{
                            String[] values = line.split(",");
                            if(values[0].trim().equals("patientid")){
                                return new Tuple3<String, Integer, Double>("patientid", -1, 0d);
                            }else{
                                return new Tuple3<String, Integer, Double>(values[0].trim(),Integer.parseInt(values[1].trim()),Double.parseDouble(values[2].trim()));
                            }
                        })
                        .filter(new Task2GeneFilter())
                        .groupBy(0)
                        .reduceGroup((tuples, out)->{
                            String id="";
                            ArrayList<Integer> geneids = new ArrayList<Integer>();

                            for (Tuple3<String, Integer, Double> tuple:tuples){
                                id = tuple.f0;
                                geneids.add(tuple.f1);
                            }

                            out.collect(new Tuple2<String, ArrayList<Integer>>(id, geneids));
                        });

        //get (patientid, cancer-type)
        DataSet<Tuple2<String, Integer>> patientData =
                env.readTextFile(geoDir+"PatientMetaData.txt")
                        .map(line->{
                            String[] values = line.split(",");
                            if(values.length==6&&!values[0].equals("id")){
                                String[] diseases = values[4].split("\\s+");
                                List diseasesArray = Arrays.asList(diseases);
                                if(diseasesArray.contains("breast-cancer")
                                        ||diseasesArray.contains("prostate-cancer")
                                        ||diseasesArray.contains("pancreatic-cancer")
                                        ||diseasesArray.contains("leukemia")
                                        ||diseasesArray.contains("lymphoma")){

                                    return new Tuple2<String, Integer>(values[0], 1);

                                }else{
                                    return new Tuple2<String, Integer>(values[0], 0);
                                }
                            }
                            else{
                                return new Tuple2<String, Integer>(values[0], 0);
                            }
                        })
                        .filter(tuple -> {
                            if(tuple.f1==0){
                                return false;
                            }else {
                                return true;
                            }
                        });

        // (patientid, [geneid-1, geneid-2, geneid-3, ...])
        // resultData is transaction
        DataSet<Tuple2<String, ArrayList<Integer>>> resultData =
                patientData
                        .join(geoData)
                        .where(0)
                        .equalTo(0)
                        .projectFirst(0)
                        .projectSecond(1);

        double coefficient = Double.parseDouble(params.getRequired("coefficient"));
        long total = resultData.count();
        double min_support = total*coefficient;

        DataSet<Tuple2<String, Integer>> input =
                resultData.flatMap((tuple, out)->{
                    for(int geneid : tuple.f1){
                        out.collect(new Tuple2<String, Integer>(tuple.f0, geneid));
                    }
                });


        //input.first(10).print();
        //KeySelector<ItemSet, String> selector = new Task2KeySelector();

        //get the itemset with size=1
        DataSet<Tuple2<List<Integer>, Integer>> initial = input
                .map(stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2.f1, 1))
                .groupBy(0)
                .reduceGroup((tuples, out)->{
                    int id = -1;
                    int count = 0;
                    for (Tuple2<Integer, Integer> tuple : tuples){
                        id = tuple.f0;
                        count += tuple.f1;
                    }
                    LinkedList<Integer> list = new LinkedList<>();
                    list.add(id);
                    if(count>= min_support){
                        out.collect(new Tuple2<>(list, count));
                    }
                });

        DataSet<Tuple2<List<Integer>, Integer>> baskets = resultData
                .map(tuple -> {
                    LinkedList<Integer> list = new LinkedList<>();
                    for (int i : tuple.f1){
                        list.add(i);
                    }
                    return new Tuple2<>(list, list.size());
                });


        DeltaIteration<Tuple2<List<Integer>, Integer>, Tuple2<List<Integer>, Integer>> iteration = initial.iterateDelta(initial, iterations, 1);

        // Candidate Set: (List<Item ID>, k)
        DataSet<Tuple2<List<Integer>, Integer>> candidateSet = aprioriGen(iteration.getWorkset());

        // reduced: (null, k)
        DataSet<Tuple2<List<Integer>, Integer>> reduced = candidateSet.reduce((value1, value2) -> new Tuple2<List<Integer>, Integer>(null, value1.f1));
        // Min support subsets: (List<Item ID>, k)
        DataSet<Tuple2<String, Integer>> minSupportSubsets = baskets.join(reduced).where(tuple -> true).equalTo(tuple -> true).with((tuple1, tuple2) -> new Tuple2<List<Integer>, Integer>(tuple1.f0, tuple2.f1)).flatMap((FlatMapFunction<Tuple2<List<Integer>, Integer>, Tuple2<String, Integer>>) (tuple, out) -> {
            int k = tuple.f1;
            List<Integer> list = new LinkedList<Integer>(tuple.f0);
            if (list.size() >= k) {
                for (int i = 0; i <= k - 2; i++) {
                    list.remove(i);
                    out.collect(new Tuple2<String, Integer>(list.toString(), 1));
                }
            }
        }).groupBy(0).sum(1).filter(tuple -> tuple.f1 >= min_support);

        // Delta (k frequent itemsets): (List<Item ID>, k)
        DataSet<Tuple2<List<Integer>, Integer>> delta = candidateSet.join(minSupportSubsets).where(tuple -> tuple.f0.toString()).equalTo(0).projectFirst(0, 1);

        iteration.closeWith(delta, delta).print();
        iteration.getWorkset().print();
        iteration.getSolutionSet().print();

        // execute program
        //env.execute("Team PSD Übung 3");


    }


    private static DataSet<Tuple2<List<Integer>, Integer>> aprioriGen(DataSet<Tuple2<List<Integer>, Integer>> freqKItemSets) {
        // Join Step
        DataSet<Tuple2<List<Integer>, Integer>> joinedSet = freqKItemSets.join(freqKItemSets).where((tuple) -> {
            List<Integer> list = new LinkedList<Integer>(tuple.f0);
            list.remove(list.size() - 1);
            return list.toString();
        }).equalTo((tuple) -> {
            List<Integer> list = new LinkedList<Integer>(tuple.f0);
            list.remove(list.size() - 1);
            return list.toString();
        }).filter(tuple -> tuple.f0.f0.get(tuple.f0.f0.size() - 1) < tuple.f1.f0.get(tuple.f1.f0.size() - 1)).map((tuple) -> {
            List<Integer> candidateList = tuple.f0.f0;
            candidateList.add(tuple.f1.f0.get(candidateList.size() - 1));
            return new Tuple2<List<Integer>, Integer>(candidateList, tuple.f1.f1 + 1);
        });
        // Prune Step
        DataSet<Tuple2<List<Integer>, Integer>> prunedSet = joinedSet.join(freqKItemSets).where(1).equalTo((tuple) -> tuple.f1 - 1).filter((tuple) -> {
            List<Integer> list = new LinkedList<Integer>(tuple.f0.f0);
            for (int i = 0; i <= tuple.f1.f0.size() - 2; i++) {
                list.remove(i);
                if (!list.toString().equals(tuple.f1.f0.toString())) {
                    return false;
                }
            }
            return true;
        }).map((tuple) -> new Tuple2<List<Integer>, Integer>(tuple.f0.f0, tuple.f0.f1));
        return prunedSet;
    }
}