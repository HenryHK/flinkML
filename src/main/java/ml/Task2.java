package ml;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by lhan on 17-6-1.
 */
public class Task2 {

    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();

        int iterations = 5;

        String geoDir = "hdfs:///share/genedata/test/";
        String patientDir = "hdfs:///share/genedata/test/";
        String outputDir = "hdfs:///user/lhan9852/assignment3/test/";

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
        DataSet<Tuple2<String, String[]>> patientData =
                env.readTextFile(geoDir+"PatientMetaData.txt")
                        .flatMap((line, out)->{
                            String[] values = line.split(",");
                            if(values.length==6&&!values[0].equals("id")){
                                String[] diseases = values[4].split("\\s+");
                                List diseasesArray = Arrays.asList(diseases);
                                if(diseasesArray.contains("breast-cancer")
                                        ||diseasesArray.contains("prostate-cancer")
                                        ||diseasesArray.contains("pancreatic-cancer")
                                        ||diseasesArray.contains("leukemia")
                                        ||diseasesArray.contains("lymphoma")){

                                    out.collect(new Tuple2<String, String[]>(values[0].trim(), diseases));

                                }
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

        long total = resultData.count();
        double min_support = total*0.3d;

        DataSet<Tuple2<String, Integer>> input =
                resultData.flatMap((tuple, out)->{
                    for(int geneid : tuple.f1){
                        out.collect(new Tuple2<String, Integer>(tuple.f0, geneid));
                    }
                });


        input.first(10).print();
        //KeySelector<ItemSet, String> selector = new Task2KeySelector();

        //get the itemset with size=1
        DataSet<ItemSet> initial = input
                // map item to 1
                .map(new Task2Mapper())
                // group by hashCode of the ItemSet
                .groupBy(new Task2KeySelector())
                // sum the number of transactions containing the ItemSet
                .reduce(new Task2ItemSetReducer())
                // remove ItemSets with frequency under the support threshold
                .filter(new Task2SupportFilter(min_support));

        IterativeDataSet<ItemSet> iteSet = initial.iterate(iterations - 1);

        DataSet<ItemSet> candidates = iteSet.cross(initial)
                .with(new Task2ItemSetCross())
                .distinct(new Task2KeySelector());

        // calculate actual numberOfTransactions
        DataSet<ItemSet> selected = candidates
                .map(new Task2FrequencyCalculator()).withBroadcastSet(resultData, "transactions")
                .filter(new Task2SupportFilter(min_support));


        DataSet<ItemSet> output = iteSet.closeWith(selected);
        output.writeAsText(outputDir+"task2");
        env.execute();
    }

}
