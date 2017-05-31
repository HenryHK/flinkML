package ml;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Created by lhan on 17-5-31.
 */
public class Task1 {

    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();

        String geoDir = "/share/genedata/small/";
        String patientDir = "/share/genedata/small/";
        String outputDir = "/user/lhan9852/assignment3/small/";

        // id, geneid, expression_value
        DataSet<Tuple3<String, Integer, Double>> geoData =
                env.readTextFile(geoDir+"GEO.txt")
                .map(line->
                        new Tuple3<String, Integer, Double>(line.split(",")[0],Integer.parseInt(line.split(",")[1]),Double.parseDouble(line.split(",")[2])))
                .filter(new GeneIDFilter());

        // id cancer-type
        DataSet<Tuple2<String, String>> patientData =
                env.readTextFile(patientDir+"PatientMetaData.txt")
                .flatMap((line, out)->{
                    String[] values = line.split(",");
                    if(values.length==6&&!values[0].equals("id")){
                        String[] diseases = values[4].split("\\s+");
                        for (String disease:diseases){
                            out.collect(new Tuple2<String, String>(values[0], disease));
                        }
                    }
                });
        patientData = patientData.filter(new CancerFilter());

        DataSet<Tuple2<String, String>> cancersData =
                patientData
                        .join(geoData)
                        .where(0)
                        .equalTo(0)
                        .projectFirst(1)
                        .projectSecond(0);

        DataSet<Tuple2<String, Integer>> result =
                cancersData
                        .map(tuple2 -> new Tuple2<String, Integer>(tuple2.f0, 1))
                        .groupBy(0)
                        .sum(1);


        result.writeAsText(outputDir+"task1.txt");
    }

}
