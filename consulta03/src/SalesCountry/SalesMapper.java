package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String[] data = value.toString().trim().split(";");
        if (data.length < 4) return;
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);
        int cuidadoDiurno = 0;
        int acompFamilias = 0;
        try { if (!data[2].trim().isEmpty()) cuidadoDiurno = Integer.parseInt(data[2].trim()); } catch (NumberFormatException e) {}
        try { if (!data[3].trim().isEmpty()) acompFamilias = Integer.parseInt(data[3].trim()); } catch (NumberFormatException e) {}
        if (cuidadoDiurno > 0 || acompFamilias > 0) {
            output.collect(new Text(departamento), new IntWritable(1));
        }
    }
}
