package SalesCountry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String[] data = value.toString().trim().split(";");
        if (data.length < 14) return;
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);
        int ninos = 0, iiee = 0;
        try { if (!data[12].trim().isEmpty()) ninos = Integer.parseInt(data[12].trim()); } catch (NumberFormatException e) {}
        try { if (!data[13].trim().isEmpty()) iiee  = Integer.parseInt(data[13].trim()); } catch (NumberFormatException e) {}
        output.collect(new Text(departamento), new Text(ninos + "," + iiee));
    }
}
class SalesMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] partes = value.toString().split("\t");
        if (partes.length < 2) return;
        String depto = partes[0].trim();
        String[] vals = partes[1].trim().split(",");
        if (vals.length < 2) return;
        try {
            double ninos = Double.parseDouble(vals[0].trim());
            double iiee  = Double.parseDouble(vals[1].trim());
            double ratio = (iiee > 0) ? ninos / iiee : 0.0;
            output.collect(new Text(depto), new Text(String.format("%.2f ninos/IIEE", ratio)));
        } catch (NumberFormatException e) {}
    }
}
