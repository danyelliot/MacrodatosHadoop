package SalesCountry;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String[] data = value.toString().trim().split(";");
        if (data.length < 6) return;
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);
        int afiliados = 0, abonados = 0;
        try { if (!data[4].trim().isEmpty()) afiliados = Integer.parseInt(data[4].trim()); } catch (NumberFormatException e) {}
        try { if (!data[5].trim().isEmpty()) abonados  = Integer.parseInt(data[5].trim()); } catch (NumberFormatException e) {}
        output.collect(new Text(departamento), new Text(afiliados + "," + abonados));
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
            double afiliados = Double.parseDouble(vals[0].trim());
            double abonados  = Double.parseDouble(vals[1].trim());
            double ratio = (afiliados > 0) ? (abonados / afiliados) * 100.0 : 0.0;
            output.collect(new Text(depto), new Text(String.format("afiliados=%.0f abonados=%.0f ratio=%.2f%%", afiliados, abonados, ratio)));
        } catch (NumberFormatException e) {}
    }
}