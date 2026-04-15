package SalesCountry;

import org.apache.hadoop.fs.Path;

public class SalesCountryDriver {
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf = new JobConf(SalesCountryDriver.class);
        job_conf.setJobName("Busqueda_IntervaloDeFechas");
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);
        job_conf.setMapperClass(SalesCountry.SalesMapper.class);
        job_conf.setReducerClass(SalesCountry.SalesCountryReducer.class);
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job_conf, new Path("/midis/DICIEMBRE_2024.csv"));
        FileOutputFormat.setOutputPath(job_conf, new Path("/output_c08"));
        my_client.setConf(job_conf);
        try { JobClient.runJob(job_conf); } catch (Exception e) { e.printStackTrace(); }
    }
}
