package SalesCountry;

import org.apache.hadoop.fs.Path;

public class SalesCountryDriver {
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf1 = new JobConf(SalesCountryDriver.class);
        job_conf1.setJobName("JUNTOS_Ratio_Job1");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(Text.class);
        job_conf1.setMapperClass(SalesCountry.SalesMapper.class);
        job_conf1.setReducerClass(SalesCountry.SalesCountryReducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job_conf1, new Path("/midis/DICIEMBRE_2024.csv"));
        FileOutputFormat.setOutputPath(job_conf1, new Path("/output_c09_temp"));
        my_client.setConf(job_conf1);

        JobClient my_client2 = new JobClient();
        JobConf job_conf2 = new JobConf(SalesCountryDriver.class);
        job_conf2.setJobName("JUNTOS_Ratio_Job2");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(SalesCountry.SalesMapper2.class);
        job_conf2.setReducerClass(SalesCountry.SalesReducer2.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job_conf2, new Path("/output_c09_temp"));
        FileOutputFormat.setOutputPath(job_conf2, new Path("/output_c09"));

        try {
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
