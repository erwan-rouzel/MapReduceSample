package fr.ecp.sio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by erwanrouzel on 26/02/2016.
 */
public class TestDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration, "sample-job");

        // Le setJarByClass s'assure que dans notre CLASS PATH ce jar est bien pris en premier
        // => C'est une bonne pratique de faire cela quand on fait job MapReduce,
        // car il y une compatibilité entre toutes les versions du runtime
        job.setJarByClass(TestDriver.class);
        job.setInputFormatClass(TextInputFormat.class);

        // Juste de la conf...
        FileInputFormat.addInputPath(job, new Path("/tmp/test.in"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/test.out"));

        job.setMapperClass(TestMapper.class);

        job.submit();
        int exitCode = job.waitForCompletion(true)?0:1;
        System.exit(exitCode);
    }
}
