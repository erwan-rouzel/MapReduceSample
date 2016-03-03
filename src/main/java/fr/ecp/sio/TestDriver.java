package fr.ecp.sio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by erwanrouzel on 26/02/2016.
 */
public class TestDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new TestDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        final Job job = Job.getInstance(getConf(), "sample-job");

        // Le setJarByClass s'assure que dans notre CLASS PATH ce jar est bien pris en premier
        // => C'est une bonne pratique de faire cela quand on fait job MapReduce,
        // car il y une compatibilité entre toutes les versions du runtime
        job.setJarByClass(TestDriver.class);

        // Pour avoir un job Map uniquement (sans Reduce) :
        // job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        // Juste de la conf...
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TestMapper.class);

        job.submit();
        int exitCode = job.waitForCompletion(true)?0:1;
        return exitCode;
    }
}
