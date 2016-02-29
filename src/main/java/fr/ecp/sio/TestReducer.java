package fr.ecp.sio;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by erwanrouzel on 26/02/2016.
 */
public class TestReducer extends Reducer {
    public TestReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
