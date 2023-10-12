import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryGenderMaxSalesDriver {

    public static void main(String[] args) throws Exception {
        // First Job: Mapper 1 and Reducer 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "CategoryGenderPrice");
        job1.setJarByClass(CategoryGenderMaxSalesDriver.class);
        job1.setMapperClass(CategoryGenderPriceMapper.class);
        job1.setReducerClass(CategoryGenderPriceReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        // Second Job: Mapper 2 and Reducer 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "CategoryGenderMaxSales");
        job2.setJarByClass(CategoryGenderMaxSalesDriver.class);
        job2.setMapperClass(CategoryMaxSalesMapper.class);
        job2.setReducerClass(CategoryMaxSalesReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        // Run both jobs sequentially
        int job1Status = job1.waitForCompletion(true) ? 0 : 1;
        int job2Status = job2.waitForCompletion(true) ? 0 : 1;

        System.exit(job1Status + job2Status);
    }
}
