import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoryGenderPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        String category = fields[4]; //4
        String gender = fields[2]; //2
        double price = Double.parseDouble(fields[6]); //6
        outputKey.set(category + "," + gender);
        outputValue.set(price);
        context.write(outputKey, outputValue);
    }
}
