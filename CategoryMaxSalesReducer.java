import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoryMaxSalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable totalSales = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // Extracting category from the key
        String[] categoryGender = key.toString().split(",");
        String category = categoryGender[0];

        // Summing up the prices for each category
        double totalPrice = 0;
        for (DoubleWritable value : values) {
            totalPrice += value.get();
        }

        // Writing the output key-value pair
        totalSales.set(totalPrice);
        context.write(new Text(category), totalSales);
    }
}
