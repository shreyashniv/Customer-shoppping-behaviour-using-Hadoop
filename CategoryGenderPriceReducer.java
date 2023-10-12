import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoryGenderPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable totalPrice = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        // Initializing the total price to zero
        double total = 0;

        // Summing up the prices for each gender under each category
        for (DoubleWritable value : values) {
            total += value.get();
        }

        // Emitting the key-value pair
        this.totalPrice.set(total);
        context.write(key, this.totalPrice);
    }
}
