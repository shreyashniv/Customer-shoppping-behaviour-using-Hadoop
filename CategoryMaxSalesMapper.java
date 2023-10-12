import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoryMaxSalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text categoryGender = new Text();
    private DoubleWritable sales = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	//Books,Female	132956.39999999994
        // Extracting the category and gender from the input key
        String[] categoryGenderAndSales = value.toString().split(",");
        
        //categoryGenderAndSales[0] = Books
        //categoryGenderAndSales[1] = Female 132956
        
        String categoryGenderStr = categoryGenderAndSales[1];
        //categoryGenderStr = Female 132956
        
        String[] categoryGenderArr = categoryGenderStr.split("\t");
        //categoryGenderArr[0] = Female
      //categoryGenderArr[1] = 132956
        
        String category = categoryGenderAndSales[0]; 
        String gender = categoryGenderArr[0];

        // Extracting the sales from the input value
        double salesAmount = Double.parseDouble(categoryGenderArr[1]);

        // Emitting the key-value pair
        this.categoryGender.set(category);
        this.sales.set(salesAmount);
        context.write(this.categoryGender, this.sales);
    }
}
