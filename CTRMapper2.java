import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import javax.naming.Context;

public class CTRMapper extends Mapper<LongWritable, Text, Text, Text>  {
    @Override
    public void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException {
        // Splitting the line into columns
        String[] columns = value.toString().split("\t");

        if (columns.length > 23) { // For the first season, there are 24 columns
            String adCreativeId = columns[18]; // 19th column
            String width = columns[13]; // 14th column
            String height = columns[14]; // 15th column
            String clicks = columns[24]; // 23rd column
            String conversions = columns[25]; // 24th column

            // Creating a composite value of width, height, clicks, and conversions
            String compositeValue = width + "\t" + height + "\t" + clicks + "\t" + conversions;

            // Writing out the adCreativeId as the key and the composite values as the value
            context.write(new Text(adCreativeId), new Text(compositeValue));
        }
    }
}