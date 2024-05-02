import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ClickPriceMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text hour = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

    
        if (fields.length >= 29 && !line.startsWith("bidID")) {
            String hourStr = fields[28];
            String numClickStr = fields[24];
            String payingPriceStr = fields[20];

            if (!numClickStr.isEmpty() && !payingPriceStr.isEmpty()) {
                hour.set(hourStr);
                outputValue.set(numClickStr + "," + payingPriceStr);
                context.write(hour, outputValue);
            }
        }
    }
}