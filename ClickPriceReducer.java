import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClickPriceReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalClicks = 0;
        double totalPrice = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 2) {
                String numClickStr = parts[0];
                String payingPriceStr = parts[1];

                if (!numClickStr.isEmpty()) {
                    totalClicks += Double.parseDouble(numClickStr);
                }
                if (!payingPriceStr.isEmpty()) {
                    totalPrice += Double.parseDouble(payingPriceStr);
                }
            }
        }

        if (totalPrice != 0) {
            double clickPriceRatio = totalClicks / totalPrice;
            context.write(key, new DoubleWritable(clickPriceRatio));
        }
    }
}