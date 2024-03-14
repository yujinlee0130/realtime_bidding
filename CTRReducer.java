import java.io.IOException;

import javax.naming.Context;

import org.w3c.dom.Text;

public class CTRReducer extends Reducer<Text, Text, Text, Text>{
@Override 
    public void reduce(Text key, Iterable<Text> values, Context context) 
    throws IOException, InterruptedException {
        int totalClicks = 0;
        int totalConversions = 0;
        int frequency = 0;
        String dimensions = "";

        for (Text val : values) {
            String[] parts = val.toString().split("\t");

            if (parts.length == 4) {
                dimensions = parts[0] + "x" + parts[1]; // Combining width and height
                totalClicks += Integer.parseInt(parts[2]);
                totalConversions += Integer.parseInt(parts[3]);
                frequency++;
            }
        }

        double clickThroughRate = (double) totalClicks / frequency;
        double conversionRate = totalClicks > 0 ? (double) totalConversions / totalClicks : 0;

        String result = frequency + "\t" + dimensions + "\t" + totalClicks + "\t" + totalConversions + "\t" + clickThroughRate + "\t" + conversionRate;
        context.write(key, new Text(result));
    }

}
