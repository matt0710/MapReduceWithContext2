package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MaxPartecipantsReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce (Text text, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {

        String[] row = new String[2];
        HashMap<String, IntWritable> map = new HashMap<>();

        for (Text t : iterable) {
            row = t.toString().split(";");
            map.put(row[0], new IntWritable(Integer.parseInt(row[1])));
        }

        IntWritable max = Collections.max(map.values());

        for (Map.Entry<String, IntWritable> entry : map.entrySet()) {
            if (entry.getValue().equals(max)) context.write(text, new Text(entry.getKey()));
        }
    }
}
