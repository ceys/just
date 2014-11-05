import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;


public class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] value) {
        super(DoubleWritable.class, value);
    }

    public void fromArray(scala.Double[] value) {
        DoubleWritable[] r = new DoubleWritable[value.length];
        for (int i = 0; i <= value.length; i++) {
            r[i] = new DoubleWritable(scala.Double.unbox(value[i]));
        }
        super.set(r);
    }

}