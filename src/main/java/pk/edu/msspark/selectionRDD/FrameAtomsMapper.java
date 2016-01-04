package pk.edu.msspark.selectionRDD;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/* FrameAtomsMapper class to create rdd from file
 */
class FrameAtomsMapper implements Serializable, PairFunction<String, Integer, Double[]>{
	
	Broadcast<Integer> frame_no_index;
	Broadcast<Integer> offset;
	Broadcast<Integer> row_length;
	
	public FrameAtomsMapper(Broadcast<Integer> fn, Broadcast<Integer> of, Broadcast<Integer> rl){
		frame_no_index = fn;
		offset = of;
		row_length = rl;
	}
 
	public Tuple2<Integer, Double[]> call(String s) throws Exception {
		String[] splitted = s.split("\\s+");
		
		int frameNo = Integer.parseInt(splitted[frame_no_index.value()]);
		Double[] d = new Double[row_length.value()];
		int start = offset.value();
		for(int i= start; i<row_length.value(); i++){
			d[i-start] = Double.parseDouble(splitted[i]);
		}
		return new Tuple2(frameNo, d);
	}
	
}
