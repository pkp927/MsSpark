
import java.io.Serializable;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/* FrameAtomsRDD class to support the Molecular Simulation data
 * which is in the form of frame number paired with the data
 * corresponding to the atoms in the that frame
 */

public class FrameAtomsRDD implements Serializable{

	// java pair rdd is used to represent the mapping
	private JavaPairRDD<Integer, Double[]> frames;

	// constructor to create rdd from file
	public FrameAtomsRDD(JavaSparkContext sc, String inputLocation){
		this.setFrameAtomsRDD(sc.textFile(inputLocation).filter(new GetFrameAtoms()).mapToPair(new FrameAtomsMapper()));
	}
	
	// setter function to create rdd from existing rdd
	public void setFrameAtomsRDD(JavaPairRDD<Integer, Double[]> f){
		this.frames = f;
	}

    // getter function to get the rdd
	public JavaPairRDD<Integer, Double[]> getFrameAtomsRDD(){
		return this.frames;
	}
	
}

/* FrameAtomsMapper class to create rdd from file
 */
class FrameAtomsMapper implements Serializable, PairFunction<String, Integer, Double[]>{

	public Tuple2<Integer, Double[]> call(String s) throws Exception {
		String[] splitted = s.split("\\s+");
		int frameNo = Integer.parseInt(splitted[1]);
		Double[] d = new Double[7];
		for(int i=2; i<=8; i++){
			d[i-2] = Double.parseDouble(splitted[i]);
		}
		return new Tuple2(frameNo, d);
	}
	
}

/* GetFrameAtoms class to filter the atoms data out of given data
 */
class GetFrameAtoms implements Function<String, Boolean> {

	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  if(splitted[0].equals("ATOM")){
			  return true;
		  }
		  return false;
	  }
}
