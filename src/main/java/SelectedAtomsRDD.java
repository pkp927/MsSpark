
import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/* SelectedAtomsRDD class to support the Molecular Simulation data
 * which is in the form of frame number paired with the data
 * corresponding to the atoms in the that frame
 */

public class SelectedAtomsRDD implements Serializable{
	
	// default parameters for atom selection
	Broadcast<Integer> firstFrame;
	Broadcast<Integer> lastFrame;
	Broadcast<int[]> skip;
	Broadcast<Double[]> min;
	Broadcast<Double[]> max;
	Broadcast<Double[]> atomTypes;
	Broadcast<Double[]> atomIds;
	
	// java pair rdd is used to represent the selected atoms corresponding to the frame number 
	private JavaPairRDD<Integer, Double[]> selection;
	
	// constructor to create RDD of the desired atoms	
	public SelectedAtomsRDD(JavaSparkContext sc, String inputLocation, String[] parameters){
		
		// broadcast the parameters
		firstFrame = sc.broadcast(Integer.parseInt(parameters[0]));
		lastFrame = sc.broadcast(Integer.parseInt(parameters[1]));
		if(!parameters[2].isEmpty()){
			String[] splitted = parameters[2].split("\\s+");
			int[] sk = new int[splitted.length];
			for(int i=0; i<splitted.length; i++){
				sk[i] = Integer.parseInt(splitted[i]);
			}
			skip = sc.broadcast(sk);
		}else{
			skip = sc.broadcast(null);
		}
		
		// set the RDD of desired atoms
		this.setSelectedAtomsRDD(sc.textFile(inputLocation).
				filter(new GetOnlyAtoms()).
				mapToPair(new SelectedAtomsMapper()).cache().
				filter(new GetSelectedAtoms(firstFrame, lastFrame, skip)).
				cache());
		
	}
	
	// constructor to create RDD of the desired atoms	
	public SelectedAtomsRDD(JavaSparkContext sc, FrameAtomsRDD far, String[] parameters){
			
			// broadcast the parameters
			firstFrame = sc.broadcast(Integer.parseInt(parameters[0]));
			lastFrame = sc.broadcast(Integer.parseInt(parameters[1]));
			if(!parameters[2].isEmpty()){
				String[] splitted = parameters[2].split("\\s+");
				int[] sk = new int[splitted.length];
				for(int i=0; i<splitted.length; i++){
					sk[i] = Integer.parseInt(splitted[i]);
				}
				skip = sc.broadcast(sk);
			}else{
				skip = sc.broadcast(null);
			}
			
			// set the RDD of desired atoms
			this.setSelectedAtomsRDD(far.getFrameAtomsRDD().
					filter(new GetSelectedAtoms(firstFrame, lastFrame, skip)).
					cache());
			
	}
	
	// setter of RDD
	public void setSelectedAtomsRDD(JavaPairRDD<Integer, Double[]> s){
		this.selection = s;
	}
	
	// getter of RDD
	public JavaPairRDD<Integer, Double[]> getSelectedAtomsRDD(){
		return this.selection;
	}

}
/* GetSelectedAtoms class to create rdd from file of desired atoms
 */
class GetSelectedAtoms implements Function<Tuple2<Integer, Double[]>, Boolean>{

	Broadcast<Integer> firstFrame;
	Broadcast<Integer> lastFrame;
	Broadcast<int[]> skip;

    public GetSelectedAtoms(Broadcast<Integer> f, Broadcast<Integer> l, Broadcast<int[]> sk){
              firstFrame = f;
              lastFrame = l;
              skip = sk;
    }
	
	public Boolean call(Tuple2<Integer, Double[]> t) throws Exception {
		if(t._1 >= firstFrame.value() && t._1 <= lastFrame.value()){
			if(skip.equals(null)){
				int[] a = skip.value();
				for(int i = 0; i < a.length; i++){
					if(t._1 == a[i]){
						return false;
					}
				}
			}
			return true;
		}
		return false;
	}
	
}
/* SelectedAtomsMapper class to create rdd from file
 */
class SelectedAtomsMapper implements Serializable, PairFunction<String, Integer, Double[]>{

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
/* GetOnlyAtoms class to filter the atoms data out of given data
 */
class GetOnlyAtoms implements Function<String, Boolean> {

	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  if(splitted[0].equals("ATOM")){
			  return true;
		  }
		  return false;
	  }
}

