

import java.io.Serializable;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FrameAtomsRDD implements Serializable{

	private JavaPairRDD<Integer, Double[]> frames;
	
	public FrameAtomsRDD(JavaSparkContext sc, String inputLocation){
		this.setFrameAtomsRDD(sc.textFile(inputLocation).filter(new GetFrameAtoms()).mapToPair(new FrameAtomsMapper()));
	}
	
	public void setFrameAtomsRDD(JavaPairRDD<Integer, Double[]> f){
		this.frames = f;
	}
	
	public JavaPairRDD<Integer, Double[]> getFrameAtomsRDD(){
		return this.frames;
	}
	
}

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

class GetFrameAtoms implements Function<String, Boolean> {

	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  if(splitted[0].equals("ATOM")){
			  return true;
		  }
		  return false;
	  }
}
