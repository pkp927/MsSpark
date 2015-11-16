
import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class VectorRDD implements Serializable{

	private JavaPairRDD<Integer, VecCalc> vec;
	
	public VectorRDD(){
		this.vec = null;
	}
	
	public VectorRDD(JavaPairRDD<Integer, VecCalc> v){
		this.vec = v;
	}
	
	public void setVectorRDD(JavaPairRDD<Integer, VecCalc> v){
		this.vec = v;
	}
	
	public JavaPairRDD<Integer, VecCalc> getVectorRDD(){
		return this.vec;
	}
	
	public JavaPairRDD<Integer, String> getStringVector(){
		return vec.mapValues(new ConvertToStringVector());
	}
	
}

class ConvertToStringVector implements Function<VecCalc, String>{

	public String call(VecCalc vc) throws Exception {
		return vc.toString();
	}
	
}

