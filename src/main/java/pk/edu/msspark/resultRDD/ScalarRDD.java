package pk.edu.msspark.resultRDD;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

/* scatorRDD class to represent scator data type
 * and operations on this data type
 */
public class ScalarRDD implements Serializable{

    // frame number is mapped to scator quantity
	private JavaPairRDD<Integer, Double> sca;

    // default constructor
	public ScalarRDD(){
		this.sca = null;
	}

    // constructor
	public ScalarRDD(JavaPairRDD<Integer, Double> v){
		this.sca = v;
	}

    // setter method
	public void setScalarRDD(JavaPairRDD<Integer, Double> v){
		this.sca = v;
	}

    // getter method
	public JavaPairRDD<Integer, Double> getScalarRDD(){
		return this.sca;
	}

    // convert to string representation
	public JavaPairRDD<Integer, String> getStringScalar(){
		return sca.mapValues(new ConvertToStringScalar());
	}
	
}

/* ConvertToStringscator class is used to convert the
 * scatorRDD to string representation
 */
class ConvertToStringScalar implements Function<Double, String>{

	public String call(Double vc) throws Exception {
		return vc.toString();
	}
	
}
