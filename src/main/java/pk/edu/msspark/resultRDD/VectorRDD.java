package pk.edu.msspark.resultRDD;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import pk.edu.msspark.utils.Vector3D;

/* VectorRDD class to represent vector data type
 * and operations on this data type
 */
public class VectorRDD implements Serializable{

    // frame number is mapped to vector quantity
	private JavaPairRDD<Integer, Vector3D> vec;

    // default constructor
	public VectorRDD(){
		this.vec = null;
	}

    // constructor
	public VectorRDD(JavaPairRDD<Integer, Vector3D> v){
		this.vec = v;
	}

    // setter method
	public void setVectorRDD(JavaPairRDD<Integer, Vector3D> v){
		this.vec = v;
	}

    // getter method
	public JavaPairRDD<Integer, Vector3D> getVectorRDD(){
		return this.vec;
	}

    // convert to string representation
	public JavaPairRDD<Integer, String> getStringVector(){
		return vec.mapValues(new ConvertToStringVector());
	}
	
}

/* ConvertToStringVector class is used to convert the
 * vectorRDD to string representation
 */
class ConvertToStringVector implements Function<Vector3D, String>{

	public String call(Vector3D vc) throws Exception {
		return vc.toString();
	}
	
}
