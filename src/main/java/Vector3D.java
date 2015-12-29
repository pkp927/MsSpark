import java.io.Serializable;

/* VecCalc class to support VectorRDD
 * by implementing functions for
 * operations on vectors
 */
public class Vector3D implements Serializable{

    // variables representing vector
	public double x, y, z;

    // default constructor
	public Vector3D(){
		x = 0; y = 0; z = 0;
	}

    // constructor
	public Vector3D(double a, double b, double c){
		x = a; y = b; z = c;
	}

    // add two vectors
	public Vector3D add(Vector3D mc){
		return new Vector3D(x+mc.x, y+mc.y, z+mc.z);
	}

    // print the values of vectors
	public void printValues(){
		System.out.print("x: "+x+"y: "+y+"z: "+z);
	}

    // convert vector to string representation
	public String toString(){
		return (Double.toString(x)+" "+Double.toString(y)+" "+Double.toString(z)); 
	}
	
}

