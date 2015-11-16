
/* MSApp.java */
import org.apache.spark.api.java.*;

import java.io.Serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;


public class MSApp {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("MS Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    String inputLoc = "hdfs://localhost:54310/example/data.txt";
    //String outputLoc = "hdfs://localhost:54310/example/output";
    String outputLoc = "/home/simi/spark-1.5.1/thesis/MsSpark/output";
    FrameAtomsRDD distData = new FrameAtomsRDD(sc, inputLoc);
    
    VectorRDD MOI = OneBodyQuery.getMOI(distData);
    MOI.getVectorRDD().saveAsTextFile(outputLoc);
    //MOI.getVectorRDD().foreach(new Print());
    
    /*
    JavaRDD<String> distData = sc.textFile("hdfs://localhost:54310/example/data.txt");

    Broadcast<Integer> frameNo = sc.broadcast(00000000); 
    
    JavaRDD<String> frame = distData.filter(new GetFrameAtoms(frameNo));
    frame.cache();
    
    // Calculate Moment of Inertia
    JavaRDD<VecCalc> mc = frame.map(new ConvertToMOIVec());
    VecCalc moi = mc.reduce(new AddVec());
    System.out.println("MOI : "+moi.x+" "+moi.y+" "+moi.z); 
    
    // Calculate Sum of Masses
    JavaRDD<Double> m = frame.map(new GetMass());
    Double mass = m.reduce(new Add());
    System.out.println("Mass : "+mass);
    
    // Calculate Center of Mass
    VecCalc com = new VecCalc(moi.x/mass, moi.y/mass, moi.z/mass);
    System.out.println("COM : "+com.x+" "+com.y+" "+com.z);

    // Calculate Radius of Gyration
    System.out.println("ROG : "+com.z/mass);

    // Calculate Dipole Moment
    JavaRDD<VecCalc> dc = frame.map(new ConvertToDMVec());
    VecCalc dm = dc.reduce(new AddVec());
    System.out.println("DM : "+dm.x+" "+dm.y+" "+dm.z);
    
    */
    
  }

}

class Print implements VoidFunction<Tuple2<Integer, VecCalc>>{

	public void call(Tuple2<Integer, VecCalc> t) throws Exception {
		System.out.print(t._1+" : ");
		t._2().printValues();
		System.out.println();
	}
	
}

/*
class Add implements Function2<Double, Double, Double>{

	public Double call(Double d1, Double d2) {
		return d1+d2;
	}
	
}

class GetMass implements Function<String, Double>{

	public Double call(String s){
		String[] splitted = s.split("\\s+");
		return Double.parseDouble(splitted[8]);
	}
	
}

class AddVec implements Function2<VecCalc, VecCalc, VecCalc>{

	public VecCalc call(VecCalc mc1, VecCalc mc2) throws Exception {
		return mc1.add(mc2);
	}
	
}

class ConvertToDMVec implements Function<String, VecCalc>{

	public VecCalc call(String s){
		String[] splitted = s.split("\\s+");
		VecCalc mc = new VecCalc();
		mc.x = Double.parseDouble(splitted[4])*Double.parseDouble(splitted[7]);
		mc.y = Double.parseDouble(splitted[5])*Double.parseDouble(splitted[7]);
		mc.z = Double.parseDouble(splitted[6])*Double.parseDouble(splitted[7]);
		return mc;
	}
	
}

class ConvertToMOIVec implements Function<String, VecCalc>{

	public VecCalc call(String s){
		String[] splitted = s.split("\\s+");
		VecCalc mc = new VecCalc();
		mc.x = Double.parseDouble(splitted[4])*Double.parseDouble(splitted[8]);
		mc.y = Double.parseDouble(splitted[5])*Double.parseDouble(splitted[8]);
		mc.z = Double.parseDouble(splitted[6])*Double.parseDouble(splitted[8]);
		return mc;
	}
	
}

class VecCalc implements Serializable{
	
	public double x, y, z;
	
	public VecCalc(){
		x = 0; y = 0; z = 0;
	}
	
	public VecCalc(double a, double b, double c){
		x = a; y = b; z = c;
	}
	
	public VecCalc add(VecCalc mc){
		return new VecCalc(x+mc.x, y+mc.y, z+mc.z);
	}
	
}

class GetFrameAtoms implements Function<String, Boolean> {
        
	  Broadcast<Integer> frameNo;

      public GetFrameAtoms(Broadcast<Integer> f){
                frameNo = f;
      }

	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  if(splitted[0].equals("ATOM") && Integer.parseInt(splitted[1]) == frameNo.value()){
			  return true;
		  }
		  return false;
	  }
}

*/





