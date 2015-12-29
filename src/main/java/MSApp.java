
/* MSApp.java */
import org.apache.spark.api.java.*;

import java.io.File;
import java.io.Serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import scala.Tuple2;

/* MSApp class is the main class to get the files input
 * and generate output files
 */
public class MSApp {
  public static void main(String[] args) {

    // create SparkContext object to access clusters
    SparkConf conf = new SparkConf().setAppName("MS Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

    // specify input and output location of files
    //String inputLoc = "hdfs://localhost:54310/example/data.txt";
    //String outputLoc = "hdfs://localhost:54310/example/output";
    String inputLoc = "/home/simi/Desktop/thesis/data.txt";
    String outputLoc = "/home/simi/spark-1.5.1/abc/output";
    String moiCacheLoc = "/home/simi/spark-1.5.1/abc/moi";
    
    File f = new File(outputLoc);
    if(f.exists()){
	    String[] entries = f.list();
	    for(String s: entries){
	    	File currentFile = new File(f.getPath(),s);
	    	currentFile.delete();
	    }
	    f.delete();
    }

    // create FrameAtomsRDD out of input data
    //FrameAtomsRDD distData = new FrameAtomsRDD(sc, inputLoc);
    
    // set the parameters for the selection of desired atoms
    String[] parameters = new String[7];
    parameters[0] = "90";     // first frame
    parameters[1] = "95";    // last frame
    parameters[2] = "";      // skip
    parameters[3] = "";      // min boundary
    parameters[4] = "";		 // max boundary
    parameters[5] = "";		 // atom type
    parameters[6] = "";		 // atom id
    
    boolean cached = false;
    
    if(cached){
    	DataFrame parquetFile = sqlContext.read().parquet(moiCacheLoc+"/moiCache.parquet");
    	parquetFile.registerTempTable("parquetFile");
    	DataFrame result = sqlContext.sql("SELECT * FROM parquetFile WHERE frameNo >= "+parameters[0]+" AND frameNo <= "+parameters[1]);
    	result.show();
    }
    
    if(!cached){
    	// create SelectedAtomsRDD of desired atoms out of the input data
        SelectedAtomsRDD selData = new SelectedAtomsRDD(sc, inputLoc, parameters);
           
        // execute the query -> Moment of Inertia
        VectorRDD MOI = OneBodyQuery.getMOI(selData);
        
        // save the result in the output folder
        MOI.getVectorRDD().coalesce(1,true).saveAsTextFile(outputLoc);
        //MOI.getVectorRDD().foreach(new PrintVecTuple());
        
        // cache the result
        final String minB = parameters[3]; 
        final String maxB = parameters[4]; 
        
        JavaRDD<MOIschema> moiCache = MOI.getVectorRDD().map(
        		new Function<Tuple2<Integer, Vector3D>, MOIschema>(){

    				public MOIschema call(Tuple2<Integer, Vector3D> t) throws Exception {
    					MOIschema ms = new MOIschema();
    					ms.setFrameNo(t._1);
    					/*
    					if(!minB.isEmpty()){
    						String[] split = minB.split("\\s+");
     						double x = Double.parseDouble(split[0]);
     						double y = Double.parseDouble(split[1]);
     						double z = Double.parseDouble(split[2]);
     						ms.setMin(new Vector3D(x,y,z));
    					}
    					if(!maxB.isEmpty()){
    						String[] split = maxB.split("\\s+");
     						double x = Double.parseDouble(split[0]);
     						double y = Double.parseDouble(split[1]);
     						double z = Double.parseDouble(split[2]);
     						ms.setMax(new Vector3D(x,y,z));
    					}
    					*/
    					ms.setMOIx(t._2().x);
    					ms.setMOIy(t._2().y);
    					ms.setMOIz(t._2().z);
    					return ms;
    				}
        			
        		});

        DataFrame MOIdf = sqlContext.createDataFrame(moiCache, MOIschema.class);
        MOIdf.show();
        MOIdf.save(moiCacheLoc+"/moiCache.parquet", SaveMode.Append);
    }
    
  }

  public static class MOIschema implements Serializable{
	  private int frameNo;
	  //private Vector3D min;
	  //private Vector3D max;
	  private double MOIx;
	  private double MOIy;
	  private double MOIz;
	/*  
	public Vector3D getMax() {
		return max;
	}
	public void setMax(Vector3D max) {
		this.max = max;
	}
	public Vector3D getMin() {
		return min;
	}
	public void setMin(Vector3D min) {
		this.min = min;
	}
	*/
	public int getFrameNo() {
		return frameNo;
	}
	public void setFrameNo(int frameNo) {
		this.frameNo = frameNo;
	}
	public double getMOIz() {
		return MOIz;
	}
	public void setMOIz(double mOIz) {
		MOIz = mOIz;
	}
	public double getMOIy() {
		return MOIy;
	}
	public void setMOIy(double mOIy) {
		MOIy = mOIy;
	}
	public double getMOIx() {
		return MOIx;
	}
	public void setMOIx(double mOIx) {
		MOIx = mOIx;
	}
  }

}

class PrintVecTuple implements VoidFunction<Tuple2<Integer, Vector3D>>{

	public void call(Tuple2<Integer, Vector3D> t) throws Exception {
		System.out.print(t._1+" : ");
		t._2().printValues();
		System.out.println();
	}
	
}

