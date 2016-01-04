package pk.edu.msspark.app;
/* MSApp.java */

import org.apache.spark.api.java.*;

import java.io.File;
import java.io.Serializable;
import java.util.Scanner;

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
import org.apache.spark.storage.StorageLevel;

import pk.edu.msspark.query.OneBodyQuery;
import pk.edu.msspark.resultRDD.VectorRDD;
import pk.edu.msspark.selectionRDD.FrameAtomsRDD;
import pk.edu.msspark.selectionRDD.SelectedAtomsRDD;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* MSApp class is the main class to get the input files 
 * and generate output files
 */
public class MSApp {
	
	  public static String[] parameters;
	
	  public static void main(String[] args) {

	    // create SparkContext object to access clusters
	    SparkConf conf = new SparkConf().setAppName("Simple Application");
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
	    FrameAtomsRDD distData = new FrameAtomsRDD(sc, inputLoc);
	    //distData.getFrameAtomsRDD().persist(StorageLevel.MEMORY_AND_DISK());
	    // testing
	    while(true){
	    	// get input from user
		    Scanner reader = new Scanner(System.in);
		    System.out.println("Enter the first frame no:");
		    String a = reader.nextLine();
		    System.out.println("Enter the last frame no:");
		    String b = reader.nextLine();
		    System.out.println("Enter the skip frame nos:");
		    String c = reader.nextLine();
		    System.out.println("Enter the min boundary:");
		    String d = reader.nextLine();
		    System.out.println("Enter the max boundary:");
		    String e = reader.nextLine();
		    
		    // set the parameters for the selection of desired atoms
		    parameters = new String[7];
		    parameters[0] = a;     // first frame
		    parameters[1] = b;     // last frame
		    parameters[2] = c;      // skip
		    parameters[3] = d;      // min boundary
		    parameters[4] = e;		 // max boundary
		    parameters[5] = "";		 // atom type
		    parameters[6] = "";		 // atom id
	    	
		    // get desired data
		    SelectedAtomsRDD selData = new SelectedAtomsRDD(sc, distData, parameters);
		    //selData.getSelectedAtomsRDD().coalesce(1,true).saveAsTextFile(outputLoc);
		    
		    while(true){
		    	// execute the query -> Moment of Inertia
		    	VectorRDD MOI = OneBodyQuery.getMOI(selData);
		    
	        	// save the result in the output folder
	        	//MOI.getVectorRDD().coalesce(1,true).saveAsTextFile(outputLoc);
	        	MOI.getVectorRDD().foreach(new PrintVecTuple());
	        	
	        	// check for finish
		    	System.out.println("Do u wanna change the data selection?(n/y):");
		    	String r = reader.nextLine();
		    	if(r.equals("y")){
		    		break;
		    	}
		    }
		    // check for finish
	    	System.out.println("Are u done?(n/y):");
	    	String r = reader.nextLine();
	    	if(r.equals("y")){
	    		break;
	    	}
	    }
	    
	    
	    /*boolean cached = false;
	    
	    if(cached){
	    	DataFrame parquetFile = sqlContext.read().parquet(moiCacheLoc+"/moiCache.parquet");
	    	parquetFile.registerTempTable("parquetFile");
	    	DataFrame result = sqlContext.sql("SELECT * FROM parquetFile WHERE frameNo >= "+parameters[0]+" AND frameNo <= "+parameters[1]);
	    	result.show();
	    }
	    
	    if(!cached){
	    	
	    	// create SelectedAtomsRDD of desired atoms out of the input data
	        SelectedAtomsRDD selData = new SelectedAtomsRDD(sc, inputLoc, parameters);
	        selData.getSelectedAtomsRDD().foreach(new PrintTuple());
	        
	        // execute the query -> Moment of Inertia
	        //VectorRDD MOI = OneBodyQuery.getMOI(selData);
	        
	        // save the result in the output folder
	        //MOI.getVectorRDD().coalesce(1,true).saveAsTextFile(outputLoc);
	        //MOI.getVectorRDD().foreach(new PrintVecTuple());
	        
	        // cache the result
	        //cacheMOIresult(sqlContext, MOI, moiCacheLoc);
	    }
	    */
	  }
	  
	  public static void cacheMOIresult(SQLContext sqlContext, VectorRDD MOI, String moiCacheLoc){
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

class PrintTuple implements VoidFunction<Tuple2<Integer, Double[]>>{

	public void call(Tuple2<Integer, Double[]> t) throws Exception {
		System.out.print(t._1+" : ");
		for(int i=0; i<t._2.length; i++){
			System.out.print(t._2[i]+" ");
		}
		System.out.println();
	}
	
}

class PrintVecTuple implements VoidFunction<Tuple2<Integer, Vector3D>>{

	public void call(Tuple2<Integer, Vector3D> t) throws Exception {
		System.out.print(t._1+" : ");
		t._2().printValues();
		System.out.println();
	}
	
}





