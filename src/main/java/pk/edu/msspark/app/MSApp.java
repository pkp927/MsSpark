package pk.edu.msspark.app;
/* MSApp.java */

import org.apache.spark.api.java.*;

import java.io.File;
import java.util.Scanner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;

import pk.edu.msspark.query.OneBodyQuery;
import pk.edu.msspark.resultRDD.*;
import pk.edu.msspark.selectionRDD.FrameAtomsRDD;
import pk.edu.msspark.selectionRDD.SelectedAtomsRDD;
import pk.edu.msspark.utils.*;
import pk.edu.msspark.cache.*;
import scala.Tuple2;

/* MSApp class is the main class to get the input files 
 * and generate output files
 */

enum Query{
	MOI,
	COM,
	ROG,
	DM,
	SOM
}

public class MSApp {
	
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
	    String cacheLoc = "/home/simi/spark-1.5.1/abc/cache";
	    
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
	    SelectedAtomsRDD selData;

	    // set parameter variable;
	    SelectParameters param = new SelectParameters();
	    
	    String query;
	    
	    // define reader variable to get input from user
	    Scanner reader = new Scanner(System.in);
	    
	    // testing
	    while(true){
		    // get the selection parameters
		    param = getSelectParameters(param);
		    
		    // ask for query
		    System.out.println("Enter the query(moi/som/com/dm/rog):");
			query = reader.nextLine();
			
			// check cache
		    param = checkCache(sqlContext, cacheLoc, param, query);
		    
	    	// if not cached
		    if(!param.cached){
			    // get desired data
			    selData = new SelectedAtomsRDD(sc, distData, param);
			    selData.rePartition(param.lastFrame - param.lastFrame + 1);
			    selData.getSelectedAtomsRDD().cache();

		    	// execute the query and cache the result
		    	executeQueryAndCache(sc,selData,query,param,sqlContext,cacheLoc);
		    	
		    	// continue with same selection
			    while(true){    
		        	// check for finish
			    	System.out.println("Do u wanna change the data selection or are u done?(n/y):");
			    	String r = reader.nextLine();
			    	if(r.equals("y")){
			    		break;
			    	}else{
			    		System.out.println("Enter the query(moi/som/com/dm/rog):");
						query = reader.nextLine();
			    	}

					// check cache
				    param = checkCache(sqlContext, cacheLoc, param, query);
				 
				    // if not cached
				    if(!param.cached){
				    	// execute the query and cache the result
				    	executeQueryAndCache(sc,selData,query,param,sqlContext,cacheLoc);
				    }
			    }
		    }
		    // check for finish
	    	System.out.println("Are u done?(n/y):");
	    	String r = reader.nextLine();
	    	if(r.equals("y")){
	    		break;
	    	}
	    }
	    
	  }  
	  
	  public static void executeQueryAndCache(JavaSparkContext sc, SelectedAtomsRDD selData,
			  String query, SelectParameters param, SQLContext sqlContext, String cacheLoc){

	      // define query variable
		  OneBodyQuery q = new OneBodyQuery(sc);
		  
		  Query qr = Query.valueOf(query.toUpperCase());
		  switch(qr){
		  case MOI:
			  VectorRDD MOI = q.getMOI(selData);
			  MOI.getVectorRDD().foreach(new PrintVecTuple());
	          //MOI.getVectorRDD().coalesce(1,true).saveAsTextFile(outputLoc);
			  MoiCache.cacheMOIresult(sqlContext, MOI, cacheLoc, param);
			  break;
		  case COM:
		      VectorRDD COM = q.getCOM(selData);
		      COM.getVectorRDD().foreach(new PrintVecTuple());
		      ComCache.cacheCOMresult(sqlContext, COM, cacheLoc, param);
			  break;
		  case SOM:
		      ScalarRDD SOM = q.getSOM(selData);
	          SOM.getScalarRDD().foreach(new PrintScaTuple());
	          SomCache.cacheSOMresult(sqlContext, SOM, cacheLoc, param);
			  break;
		  case ROG:
		      ScalarRDD ROG = q.getROG(selData,param.axis);
	          ROG.getScalarRDD().foreach(new PrintScaTuple());
	          RogCache.cacheROGresult(sqlContext, ROG, cacheLoc, param);
			  break;
		  case DM:
		      VectorRDD DM = q.getDM(selData);
		      DM.getVectorRDD().foreach(new PrintVecTuple());
		      DmCache.cacheDMresult(sqlContext, DM, cacheLoc, param);
			  break;
		  }
	  }
	  
	  public static SelectParameters checkCache(SQLContext sqlContext, String cacheLoc, SelectParameters param, String query){
		  Query qr = Query.valueOf(query.toUpperCase());
		  switch(qr){
		  case MOI:
			  param = MoiCache.checkMOIcache(sqlContext, cacheLoc, param);
			  break;
		  case COM:
			  param = ComCache.checkCOMcache(sqlContext, cacheLoc, param);
			  break;
		  case SOM:
			  param = SomCache.checkSOMcache(sqlContext, cacheLoc, param);
			  break;
		  case ROG:
			  Scanner reader = new Scanner(System.in);
			  System.out.println("Enter the axis:");
			  param.axis = reader.nextLine();
			  param = RogCache.checkROGcache(sqlContext, cacheLoc, param);
			  break;
		  case DM:
			  param = DmCache.checkDMcache(sqlContext, cacheLoc, param);
			  break;
		  }
		  return param;
	  }
	  
	  public static SelectParameters getSelectParameters(SelectParameters param){

	    	// get input parameters for selection from user
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
		    param.firstFrame = Integer.parseInt(a);  
		    param.lastFrame = Integer.parseInt(b);      
		    param.skip = c;      					
		    String[] split = d.split("\\s+");
		    if(split.length > 1) param.minBound = new Vector3D(
		    		Double.parseDouble(split[0]),Double.parseDouble(split[1]),Double.parseDouble(split[2]));
		    else param.minBound = new Vector3D();
		    split = e.split("\\s+");
		    if(split.length > 1) param.maxBound = new Vector3D(
		    		Double.parseDouble(split[0]),Double.parseDouble(split[1]),Double.parseDouble(split[2]));  
		    else param.maxBound = new Vector3D();
		    param.atomType = "";		
		    param.atomId = "";	
		    param.cached = true;
		
		    return param;
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

class PrintScaTuple implements VoidFunction<Tuple2<Integer, Double>>{

	public void call(Tuple2<Integer, Double> t) throws Exception {
		System.out.println(t._1+" : "+t._2);
	}
	
}

