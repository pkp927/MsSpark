package pk.edu.msspark.cache;

import java.io.File;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import pk.edu.msspark.resultRDD.VectorRDD;
import pk.edu.msspark.utils.SelectParameters;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

public class DmCache {
	
	  public static SelectParameters checkDMcache(SQLContext sqlContext, String dmCacheLoc, SelectParameters param){
		   // File f = new File(dmCacheLoc+"/dmCache.parquet");
		  	//if(!f.exists()){param.cached = false; return param;}   
		    DataFrame parquetFile = sqlContext.read().parquet(dmCacheLoc+"/dmCache.parquet");
		if(parquetFile==null){param.cached = false; return param;} 
	    	parquetFile.registerTempTable("parquetFile");
	    	DataFrame result;

	    	String query = "SELECT DISTINCT * FROM parquetFile "
					+ "WHERE frameNo >= "+param.firstFrame+" AND frameNo <= "+param.lastFrame;

	    	if(!param.skip.isEmpty()) query = query + " AND frameNo NOT IN ( "+param.skip.trim().replace(" ", " , ")+" )";
	    	if(param.minBound != null) query = query + " AND minx="+param.minBound.x+" AND miny="+param.minBound.y+ " AND minz="+param.minBound.z;
	    	else query = query + " AND minx=0 AND miny=0 AND minz=0";	    	 
	    	if(param.maxBound != null) query = query + " AND maxx="+param.maxBound.x+" AND maxy="+param.maxBound.y+ " AND maxz="+param.maxBound.z;
	    	else query = query + " AND maxx=0 AND maxy=0 AND maxz=0";
	    	
	    	result = sqlContext.sql(query);
	    	result.show();
	    	long n1 = result.count();
	    	long n2 = param.lastFrame - param.firstFrame - param.skip.split("\\s+").length+1;
	    	if(n1<n2) param.cached = false;
	    	else param.cached = true;
	    	return param;
	  }

	  public static void cacheDMresult(SQLContext sqlContext, VectorRDD DM, String dmCacheLoc, SelectParameters param){
		    final Vector3D minB = param.minBound; 
	        final Vector3D maxB = param.maxBound; 
	        
	        JavaRDD<DMschema> moiCache = DM.getVectorRDD().map(
	        		new Function<Tuple2<Integer, Vector3D>, DMschema>(){

	    				public DMschema call(Tuple2<Integer, Vector3D> t) throws Exception {
	    					DMschema ms = new DMschema();
	    					ms.setFrameNo(t._1);
	    					
	    					if(minB != null){
	     						ms.setMinx(minB.x);
	     						ms.setMiny(minB.y);
	     						ms.setMinz(minB.z);
	    					}
	    					if(maxB != null){
	     						ms.setMaxx(maxB.x);
	     						ms.setMaxy(maxB.y);
	     						ms.setMaxz(maxB.z);
	    					}
	    					
	    					ms.setDMx(t._2().x);
	    					ms.setDMy(t._2().y);
	    					ms.setDMz(t._2().z);
	    					return ms;
	    				}
	        			
	        		});

	        DataFrame DMdf = sqlContext.createDataFrame(moiCache, DMschema.class);
	        DMdf.show((int)DMdf.count());
	        //DMdf.save(dmCacheLoc+"/dmCache.parquet", SaveMode.Append);
	  }

	  public static class DMschema implements Serializable{
		  private int frameNo;
		  private double minx;
		  private double miny;
		  private double minz;
		  private double maxx;
		  private double maxy;
		  private double maxz;
		  private double DMx;
		  private double DMy;
		  private double DMz;		  
		
		public int getFrameNo() {
			return frameNo;
		}
		public void setFrameNo(int frameNo) {
			this.frameNo = frameNo;
		}
		public double getDMz() {
			return DMz;
		}
		public void setDMz(double mOIz) {
			DMz = mOIz;
		}
		public double getDMy() {
			return DMy;
		}
		public void setDMy(double mOIy) {
			DMy = mOIy;
		}
		public double getDMx() {
			return DMx;
		}
		public void setDMx(double mOIx) {
			DMx = mOIx;
		}
		public double getMaxz() {
			return maxz;
		}
		public void setMaxz(double maxz) {
			this.maxz = maxz;
		}
		public double getMaxy() {
			return maxy;
		}
		public void setMaxy(double maxy) {
			this.maxy = maxy;
		}
		public double getMaxx() {
			return maxx;
		}
		public void setMaxx(double maxx) {
			this.maxx = maxx;
		}
		public double getMinz() {
			return minz;
		}
		public void setMinz(double minz) {
			this.minz = minz;
		}
		public double getMiny() {
			return miny;
		}
		public void setMiny(double miny) {
			this.miny = miny;
		}
		public double getMinx() {
			return minx;
		}
		public void setMinx(double minx) {
			this.minx = minx;
		}
	  }

}
