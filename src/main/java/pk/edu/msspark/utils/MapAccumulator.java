package pk.edu.msspark.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.AccumulatorParam;

/* Class to support accumulators for HashMap 
 */
public class MapAccumulator implements AccumulatorParam<Map<Integer, String>>, Serializable {

    public Map<Integer, String> addAccumulator(Map<Integer, String> t1, Map<Integer, String> t2) {
        return mergeMap(t1, t2);
    }

    public Map<Integer, String> addInPlace(Map<Integer, String> r1, Map<Integer, String> r2) {
        return mergeMap(r1, r2);

    }

    public Map<Integer, String> zero(final Map<Integer, String> initialValue) {
        return new HashMap<Integer, String>();
    }

    private Map<Integer, String> mergeMap( Map<Integer, String> map1, Map<Integer, String> map2) {
        //Map<Integer, String> result = new HashMap<Integer, String>();
        Iterator it = map2.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        map1.put(Integer.valueOf(pair.getKey().toString()), pair.getValue().toString());
	        System.out.println("acc:"+Integer.valueOf(pair.getKey().toString())+":"+ pair.getValue().toString());
	    }
        return map1;
    }

}
