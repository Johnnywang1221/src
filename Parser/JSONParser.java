package Parser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import collection.CollectionTable;
import collection.Constants;


public final class JSONParser {

	public static List<Put> parseJSON(String string) {
		// 轨迹列族名
		byte[] cf = CollectionTable.INFO_CF;		
				
		JSONObject jo = JSONObject.fromObject(string);
		String username = jo.getString(Constants.USERNAME);
		
		JSONArray recordList = jo.getJSONArray(Constants.RECORDLIST);
		List<Put> resultList = new ArrayList<Put>();
		
		for (int i = 0; i < recordList.size(); i++) {
			JSONObject job = recordList.getJSONObject(i);
			String ts_str = job.getString(Constants.TIMESTAMP);
			
			if (ts_str != null) {
				try {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
					Date timestamp = sdf.parse(ts_str);
					byte[] rowKey = CollectionTable.generateRowKey(username,timestamp.getTime());
					Put put = new Put(rowKey);
					
					if (job.has(Constants.LON)) {
						double longitude = job.getDouble(Constants.LON);
						put.add(cf, Bytes.toBytes(Constants.LON),Bytes.toBytes(longitude));
					}
					if (job.has(Constants.LAT)) {
						double latitude = job.getDouble(Constants.LAT);
						put.add(cf, Bytes.toBytes(Constants.LAT),Bytes.toBytes(latitude));
					}			
					if (job.has(Constants.LIGHT)) {
						double light = job.getDouble(Constants.LIGHT);
						put.add(cf, Bytes.toBytes(Constants.LIGHT),
								Bytes.toBytes(light));
					}

					if (job.has(Constants.NOISE)) {
						double noise = job.getDouble(Constants.NOISE);
						put.add(cf, Bytes.toBytes(Constants.NOISE),
								Bytes.toBytes(noise));
					}

					if (job.has(Constants.OREN_X)) {
						double orien_x = job.getDouble(Constants.OREN_X);
						put.add(cf, Bytes.toBytes(Constants.OREN_X),
								Bytes.toBytes(orien_x));
					}

					if (job.has(Constants.OREN_Y)) {
						double orien_y = job.getDouble(Constants.OREN_Y);
						put.add(cf, Bytes.toBytes(Constants.OREN_Y),
								Bytes.toBytes(orien_y));
					}

					if (job.has(Constants.BATTERY)) {
						int battery = job.getInt(Constants.BATTERY);
						put.add(cf, Bytes.toBytes(Constants.BATTERY),
								Bytes.toBytes(battery));
					}

					if (job.has(Constants.CONNECT)) {
						int connect = job.getInt(Constants.CONNECT);
						put.add(cf, Bytes.toBytes(Constants.CONNECT),
								Bytes.toBytes(connect));
					}

					if (job.has(Constants.CHARGE)) {
						int charge = job.getInt(Constants.CHARGE);
						put.add(cf, Bytes.toBytes(Constants.CHARGE),
								Bytes.toBytes(charge));
					}

					resultList.add(put);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
			}// end if

		}// end for
		
		return resultList;
	}

}
