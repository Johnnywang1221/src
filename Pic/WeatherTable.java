package Pic;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import collection.Constants;


/**
 * 城市天气表
 * rowKey:城市+时间戳
 * @author leeying
 *
 */
public class WeatherTable extends CommonOperate{
	//实例
	//private static WeatherTable instance = null;
	// 表名
	private static final String tableName = "weather";
	// 列名
	public final static byte[] TEMP = "temp".getBytes();
	public final static byte[] HUMI = "humi".getBytes();
	private final static byte[] WSPD = "wspeed".getBytes();
	private final static byte[] PRECIP = "precip".getBytes();
	private final static byte[] WEATH = "weather".getBytes();
	private final static byte[] PRESSU = "pressure".getBytes();
	
	public HTable hTable = null;
	
	@Override
	public HTable getHTable() {
		return hTable;
	}

	
	/*public static WeatherTable getInstance(){
		if(instance == null){
			instance = new WeatherTable();
		}
		return instance;
	}*/

	public WeatherTable() {
		Configuration conf = HBaseConfiguration.create();
		try {
			hTable = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 生成行键
	 * 
	 * @param city
	 * @param timestamp
	 * @return
	 */
	private byte[] generateRowKey(String city, Date timestamp) {
		// rowkey 是城市+观测点ID+时间戳
		String rowKey = city.toLowerCase()
				+ Constants.SEPARATER + timestamp.getTime();
		return rowKey.getBytes();
	}
	
	/**
	 * 存储
	 * @param city 城市
	 * @param timestamp 时间戳
	 * @param temp 温度
	 * @param humi 湿度
	 * @param wspeed 风速
	 * @param precip 降雨量
	 * @param pressure 压强
	 * @param weather 天气状况
	 */
	public Boolean set(String city,Date timestamp,int temp,int humi,int wspeed,int precip,int pressure,int weather){
		Put put = new Put(generateRowKey(city, timestamp));
		put.add(COLFAM_NAME, TEMP, Bytes.toBytes(temp));
		put.add(COLFAM_NAME, HUMI, Bytes.toBytes(humi));
		put.add(COLFAM_NAME, WSPD, Bytes.toBytes(wspeed));
		put.add(COLFAM_NAME, PRECIP, Bytes.toBytes(precip));
		put.add(COLFAM_NAME, WEATH, Bytes.toBytes(weather));
		put.add(COLFAM_NAME,PRESSU,Bytes.toBytes(pressure));
		
		try {
			hTable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	/**
	 * 获得与时间戳最接近的记录
	 * @param city 城市
	 * @param timestamp 时间戳
	 */
	public WeatherDataUnit getNearst(String city,Date timestamp){
		WeatherDataUnit wdu = null;	
		try {
			Result r = findNearestValidRecord(city.toLowerCase(), timestamp);			
		    if(r!= null && !r.isEmpty()){
		    	// TODO: 可能r.getValue是null值
		    	int temp = Bytes.toInt(r.getValue(COLFAM_NAME, TEMP));
		    	int humi = Bytes.toInt(r.getValue(COLFAM_NAME, HUMI));
		    	int wspend = Bytes.toInt(r.getValue(COLFAM_NAME, WSPD));
		    	int precip = Bytes.toInt(r.getValue(COLFAM_NAME, PRECIP));
		    	int pressure = Bytes.toInt(r.getValue(COLFAM_NAME, PRESSU));
		    	int weather = Bytes.toInt(r.getValue(COLFAM_NAME, WEATH));
				    	
		    	return new WeatherDataUnit(temp,humi,wspend,precip,pressure,weather);
		    }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return wdu;
	}
	
	/**
	 * 删除一个city一个stamptime的记录
	 * @param city
	 * @param timestamp
	 * @return
	 */
	public Boolean delete(String city,Date timestamp){
		Delete del = new Delete(generateRowKey(city, timestamp));
		try {
			hTable.delete(del);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
		
	}

	
}


	
	
	

