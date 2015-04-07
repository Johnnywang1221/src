package Web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import net.sf.json.JSONObject;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import Common.AbstractTable;
import Common.Column;

import collection.Constants;

public class POITable extends AbstractTable {
	// 表名
	private static final String tableName = "poi_test";
	// 图片列族名
	public static final byte[] PIC_CF = "pic".getBytes();
	// 图片列名
	public static final byte[] PIC_COL = "pic".getBytes();
	// PM2.5列名
	public static final byte[] FPM_COL = "fpm".getBytes();
	// info列族名
	public static final byte[] INFO_CF = "info".getBytes();
	// POI信息列名
	public static final byte[] POI_INFO_COL = "poi_info".getBytes();

	// 图片数目
	public static int pic_num = 0;

	synchronized public int getPicNum() {
		return pic_num;
	}

	public POITable() {
		try {
			HBaseAdmin hAdmin = new HBaseAdmin(Constants.conf);
			if (hAdmin.tableExists(tableName)) {
			      // do nothing	
			}
			else{
				// 设置版本保存策略
				HColumnDescriptor des = new HColumnDescriptor(PIC_CF);
				des.setMaxVersions(Integer.MAX_VALUE);
				HColumnDescriptor des1 = new HColumnDescriptor(INFO_CF);
				HTableDescriptor t = new HTableDescriptor(tableName);
				t.addFamily(des);
				t.addFamily(des1);
				hAdmin.createTable(t);
			}
			hAdmin.close();
			
			hTable = new HTable(Constants.conf, tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 获得下一个存放图片的列名 因为图片列名的数目不能事先确定，所以需要生成下一个图片名
	 * 
	 * @return
	 */
	synchronized private byte[] nextPIC_COL(String poi, Date timestamp) {
		String colName = new String(PIC_COL) + ++pic_num;
		return colName.getBytes();
	}

	/**
	 * 生成行键
	 * 
	 * @param poi
	 * @return
	 */
	private byte[] generateRowKey(String poi) {
		return poi.getBytes();
	}

	/**
	 * 设置某POI某日期（精确到天）的fpm值
	 * 
	 * @param poi
	 * @param timestamp
	 * @param fpm
	 * @return
	 */
	public Boolean setDayFpm(String poi, Date timestamp, int fpm) {
		// 只要精确到天
		timestamp = new Date(timestamp.getYear(), timestamp.getMonth(),
				timestamp.getDate(), 0, 0, 0);

		// System.out.println("保存poi = "+poi+" Timestamp = " +
		// timestamp.getTime());

		List<KeyValue> kvs = new ArrayList<KeyValue>();
		kvs.add(new KeyValue(generateRowKey(poi), PIC_CF, FPM_COL, timestamp
				.getTime(), Bytes.toBytes(fpm)));
		return this.put(generateRowKey(poi), timestamp, kvs);
	}

	/**
	 * 获得某POI某日期（精确到天）的fpm值,没有则返回-1
	 * 
	 * @param poi
	 * @param timestamp
	 * @return
	 */
	public int getDayFpm(String poi, Date timestamp) {
		// 只要精确到天
		timestamp = new Date(timestamp.getYear(), timestamp.getMonth(),
				timestamp.getDate(), 0, 0, 0);

		// System.out.println("查询poi = "+poi+" Timestamp = " +
		// timestamp.getTime());

		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(PIC_CF, FPM_COL));
		Result r = this.get(generateRowKey(poi), timestamp, null, columnList,false);
		if (r != null) {
			byte[] val = null;
			if (!r.isEmpty()
					&& (val = r.getValue(PIC_CF, FPM_COL)) != null) {
				return Bytes.toInt(val);
			}
		}

		return -1;

	}

	/**
	 * 添加某POI某日期（精确到天）的图片
	 * 
	 * @param poi
	 * @param timestamp
	 * @param content
	 * @return
	 */
	public Boolean setPic(String poi, Date timestamp, byte[] content) {
		// 只要精确到天
		timestamp = new Date(timestamp.getYear(), timestamp.getMonth(),
				timestamp.getDate(), 0, 0, 0);
		// 存入图片的列名
		byte[] col = nextPIC_COL(poi, timestamp);
		List<KeyValue> kvs = new ArrayList<KeyValue>();
		kvs.add(new KeyValue(generateRowKey(poi), PIC_CF, col, timestamp
				.getTime(), content));
		/*
		 * System.out.println("写入POI = " + poi + "列=" + new String(col) +
		 * " Time = " + timestamp.getTime() + "图片=" + new String(content));
		 */
		return this.put(generateRowKey(poi), timestamp, kvs);

	}

	/**
	 * 获得符合poiPrefix的所有POI点某天的所有图片，没有则返回null
	 * 
	 * @param poi
	 * @return
	 */
	public Set<byte[]> getPic(String poiPrefix, Date timestamp) {
		// System.out.println("查询POI = " + poi + " Time = " +
		// timestamp.getTime()
		// + "的所有图片");
		// 只要精确到天
		timestamp = new Date(timestamp.getYear(), timestamp.getMonth(),
				timestamp.getDate(), 0, 0, 0);
		Scan scan = new Scan();
		scan.setStartRow(generateRowKey(poiPrefix));
		scan.setStopRow(generateRowKey(Integer.parseInt(poiPrefix) + 1 + ""));
		scan.addFamily(PIC_CF);
		scan.setTimeStamp(timestamp.getTime());
		
		try {
			ResultScanner rs = hTable.getScanner(scan);
			Set<byte[]> results = new HashSet<byte[]>();
			for(Result r : rs){
				for (KeyValue kv : r.list()) {
					if(!Bytes.toString(kv.getQualifier()).equals(Bytes.toString(FPM_COL))){
						results.add(kv.getValue());
					}
					
				}
			}
			rs.close();
			
			if(results.isEmpty()){
				return null;
			}
			
			return results;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;

	}

	/**
	 * 设置某poi的展示级别level
	 * 
	 * @param poi
	 * @param level
	 * @return
	 */
	public Boolean setPOIInfo(String poi, double lon,double lat,int min_level,int max_level) {
		//组合成JSON字符串
		JSONObject jsonobject = new JSONObject();
		jsonobject.accumulate("poi", poi);
		jsonobject.accumulate("lon", lon);
		jsonobject.accumulate("lat", lat);
		jsonobject.accumulate("min_level", min_level);
		jsonobject.accumulate("max_level", max_level);
		
		List<KeyValue> kvs = new ArrayList<KeyValue>();
		kvs.add(new KeyValue(generateRowKey(poi), INFO_CF, POI_INFO_COL, Bytes.toBytes(jsonobject.toString())));
		
		return this.put(generateRowKey(poi), null, kvs);

	}

	/**
	 * 获得所有POI的信息，没有则返回null
	 * 
	 * @param level
	 * @return
	 */
	public Set<JSONObject> getPOIsInfo() {
		// 过滤器
		/*SingleColumnValueFilter f = new SingleColumnValueFilter(INFO_CF,
				POI_INFO_COL, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(level));
		f.setFilterIfMissing(true);
		FilterList fl = new FilterList(f);*/
		// 设置扫描列
		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(INFO_CF, POI_INFO_COL));

		//扫描
		ResultScanner rs = this.scan(null, null, columnList, null);
		if (rs != null) {
			Set<JSONObject> results = new HashSet<JSONObject>();
			for (Result r : rs) {
				if (!r.isEmpty()) {
					results.add(JSONObject.fromObject(Bytes.toString(r.getValue(INFO_CF, POI_INFO_COL))));
				}
			}
			rs.close();
			// 为了返回null
			if(results.size() == 0){
				return null;
			}
			return results;
		}

		return null;

	}

	/**
	 * 删除POI
	 * 
	 * @param poi
	 * @return
	 */
	public Boolean deletePOI(String poi) {
		Delete del = new Delete(generateRowKey(poi));

		try {
			hTable.delete(del);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		POITable pt = new POITable();

		Date day1 = new Date();
		Date day2 = new Date();
		day2.setDate(day2.getDate() + 1);
		Date day3 = new Date();
		day3.setDate(day2.getDate() + 1);

		pt.setDayFpm("1", day1, 1);
		pt.setDayFpm("2", day1, 1);
		pt.setDayFpm("3", day1, 1);
		pt.setPic("1", day1, "pic1".getBytes());
		pt.setPic("1", day1, "pic2".getBytes());
		pt.setPic("1", day2, "pic3".getBytes());

		System.out.println(pt.getDayFpm("1", day1));
		System.out.println(pt.getDayFpm("2", day1));
		System.out.println(pt.getDayFpm("1", day2));

		System.out.println("day1:");
		Set<byte[]> pics = pt.getPic("1", day1);
		for (byte[] pic : pics) {
			System.out.println(new String(pic));
		}

		System.out.println("day2:");
		pics = pt.getPic("1", day2);
		for (byte[] pic : pics) {
			System.out.println(new String(pic));
		}
		
		System.out.println("day3:");
		pics = pt.getPic("1", day3);
		Assert.assertNull(pics);

		pt.setPOIInfo("1", 1,1,123,124);
		pt.setPOIInfo("2", 1,1,123,124);
		pt.setPOIInfo("3", 0,0,123,124);
		
		System.out.println("poi = 1");
		Set<JSONObject> pois = pt.getPOIsInfo();
		for(JSONObject poi :pois){
			System.out.println(poi.toString());
		}

	}

}
