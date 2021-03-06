package Pic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import collection.Constants;


public class MonitorSiteTable extends CommonOperate {
	// 实例
	//private static MonitorSiteTable instance = null;

	// 表名
	private static final String tableName = "monitor";

	// 列名
	private final static byte[] FPM = "fpm".getBytes();
	private final static byte[] CPM = "cpm".getBytes();
	private final static byte[] AQI = "aqi".getBytes();

	private HTable hTable = null;

	@Override
	public HTable getHTable() {
		return hTable;
	}

	// 获得唯一的实例
	/*public static MonitorSiteTable getInstance() {
		if (instance == null) {
			instance = new MonitorSiteTable();
		}
		return instance;
	}*/

	// 构造函数
	public MonitorSiteTable() {
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
	 * @param siteId
	 * @param timestamp
	 * @return
	 */
	private byte[] generateRowKey(String city, int siteId, Date timestamp) {
		// rowkey 是城市+观测点ID+时间戳
		String rowKey = city.toLowerCase() + Constants.SEPARATER + siteId
				+ Constants.SEPARATER + timestamp.getTime();
		return rowKey.getBytes();
	}

	/**
	 * 
	 * 存储观测点的信息
	 * 
	 * @param city
	 *            城市名
	 * @param siteId
	 *            观测点ID
	 * @param timestamp
	 *            时间戳
	 * @param fpm
	 *            pm2.5浓度
	 * @param cpm
	 *            pm10浓度
	 * @param aqi
	 *            AQI指数
	 * @return true代表存储成功
	 */
	public Boolean set(String city, int siteId, Date timestamp, int fpm,
			int cpm, int aqi) {
		Put put = new Put(generateRowKey(city, siteId, timestamp));
		put.add(COLFAM_NAME, FPM, Bytes.toBytes(fpm));
		put.add(COLFAM_NAME, CPM, Bytes.toBytes(cpm));
		put.add(COLFAM_NAME, AQI, Bytes.toBytes(aqi));
		try {
			hTable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
			// System.out.println("插入不成功！"+e.getMessage());
			return false;
		}

		return true;
	}

	/**
	 * 删除记录
	 * 
	 * @param city
	 * @param siteId
	 * @param timestamp
	 */
	public Boolean delete(String city, int siteId, Date timestamp) {
		Delete del = new Delete(generateRowKey(city, siteId, timestamp));
		try {
			hTable.delete(del);
		} catch (IOException e) {
			e.printStackTrace();
			// System.out.println("删除不成功！"+e.getMessage());
			return false;
		}

		return true;

	}

	/**
	 * 读取与时间戳之前或之后的最接近整点的观测信息,没有则返回NULL
	 * 
	 * @param city
	 *            城市名
	 * @param siteId
	 *            观测点ID
	 * @param timestamp
	 *            时间戳
	 * @return
	 */
	public MonitorSiteDataUnit getNearst(String city, int siteId, Date timestamp) {
		MonitorSiteDataUnit msdu = null;
		try {
			// 找到最近的结果
			Result result = findNearestValidRecord(city.toLowerCase()
					+ Constants.SEPARATER + siteId, timestamp);
			// 解析结果
			if (result != null && !result.isEmpty()) {
				NavigableMap<byte[], byte[]> map = result
						.getFamilyMap(COLFAM_NAME);
				// TODO:还没考虑没有数据的情况呢
				return new MonitorSiteDataUnit(map.get(FPM), map.get(CPM),
						map.get(AQI));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return msdu;

	}

	public static void main(String[] argv) {
		try {
			MonitorSiteTable mst = new MonitorSiteTable();
			// 插入
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			Date timestamp1 = sdf.parse("2014-05-06 16:00:00");
			mst.set("Beijing", 1, timestamp1, 16, 16, 16);
			Date timestamp2 = sdf.parse("2014-05-06 17:00:00");
			mst.set("Beijing", 1, timestamp2, 17, 17, 17);
			Date timestamp3 = sdf.parse("2014-05-06 18:00:00");
			mst.set("Beijing", 1, timestamp3, 18, 18, 18);

			Date queryTime = sdf.parse("2014-05-06 16:29:59");
			MonitorSiteDataUnit msdu = mst.getNearst("BEIJING", 1, queryTime);
			if (msdu != null) {
				System.out.println(msdu.toString());
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
