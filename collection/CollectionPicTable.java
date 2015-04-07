package collection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;

import Common.AbstractTable;
import Common.Column;

/**
 * 收集的图片表
 * 
 * @author leeying
 * 
 */
public class CollectionPicTable extends AbstractTable{
	// 表名
	public static final String tableName = "pic";
	// 收集数据列族
	public static final byte[] INFO_CF = "info".getBytes();
	// POI列族
	public static final byte[] POI_CF = "poi".getBytes();

	// 图片内容列名
	public static final byte[] PIC_COL = "pic".getBytes();
	// 压缩图片内容
	public static final byte[] COMPRESS_COL = "compress".getBytes();
	// 图片PM2.5列名
	public static final byte[] FPM_COL = "fpm".getBytes();

	public static final byte[] POI_COL = "poi".getBytes();

	public CollectionPicTable() {
		try {
			hTable = new HTable(Constants.conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 生成行键
	 * 
	 * @param lon
	 * @param lat
	 * @param timestamp
	 * @return
	 */
	public static byte[] generateRowkey(double lon, double lat, Date timestamp) {
		String rowkey = timestamp.getTime() + Constants.SEPARATER +lon + Constants.SEPARATER + lat; 
		return rowkey.getBytes();
	}

	/**
	 * 从行键提取经度Lon 失败返回-1
	 * 
	 * @param rowKey
	 * @return
	 */
	public double retrieveLonFromRowKey(byte[] rowKey) {
		String str = new String(rowKey);
		int fromIndex;
		int toIndex;
		if ((fromIndex = str.indexOf(Constants.SEPARATER)) != -1
				&& (toIndex = str.lastIndexOf(Constants.SEPARATER)) != -1) {
			return Double.parseDouble(str.substring(fromIndex + 1, toIndex));
		} else {
			return -1;
		}
	}

	/**
	 * 从行键提取纬度Lat 失败返回-1
	 * 
	 * @param rowKey
	 * @return
	 */
	public double retrieveLatFromRowKey(byte[] rowKey) {
		String str = new String(rowKey);
		int index;
		if ((index = str.lastIndexOf(Constants.SEPARATER)) != -1) {
			return Double.parseDouble(str.substring(index + 1));
		}
		
		return -1;
	}

	/**
	 * 从行键提取时间戳 失败返回null
	 * 
	 * @param rowKey
	 * @return
	 */
	public Date retrieveTimeFromRowKey(byte[] rowKey) {	
		String str = new String(rowKey);
		int index;
		if ((index = str.indexOf(Constants.SEPARATER)) != -1) {
			long timestamp = Long.parseLong(str.substring(0, index));
			return new Date(timestamp);
		} 
		
		return null;

	}

	/**
	 * 保存（lon，lat）地点 timestamp时间 图片内容content和pm2.5值fpm
	 * 
	 * @param lon
	 * @param lat
	 * @param timestamp
	 * @param content		      原图 or 图片路径
	 * @param compressContent 压缩图片
	 * @param fpm	图片PM2.5值
	 * @return
	 */
	public Boolean setPic(double lon, double lat, Date timestamp,
			byte[] content, byte[] compressContent,int fpm) {
		byte[] rowKey = generateRowkey(lon, lat, timestamp);
		List<KeyValue> kvs = new ArrayList<KeyValue>();	
		kvs.add(new KeyValue(rowKey,INFO_CF, PIC_COL, content));
		kvs.add(new KeyValue(rowKey,INFO_CF, FPM_COL, Bytes.toBytes(fpm)));	
		if(compressContent != null){
			kvs.add(new KeyValue(rowKey,INFO_CF, COMPRESS_COL, compressContent));	
		}
		return this.put(rowKey, null, kvs);	
	}
	
	/**
	 * 获得图片,没有则返回null
	 * @param lon
	 * @param lat
	 * @param timestamp
	 * @return
	 */
	public byte[] getPic(double lon, double lat, Date timestamp) {
		// 设置查询列
		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(INFO_CF, PIC_COL));
		// 查询
		Result r = this.get(generateRowkey(lon, lat, timestamp), null, null,
				columnList,false);
		if (!r.isEmpty()) {
			return r.getValue(INFO_CF, PIC_COL);
		}

		return null;

	}
	
	
	/**
	 * 获得从beginTime至今的图片
	 * 没有则返回null
	 * @return
	 */
	public List<Pic> getPic(Date beginTime){
		//起始行键																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																								
		byte[] startRowKey = generateRowkey(0, 0, beginTime);
		Scan scan = new Scan();
		scan.addColumn(INFO_CF,PIC_COL);
		scan.setStartRow(startRowKey);
		
		try {
			ResultScanner rs = hTable.getScanner(scan);
			List<Pic> results = new ArrayList<Pic>();
			for(Result r: rs){
				byte[] val = null;
				if((val = r.getValue(INFO_CF, PIC_COL)) != null){
					Date timestamp = retrieveTimeFromRowKey(r.getRow());
					double lon = retrieveLonFromRowKey(r.getRow());
					double lat = retrieveLatFromRowKey(r.getRow());
					Pic pic = new Pic(timestamp,lon,lat,Bytes.toString(val));
					
					results.add(pic);
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
	 * 删除图片
	 * 
	 * @param rowKey
	 * @return
	 */
	public Boolean delPic(byte[] rowKey) {
		Delete del = new Delete(rowKey);
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
	 * 扫描获得所有POI值，没有则返回null
	 * 
	 * @return
	 */
	public Set<String> getAllPOI() {
		// 构造扫描所有具有POI的数据的扫描器
		SingleColumnValueFilter f = new SingleColumnValueFilter(POI_CF,
				POI_COL, CompareFilter.CompareOp.NOT_EQUAL, (byte[]) null);
		f.setFilterIfMissing(true);

		List<byte[]> cfList = new ArrayList<byte[]>();
		cfList.add(POI_CF);
		ResultScanner rs = this.scan(null, cfList, null, new FilterList(f));

		if (rs != null) {
			Set<String> results = new HashSet<String>();
			for (Result r : rs) {
				byte[] poi = null;
				if (!r.isEmpty()
						&& (poi = r.getValue(CollectionPicTable.POI_CF,
								CollectionPicTable.POI_COL)) != null) {
					results.add(Bytes.toString(poi));
				}
			}
			rs.close();

			if (results.isEmpty()) {
				return null;
			}
			return results;
		}

		return null;
	}

	
	/**
	 * 导入伪数据
	 */
	public void importFakeData(String basePath) {
		File f = new File("resource/txt/newimg_info.txt");
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				String filename = tokens[0] + ".jpg";
				double lon = Double.parseDouble(tokens[1]);
				double lat = Double.parseDouble(tokens[2]);
				String relativePath = tokens[3];
				int fpm = Integer.parseInt(tokens[4]);
				String time = tokens[6] + " 00:00:00";

				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy/MM/dd hh:mm:ss");
				Date timestamp = sdf.parse(time);

				String absolutePath = basePath.endsWith(Constants.SEPARATER) ? 
						basePath + relativePath + filename
						: basePath + Constants.SEPARATER + relativePath
								+ filename;
				//存储图片
				setPic(lon, lat, timestamp, absolutePath.getBytes(), null,
						fpm);
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		for(String arg:args){
			System.out.println(arg);
		}
		new CollectionPicTable().importFakeData("/home/ps/img/test");
		
	}

}
