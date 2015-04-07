package collection;

import static org.junit.Assert.fail;

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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.Assert;

import Common.AbstractTable;
import Parser.JSONParser;
import Pic.WeatherTable;

/**
 * 收集的数据表
 * 
 * @author leeying
 * 
 */
public class CollectionTable extends AbstractTable{
	// 表名
	public static final String tableName = "collection";
	// 收集数据列族
	public static final byte[] INFO_CF = "info".getBytes();
	// 网格列族
	public static final byte[] GRID_CF = "grid".getBytes();
	
	/**
	 * 开启or关闭写缓存
	 * @param flag
	 */
	public void setAutoFlush(Boolean flag){
		if (flag == false) {
			// 开启客户端缓存
			hTable.setAutoFlush(false);
			// 默认为2M
			try {
				hTable.setWriteBufferSize(2 * 1024 * 1024);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			hTable.setAutoFlush(true);
		}
	}
	
	public void flushCommit(){
		try {
			hTable.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public CollectionTable() {
		try {
			hTable = new HTable(Constants.conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 根据行键提取用户名
	 * 
	 * @return
	 */
	public String retrieveUsernameFromKey(String key) {
		int index;
		if ((index = key.indexOf(Constants.SEPARATER)) != -1) {
			String username = key.substring(0, index);
			return username;
		}
		
		return null;
	}
	
	/**
	 * 根据行键提取时间戳
	 * @param key
	 * @return
	 */
	public Date retrieveTimeFromKey(String key){
		int index;
		if ((index = key.indexOf(Constants.SEPARATER)) != -1) {
			String timestamp = key.substring(index+1, key.length());
			long ts = Long.parseLong(timestamp);
			
			return new Date(ts);
		}
		
		return null;
	}
	
	/**
	 *  生成行键
	 * @param username
	 * @param timestamp
	 * @return
	 */
	public static byte[] generateRowKey(String username, long timestamp) {
		String rowKey = username + Constants.SEPARATER + timestamp;
		return rowKey.getBytes();
	}
	
	/**
	 * 导入伪数据
	 */
	public Set<byte[]> importFakeData(){
		File f = new File("resource/txt/tbl_traj_new.txt");
		List<Put> puts = new ArrayList<Put>();
		Set<byte[]> rowKeySet = new HashSet<byte[]>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			while(( line = br.readLine()) != null){
				String[] tokens = line.split("\t");
				String username = tokens[1];
				int tid = Integer.parseInt(tokens[2]);
				double lon = Double.parseDouble(tokens[3]);
				double lat = Double.parseDouble(tokens[4]);
				String time = tokens[5];
				int humi = Integer.parseInt(tokens[6]);
				int temp = Integer.parseInt(tokens[7]);
				int light = Integer.parseInt(tokens[8]);
				
				//System.out.println(username + "\t" + tid + "\t"+lon + "\t" + lat + "\t" + time + "\t" + humi + "\t" + temp + "\t" + light);
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				Date timestamp = sdf.parse(time);
				
				byte[] rowKey = CollectionTable.generateRowKey(username, timestamp.getTime());
				Put put = new Put(rowKey);
				put.add(CollectionTable.INFO_CF, Constants.LON.getBytes(), Bytes.toBytes(lon));
				put.add(CollectionTable.INFO_CF, Constants.LAT.getBytes(), Bytes.toBytes(lat));
				put.add(CollectionTable.INFO_CF, WeatherTable.HUMI, Bytes.toBytes(humi));
				put.add(CollectionTable.INFO_CF, WeatherTable.TEMP, Bytes.toBytes(temp));
				put.add(CollectionTable.INFO_CF, Constants.LIGHT.getBytes(), Bytes.toBytes(light));
				put.add(CollectionTable.INFO_CF, Constants.TID.getBytes(), Bytes.toBytes(tid));
				
				puts.add(put);
				rowKeySet.add(rowKey);
				
				//break;
			}
			
			//System.out.println("Puts num = " + puts.size());
			//System.out.println("Set num = " + rowKeySet.size());
			
			//打开写缓存
			hTable.setAutoFlush(false);
			Assert.assertFalse(hTable.isAutoFlush());
			//插入数据库
			hTable.put(puts);
			hTable.flushCommits();
			
			hTable.setAutoFlush(true);
			
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
		
		return rowKeySet;
		
	}
	
	public void setJSON(String content){
		// 解析JSON字符串
		List<Put> puts = JSONParser.parseJSON(content);
		try {
			hTable.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void delete(byte[] rowKey){
		Delete del = new Delete(rowKey);
		try {
			hTable.delete(del);
		} catch (IOException e) {
			fail();
		}
	}

}
