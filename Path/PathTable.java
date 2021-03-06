package Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import Common.AbstractTable;

import collection.Constants;
/**
 * 轨迹表
 * @author leeying
 *
 */
public class PathTable extends AbstractTable{
	// 最大版本数
	private static final int MAX_VERSION_NUM = 1000;

	private static final String tableName = "path";

	private static final byte[] INFO_CF = "info".getBytes();
	// 轨迹运行中经历的区域列表
	private static final byte[] REGSEQ = "regions".getBytes();

	public PathTable() {
		try {
			HBaseAdmin hAdmin = new HBaseAdmin(Constants.conf);
			if (hAdmin.tableExists(tableName)) {
			     //hAdmin.disableTable(tableName);
			     //hAdmin.deleteTable(tableName);
			}
			else{
				// 设置版本保存策略
				HColumnDescriptor des = new HColumnDescriptor(INFO_CF);
				//des.setTimeToLive(HColumnDescriptor.DEFAULT_TTL);
				des.setMaxVersions(MAX_VERSION_NUM);
				HTableDescriptor t = new HTableDescriptor(tableName);
				t.addFamily(des);
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
	 * 计算行键
	 * @param tid
	 * @return
	 */
	public byte[] generateRowKey(int tid){
		return Bytes.toBytes(tid);
	}

	/**
	 * 为轨迹tid添加区域
	 * 
	 * @param tid
	 * @param timestamp 用来判断region间的前后顺序，可以为null，但当为null的时候必须保证调用此函数region的顺序
	 * @param region
	 */
	public Boolean setRegion(int tid, Date timestamp,String region) {
		byte[] rowKey = generateRowKey(tid);
		List<KeyValue> kvs = new ArrayList<KeyValue>();
		kvs.add(new KeyValue(rowKey,INFO_CF, REGSEQ, region.getBytes()));
		
		return this.put(rowKey, timestamp, kvs);
	}
	
	/**
	 * 获得轨迹经过的region列表
	 * 没有则返回null
	 * @param tid
	 * @return
	 */
	public List<String> getRegionSequence(int tid) {
		List<byte[]> cfList = new ArrayList<byte[]>();
		cfList.add(INFO_CF);
		Result r = this.get(generateRowKey(tid), null, cfList, null, true);

		List<String> results = new ArrayList<String>();
		if (r != null) {
			if (!r.isEmpty()) {
				List<KeyValue> kvs = r.list();
				for (KeyValue kv : kvs) {
					results.add(0, new String(kv.getValue()));
				}
			}
		}

		if (results.isEmpty()) {
			return null;
		}

		return results;
	}
	
	/**
	 * 删除
	 * @param tid
	 */
	public void delete(int tid){
		//System.out.println("delete " + new String(generateRowKey(tid)));
		Delete del = new Delete(generateRowKey(tid));
		try {
			hTable.delete(del);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
