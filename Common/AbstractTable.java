package Common;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;


public abstract class AbstractTable {
	// communite with hbase table
	// HTable对象对于客户端读写数据来说不是线程安全的，因此多线程时，要为每个线程单独创建复用一个HTable对象，不同对象间不要共享HTable对象使用，特别是在客户端auto
	// flash被置为false时，由于存在本地write buffer，可能导致数据不一致。
	protected HTable hTable = null;

	/**
	 * 存储
	 * 
	 * @param rowKey
	 *            行键
	 * @param timestamp
	 *            时间戳 
	 * @param kvs
	 * @return
	 */
	public Boolean put(byte[] rowKey, Date timestamp, List<KeyValue> kvs) {
		Put put = null;
		if (timestamp == null) {
			put = new Put(rowKey);
		} else {
			put = new Put(rowKey, timestamp.getTime());
		}

		try {
			for (KeyValue kv : kvs) {
				put.add(kv);
			}
			hTable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

		return true;
	}
	
	/**
	 * 查询，没有则返回null
	 * 
	 * @param rowKey
	 * @param cfList
	 * @param columnList
	 * @param setMaxVersion 取所有的版本
	 * @return
	 */
	public Result get(byte[] rowKey, Date timestamp
			,List<byte[]> cfList, List<Column> columnList, Boolean setMaxVersion) {
		Get get = new Get(rowKey);
		if(timestamp != null){
			get.setTimeStamp(timestamp.getTime());
		}
		// 添加列族
		if (cfList != null) {
			for (byte[] cf : cfList) {
				get.addFamily(cf);
			}
		}
		// 添加列
		if (columnList != null) {
			for (Column col : columnList) {
				get.addColumn(col.getCF_NAME(), col.getCOL_NAME());
			}
		}
		
		if(setMaxVersion){
			get.setMaxVersions();
		}
		
		try {
			// r永远不会为null呢
			Result r = hTable.get(get);
			return r;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;

	}
	
	/**
	 * 扫描，失败返回null
	 * @param timestamp
	 * @param cfList
	 * @param columnList
	 * @param f
	 * @return
	 */
	
	public ResultScanner scan(Date timestamp,List<byte[]> cfList, List<Column> columnList,
			FilterList f) {
		Scan scan = new Scan();
		if (cfList != null) {
			for (byte[] cf : cfList) {
				scan.addFamily(cf);
			}
		}
		if(columnList != null){
			for(Column c : columnList){
				scan.addColumn(c.getCF_NAME(), c.getCOL_NAME());
			}	
		}
		if(f != null){
			scan.setFilter(f);
		}
		
		if(timestamp != null){
			//scan.setTimeRange(0, timestamp.getTime());
		}
		
		try {
			ResultScanner rs =  hTable.getScanner(scan);
			return rs;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
		
	}
}
