package Fusion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import Common.AbstractTable;
import Common.Column;

import collection.Constants;

public class GridTable extends AbstractTable {
	
	private static final String tableName = "grid";
	
	private static final byte[] INFO_CF = "info".getBytes();
	
	public GridTable() {
		try {
			hTable = new HTable(Constants.conf, tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 生成行键
	 * @param grid
	 * @return
	 */
	private byte[] generateRowKey(Grid grid){
		return generateRowKey(grid.getX(),grid.getY());
	}
	
	/**
	 * 生成行键
	 * @param grid_x
	 * @param grid_y
	 * @return
	 */
	private byte[] generateRowKey(int grid_x,int grid_y){
		String rowKey = grid_x + Constants.SEPARATER + grid_y;
		return rowKey.getBytes();
	}
	
	/**
	 * 写入区域Grid的融合后光线值为light
	 * @param grid
	 * @param light
	 * @return
	 */
	public Boolean setLight(Grid grid,int light){
		byte[] rowKey = generateRowKey(grid);
		List<KeyValue> kvs = new ArrayList<KeyValue>();
		kvs.add(new KeyValue(rowKey,INFO_CF, Constants.LIGHT.getBytes(), Bytes.toBytes(light)));
		return this.put(rowKey, null, kvs);
		
	}
	
	/**
	 * 获得区域Grid的光线值
	 * 没有返回-1
	 * @param grid
	 * @return
	 */
	public int getLight(Grid grid) {
		// 查找
		byte[] rowkey = generateRowKey(grid);
		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(INFO_CF, Constants.LIGHT.getBytes()));
		Result r = this.get(rowkey, null, null, columnList,false);

		if (r != null) {
			byte[] light = null;
			if (!r.isEmpty()
					&& (light = r.getValue(INFO_CF, Constants.LIGHT.getBytes())) != null) {
				return Bytes.toInt(light);
			} else {
				return -1;
			}
		}

		return -1;

	}
	
	/**
	 * 删除区域grid的数据
	 * @param grid
	 * @return
	 */
	public Boolean delete(Grid grid){
		Delete del = new Delete(generateRowKey(grid));	
		try {
			hTable.delete(del);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GridTable gt = new GridTable();
		Grid grid = new Grid(1,1);
		gt.setLight(grid, 1);
		System.out.println(gt.getLight(grid));
		gt.delete(grid);
		System.out.println(gt.getLight(grid));
		gt.setLight(grid, 2);
		System.out.println(gt.getLight(grid));

	}

}
