package Fusion;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import Common.Column;

import collection.CollectionTable;
import collection.Constants;


public class FusionView {
	// 区域X坐标
	public static final byte[] X_COL = "x".getBytes();
	// 区域Y坐标
	public static final byte[] Y_COL = "y".getBytes();
	
	private CollectionTable ct = null;

	private GridTable gt = null;

	private GridComputeInterface gci = null;

	public FusionView(GridComputeInterface gci) {
		ct = new CollectionTable();
		gt = new GridTable();
		this.gci = gci;
	}

	/**
	 * 将CollectionTable表中所有数据根据lon，lat计算得到grid_x,grid_y
	 */
	public void computeGrid() {
		// 过滤掉lon列为空值
		SingleColumnValueFilter lon_f = new SingleColumnValueFilter(
				CollectionTable.INFO_CF, Constants.LON.getBytes(),
				CompareFilter.CompareOp.NOT_EQUAL, (byte[]) null);
		lon_f.setFilterIfMissing(true);

		// 过滤掉lat列为空值
		SingleColumnValueFilter lat_f = new SingleColumnValueFilter(
				CollectionTable.INFO_CF, Constants.LAT.getBytes(),
				CompareFilter.CompareOp.NOT_EQUAL, (byte[]) null);
		lat_f.setFilterIfMissing(true);
		// 设置扫描列
		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(CollectionTable.INFO_CF, Constants.LON.getBytes()));
		columnList.add(new Column(CollectionTable.INFO_CF, Constants.LAT.getBytes()));	
		
		ResultScanner rs = ct.scan(null, null, columnList, new FilterList(lon_f,lat_f));
		if(rs != null){
			//打开写缓存
			ct.setAutoFlush(false);
			for (Result r : rs) {
				if (!r.isEmpty()) {
					byte[] rowkey = r.getRow();
					byte[] lon = r.getValue(CollectionTable.INFO_CF,
							Constants.LON.getBytes());
					byte[] lat = r.getValue(CollectionTable.INFO_CF,
							Constants.LAT.getBytes());

					if (lon != null || lat != null) {
						// 计算Grid
						Grid grid = gci.computer(Bytes.toDouble(lon), Bytes.toDouble(lat));
						// 写入Grid
						List<KeyValue> kvs = new ArrayList<KeyValue>();
						kvs.add(new KeyValue(rowkey,CollectionTable.GRID_CF, X_COL,
								Bytes.toBytes(grid.getX())));
						kvs.add(new KeyValue(rowkey,CollectionTable.GRID_CF, Y_COL,
								Bytes.toBytes(grid.getY())));
						
						ct.put(rowkey, null, kvs);
					}

				}
			}
			//关闭扫描器
			rs.close();
			//关闭写缓存
			ct.flushCommit();
			ct.setAutoFlush(true);
		}
		
	}
	
	/**
	 * 查询某个Grid的所有光线值，没有则返回null
	 * @param grid
	 * @return
	 */
	public List<Integer> getLightInGrid(Grid grid){
		//System.out.println("查询"+grid+"的光线信息");
		// 设置过滤的列
		List<Column>columnList = new ArrayList<Column>();
		columnList.add(new Column(CollectionTable.INFO_CF, Constants.LIGHT.getBytes()));
		columnList.add(new Column(CollectionTable.GRID_CF, X_COL));
		columnList.add(new Column(CollectionTable.GRID_CF, Y_COL));
		
		// 过滤器
		int grid_x = grid.getX();
		int grid_y = grid.getY();
		SingleColumnValueExcludeFilter x_f = new SingleColumnValueExcludeFilter(
				CollectionTable.GRID_CF, X_COL,
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(grid_x));
		x_f.setFilterIfMissing(true);
		
		SingleColumnValueExcludeFilter y_f = new SingleColumnValueExcludeFilter(
				CollectionTable.GRID_CF, Y_COL,
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(grid_y));
		y_f.setFilterIfMissing(true);
		
		ResultScanner rs = ct.scan(null, null, columnList, new FilterList(x_f,
				y_f));
		if (rs != null) {
			List<Integer> results = new ArrayList<Integer>();
			for (Result r : rs) {
				byte[] val = null;
				if ((val = r.getValue(CollectionTable.INFO_CF,
						Constants.LIGHT.getBytes())) != null) {
					results.add(Bytes.toInt(val));
				}
			}
			rs.close();
			
			if(results.isEmpty()){
				return null;
			}
			return results;
		}

		return null;

	}
	
	
	
	
	/**
	 * 获得所有Grid,没有则返回null
	 * @return
	 */
	public Set<Grid> getAllGrid(){
		// 构造扫描所有具有Grid的数据的扫描器
		SingleColumnValueFilter f = new SingleColumnValueFilter(
				CollectionTable.GRID_CF,X_COL ,
				CompareFilter.CompareOp.NOT_EQUAL, (byte[]) null);
		f.setFilterIfMissing(true);
		// 设置扫描列族
		List<byte[]>cfList = new ArrayList<byte[]>();
		cfList.add(CollectionTable.GRID_CF);
		
		ResultScanner rs = ct.scan(null, cfList, null, new FilterList(f));
		if (rs != null) {
			Set<Grid> results = new HashSet<Grid>();
			for (Result r : rs) {
				//System.out.println("result = " + r.getRow());
				byte[] grid_x = r.getValue(CollectionTable.GRID_CF, X_COL);
				byte[] grid_y = r.getValue(CollectionTable.GRID_CF, Y_COL);

				results.add(new Grid(Bytes.toInt(grid_x), Bytes.toInt(grid_y)));
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
	 * 保存Grid的融合light值
	 * @param grid
	 * @param light
	 * @return
	 */
	public Boolean setGridLight(Grid grid, int light){
		return gt.setLight(grid, light);
	}
	
	/**
	 * 获得Grid的融合light值
	 * @param grid
	 * @return
	 */
	public int getGridLight(Grid grid){
		return gt.getLight(grid);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}



}
