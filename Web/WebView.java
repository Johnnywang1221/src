package Web;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;

import Common.Column;

import collection.CollectionPicTable;

public class WebView{
	// 图片收集表
	private CollectionPicTable cpt = null;

	// POI计算
	private POIComputeInterface pci = null;

	// POI表
	private POITable pt = null;

	public WebView(POIComputeInterface pci) {
		this.cpt = new CollectionPicTable();
		this.pt = new POITable();
		this.pci = pci;
	}

	/**
	 * 计算并写入图片收集表中所有数据的POI结果
	 */
	public void computePOI(){
		// 扫描所有数据
		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(CollectionPicTable.INFO_CF, CollectionPicTable.FPM_COL));
		ResultScanner rs = cpt.scan(null, null, columnList, null);
		// 计算POI并写入
		if(rs != null){
			for (Result r : rs) {
				byte[] rowkey = r.getRow();
				double lon = cpt.retrieveLonFromRowKey(rowkey);
				double lat = cpt.retrieveLatFromRowKey(rowkey);
				if (lon != -1 && lat != -1) {
					String poi = pci.computePOI(lon, lat);
					//System.out.println("lon/lat=" + lon + "/" + lat "计算poi=" + poi);
					List<KeyValue> kvs = new ArrayList<KeyValue>();
					kvs.add(new KeyValue(rowkey,CollectionPicTable.POI_CF,
							CollectionPicTable.POI_COL, Bytes.toBytes(poi)));
					cpt.put(rowkey, null, kvs);
					
				}
			}
			rs.close();
		}
		
	}

	/**
	 * 扫描图片收集表获得所有POI值，没有则返回null
	 * 
	 * @return
	 */
	public Set<String> getAllPOI() {
		return cpt.getAllPOI();
	}

	/**
	 * 从图片收集表中获得poi里的所有Map<日期，当天PM2.5值列表> 其中日期精确到天数 没有则返回null
	 * 
	 * @param poi
	 * @return
	 */
	public Map<Date, List<Integer>> getPicFpmInPOI(String poi) {
		// 构造扫描所有具有POI的数据的扫描器
		SingleColumnValueExcludeFilter f = new SingleColumnValueExcludeFilter(
				CollectionPicTable.POI_CF, CollectionPicTable.POI_COL,
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(poi));
		f.setFilterIfMissing(true);
		// 设置扫描的列族
		List<byte[]> cfList = new ArrayList<byte[]>();
		cfList.add(CollectionPicTable.POI_CF);
		cfList.add(CollectionPicTable.INFO_CF);
		// 扫描结果
		ResultScanner rs = cpt.scan(null, cfList, null, new FilterList(f));
		if (rs != null) {
			Map<Date, List<Integer>> results = new HashMap<Date, List<Integer>>();
			for (Result r : rs) {
				//图片值
				byte[] content = r.getValue(CollectionPicTable.INFO_CF,
						CollectionPicTable.PIC_COL);
				//PM2.5值
				byte[] fpm = r.getValue(CollectionPicTable.INFO_CF,
						CollectionPicTable.FPM_COL);
				if (content != null && fpm != null) {
					Date timestamp = cpt.retrieveTimeFromRowKey(r.getRow());
					// 精确到天
					timestamp = new Date(timestamp.getYear(),
							timestamp.getMonth(), timestamp.getDate(), 0, 0, 0);
				
					// 图片和PM2.5写入到POI表中
					JSONObject pic = new JSONObject();
					pic.accumulate("pic", content);
					pic.accumulate("fpm", Bytes.toInt(fpm));
					setPic(poi, timestamp, Bytes.toBytes(pic.toString()));
					
					
					// 添加日期的fpm值
					List<Integer> fpms;
					if ((fpms = results.get(timestamp)) != null) {
						fpms.add(Bytes.toInt(fpm));
						results.put(timestamp, fpms);
					} else {
						fpms = new ArrayList<Integer>();
						fpms.add(Bytes.toInt(fpm));
						results.put(timestamp, fpms);
					}

				}
			}// end for
			rs.close();
			
			if(results.size() == 0){
				return null;
			}
			return results;
		}

		return null;

	}

	/**
	 * 设置某poi某天的PM2.5值
	 * 
	 * @param poi
	 * @param timestamp
	 * @param fpm
	 *            PM2.5值
	 * @return
	 */
	public Boolean setDayFpm(String poi, Date timestamp, int fpm) {
		return pt.setDayFpm(poi, timestamp, fpm);
	}

	/**
	 * 获得某POI某日期（精确到天）的fpm值,没有则返回-1
	 * 
	 * @param poi
	 * @param timestamp
	 * @return
	 */
	public int getDayFpm(String poi, Date timestamp) {
		return pt.getDayFpm(poi, timestamp);
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
		return pt.setPic(poi, timestamp, content);
	}

	/**
	 * 获得符合poiPrefix的所有POI点某天的所有图片，没有则返回null
	 * 
	 * @param poi
	 * @return
	 */
	public Set<byte[]> getPic(String poiPrefix, Date timestamp) {
		return pt.getPic(poiPrefix, timestamp);
	}

	/**
	 * 设置某poi的信息
	 * @param poi poi标识
	 * @param lon 经度
	 * @param lat 纬度
	 * @param min_level 显示级别区间
	 * @param max_level 
	 * @return
	 */

	public Boolean setPOIInfo(String poi, double lon,double lat,int min_level,int max_level) {
		return pt.setPOIInfo(poi,lon,lat,min_level,max_level);
	}

	/**
	 * 获得所有POI点的信息，没有则返回null
	 * 
	 * @param level
	 * @return
	 */
	public Set<JSONObject> getPOIsInfo() {
		return pt.getPOIsInfo();
	}

}
