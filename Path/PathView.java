package Path;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import Common.Column;
import collection.CollectionTable;
import collection.Constants;

/**
 * 用户轨迹模块的视图
 * @author leeying
 *
 */
public class PathView{
	
	public CollectionTable ct = null;

	public PathView() {
		ct = new CollectionTable();
	}

	/**
	 * 从CollectionTable获得所有的用户上传的<lon,lat,tid,timestamp>数据
	 * 没有则返回null
	 * 
	 * @return
	 */
	public List<PathDataUnit> getAllLocatedRecord() {
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

		// 过滤掉tid列为空值
		SingleColumnValueFilter tid_f = new SingleColumnValueFilter(
				CollectionTable.INFO_CF, Constants.TID.getBytes(),
				CompareFilter.CompareOp.NOT_EQUAL, (byte[]) null);
		tid_f.setFilterIfMissing(true);

		List<Column> columnList = new ArrayList<Column>();
		columnList.add(new Column(CollectionTable.INFO_CF, Constants.LON
				.getBytes()));
		columnList.add(new Column(CollectionTable.INFO_CF, Constants.LAT
				.getBytes()));
		columnList.add(new Column(CollectionTable.INFO_CF, Constants.TID
				.getBytes()));

		ResultScanner rs = ct.scan(null, null, columnList, new FilterList(
				lon_f, lat_f, tid_f));
		if (rs != null) {
			List<PathDataUnit> results = new ArrayList<PathDataUnit>();
			for (Result r : rs) {
				if (!r.isEmpty()) {
					Date timestamp = ct.retrieveTimeFromKey(new String(r.getRow()));
					byte[] lon = r.getValue(CollectionTable.INFO_CF,
							Constants.LON.getBytes());
					byte[] lat = r.getValue(CollectionTable.INFO_CF,
							Constants.LAT.getBytes());
					byte[] tid = r.getValue(CollectionTable.INFO_CF,
							Constants.TID.getBytes());

					PathDataUnit pdu = new PathDataUnit(Bytes.toDouble(lon),
							Bytes.toDouble(lat), Bytes.toInt(tid), timestamp);
					results.add(pdu);

				}
			}// end for
			rs.close();
			
			if(results.isEmpty()){
				return null;
			}
			
			return results;
		}

		return null;

	}
	
	
	
}



