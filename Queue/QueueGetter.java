package Queue;

import java.util.Date;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import collection.CollectionPicTable;
import collection.CollectionTable;
import collection.Constants;

import Queue.DataElem.DataType;

/**
 * 缓存队列"消费者"
 * 
 * @author leeying
 * 
 */
public class QueueGetter implements Runnable {
	// 图片压缩线程池
	/*ExecutorService compressThreadPool = Executors.newSingleThreadExecutor(); 
	
	// 图片压缩线程
	Callable<byte[]> compressThread = new Callable<byte[]>() {
		public byte[] call() throws Exception {
			byte[] content = null;
			while( (content = getPic()) == null){
				// do nothing
			}
			// 压缩过程差不多需要300多ms
			return compressImageByarray.compress(content);
		}
	};
	
	// 原图
	private byte[] originalPic = null;
	
	synchronized public void setPic(byte[] content){
		originalPic = content;
	}
	
	synchronized public byte[] getPic(){
		return originalPic;
	}*/
	
	
	// 缓存队列
	private SingletonDataQueue dataQueue;
	// 感应数据收集表
	private CollectionTable ct = null;
	// 图片收集表
	private CollectionPicTable cpt = null;

	static Logger logger = Logger.getLogger(QueueGetter.class);

	public QueueGetter(SingletonDataQueue dataQueue) {
		this.dataQueue = dataQueue;
		
		ct = new CollectionTable();
		// 开启写缓存
		ct.setAutoFlush(false);

		cpt = new CollectionPicTable();
	}

	@Override
	public void run() {
		while (true) {
			Date before = new Date();
			DataElem elem = dataQueue.getAndRemoveELem();
			if (elem != null) {
				// 内容是JSON格式
				if (elem.getDt() == DataType.JSON) {
					ct.setJSON(elem.getContentInString());
					Date after = new Date();
					logger.info("Deal with JSON string in "
							+ (after.getTime() - before.getTime())
							+ " ms ");

				}
				// 内容是图片格式
				else if (elem.getDt() == DataType.PIC) {
					// 图片划分
					/*
					 * List<byte[]> subPics = CollectionTable.splitPic(
					 * elem.getContent(), 1024 * 1024);
					 */
					
					//压缩图片
					//Future<byte[]> future = compressThreadPool.submit(compressThread);
					
					
					// 解析图片
					//JSONObject json = JSONObject.fromObject(EXIFParser
					//		.parse(elem.getContent()));
					
					// 存储图片
					JSONObject pic = JSONObject.fromObject(elem.getContentInString()).getJSONObject("pic");
					if (!pic.isNullObject()) {
							Date timestamp = new Date(pic.getLong(Constants.TIMESTAMP));
							
							//提取压缩结果
							/*byte[] compressContent = null;
							try {
								compressContent = future.get(100,TimeUnit.MILLISECONDS);
							} catch (InterruptedException | ExecutionException e) {								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (TimeoutException e) {
								e.printStackTrace();
							}*/
							
							cpt.setPic(pic.getDouble(Constants.LON),
									pic.getDouble(Constants.LAT), timestamp,
									elem.getUri(),null,
									pic.getInt(Constants.FPM));
						
					}
					Date after = new Date();
					logger.info("Deal with PIC" + " in"
							+ (after.getTime() - before.getTime()) + "ms");

				} else {
					logger.info(elem.getDt() + "is not a valid dataType");
				}

			}// end if

			// 队列空,等待
			else {
				try {
					// 最后要将writebuffer里的结果flush回去呀。。。
					ct.flushCommit();
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
			}

		}// end while

	}

}
