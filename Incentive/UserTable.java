package Incentive;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class UserTable {
	// 表名
	private static final String tableName = "user";
	// 列族名
	private final static byte[] COLFAM_NAME = "info".getBytes();
	// 密码
	private final static byte[] PWD = "pwd".getBytes();
	// 权限
	private final static byte[] PRIO = "prio".getBytes();
	// 总共获得的激励
	private final static byte[] EARN = "earn".getBytes();
	// 参与次数
	private final static byte[] TIMES = "times".getBytes();
	// 推送token
	private final static byte[] TOKEN = "token".getBytes();
	
	//private static UserTable instance = null;

	private HTable hTable = null;
	
	/*public static UserTable getInstance(){
		if(instance == null){
			instance = new UserTable();
		}
		return instance;
	}*/
	
	public UserTable(){
		Configuration conf = HBaseConfiguration.create();
		try {
			hTable = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 存储用户基本信息
	 * @param username
	 * @param pwd
	 * @param priv
	 * @return
	 */
	public Boolean set(String username,String pwd,int priv,String token){
		Put put = new Put(username.getBytes());
		if(pwd!=null)
		put.add(COLFAM_NAME, PWD, pwd.getBytes());
		put.add(COLFAM_NAME,PRIO,Bytes.toBytes(priv));
		if(token!=null)
		put.add(COLFAM_NAME,TIMES,token.getBytes());
		
		try {
			hTable.put(put);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	/**
	 * 查找用户密码
	 * @param username
	 * @return
	 */
	public String getPwd(String username){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, PWD);
		
		try {
			Result r = hTable.get(get);
			if(!r.isEmpty()){
				return new String(r.getValue(COLFAM_NAME, PWD));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	
	
	/**
	 * 为用户username增加earn激励收入
	 * @param username
	 * @param earn
	 * @return
	 */
	// 其实可以写入不同的version呢！！！！！！！！！！！！！！！！！！！！！！！
	public Boolean setEarn(String username,double earn){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, EARN);
		try {
			Result r = hTable.get(get);
			// 有值，value += earn;
			if(r != null && !r.isEmpty()){
				double value = Bytes.toDouble(r.getValue(COLFAM_NAME, EARN));
				value += earn;
				
				//重新存回新值
				Put put = new Put(username.getBytes());
				put.add(COLFAM_NAME, EARN, Bytes.toBytes(value));
				hTable.put(put);
			}
			// 还没有值，value设为earn
			else{
				//重新存回新值
				Put put = new Put(username.getBytes());
				put.add(COLFAM_NAME, EARN, Bytes.toBytes(earn));
				hTable.put(put);
			}
			
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * 用户的参与次数+1
	 * @param username
	 * @param times
	 * @return
	 */
	// 其实可以写入不同的version呢！！！！！！！！！！！！！！！！！！！！！！！
	public Boolean setTimes(String username){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, TIMES);
		try {
			Result r = hTable.get(get);
			// 有值，value++;
			if(!r.isEmpty()){
				int value = Bytes.toInt(r.getValue(COLFAM_NAME, TIMES));
				value += 1;
				
				//重新存回新值
				Put put = new Put(username.getBytes());
				put.add(COLFAM_NAME, TIMES, Bytes.toBytes(value));
				hTable.put(put);
			}
			// 还没有值，value设为1
			else{
				//重新存回新值
				Put put = new Put(username.getBytes());
				put.add(COLFAM_NAME, TIMES, Bytes.toBytes(1));
				hTable.put(put);
			}
			
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	/**
	 * 为用户username增加earn激励收入，并将参与次数+1
	 * 推荐使用这个函数代替执行上者两个函数，因为只需一次查询和插入
	 * @param username
	 * @param earn
	 * @return
	 */
	public Boolean setEarnAndTimes(String username,double earn){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, EARN);
		get.addColumn(COLFAM_NAME, TIMES);
		try {
			Result r = hTable.get(get);
			// 有值，value += earn;
			if(r != null && !r.isEmpty()){
				double newEarn = Bytes.toDouble(r.getValue(COLFAM_NAME, EARN));
				newEarn += earn;
				
				int newTimes = Bytes.toInt(r.getValue(COLFAM_NAME, TIMES));
				newTimes += 1;
				
				//重新存回新值
				Put put = new Put(username.getBytes());
				put.add(COLFAM_NAME, EARN, Bytes.toBytes(newEarn));
				put.add(COLFAM_NAME, TIMES, Bytes.toBytes(newTimes));
				hTable.put(put);
			}
			// 还没有值，value设为earn
			else{
				//重新存回新值
				Put put = new Put(username.getBytes());
				put.add(COLFAM_NAME, EARN, Bytes.toBytes(earn));
				put.add(COLFAM_NAME, TIMES, Bytes.toBytes(1));
				hTable.put(put);
			}
			
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	/**
	 * 获得用户的总收益
	 * @param username
	 * @return
	 */
	public double getEarn(String username){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, EARN);
		
		try {
			Result r = hTable.get(get);
			if(!r.isEmpty()){
				byte[] earn;
				if((earn = r.getValue(COLFAM_NAME, EARN)) != null){
					return Bytes.toDouble(earn);
				}
				else{
					return (double)0;
				}
				
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (double)0;
	}
	
	/**
	 * 获得用户的参与次数
	 * @param username
	 * @return
	 */
	public int getTimes(String username){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, TIMES);
		
		try {
			Result r = hTable.get(get);
			if(!r.isEmpty()){
				byte[] times;
				if((times = r.getValue(COLFAM_NAME, TIMES)) != null){
					return Bytes.toInt(times);
				}
				else{
					return 0;
				}
				
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	public Boolean delete(String username){
		Delete del = new Delete(username.getBytes());
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
	 * 获得用户的推送token
	 * @param username
	 * @return
	 */
	public String getToken(String username){
		Get get = new Get(username.getBytes());
		get.addColumn(COLFAM_NAME, TOKEN);
		
		try {
			Result r = hTable.get(get);
			if(!r.isEmpty()){
				return new String(r.getValue(COLFAM_NAME, TOKEN));			
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	public Boolean setToken(String userName, String token) {
		Put put = new Put(userName.getBytes());
		put.add(COLFAM_NAME, TOKEN, token.getBytes());
		try {
			hTable.put(put);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	
	
	
	

}
