package RMI;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import Queue.QueueGetter;

public class QueueServer {
	private static final int TN = 20;
	// 端口号
	private static final int PORT = 1099;

	/**
	 * 启动 RMI 注册服务并进行对象注册
	 */
	public static void main(String[] argv) {
		try {
			// System.setProperty("java.rmi.server.hostname", "10.108.104.192");
			// 启动RMI注册服务，指定端口为1099　（1099为默认端口）
			// 也可以通过命令 ＄java_home/bin/rmiregistry 1099启动 但必须事先用RMIC生成一个stub类为它所用
			LocateRegistry.createRegistry(PORT);

			// 创建远程对象的一个或多个实例，下面是hello对象
			// 可以用不同名字注册不同的实例
			QueueInterface instance = new QueueInstance();

			// 把instance注册到RMI注册服务器上，命名为Queue
			// 如果要把hello实例注册到另一台启动了RMI注册服务的机器上
			// Naming.rebind("//192.168.1.105:1099/Hello",hello);
			Naming.rebind("rmi://10.108.107.92:1099/Queue", instance);
			
			// 开启读线程
			for (int i = 0; i < TN; i++) {
				QueueGetter getter = new QueueGetter(
						instance.getSingletonDataQueue());
				Thread t = new Thread(getter);
				t.start();
			}
			
			System.out.println("Queue Server is ready.");
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
}
