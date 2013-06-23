package test.hadoop.util;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
/***
 * HDFS 工具类
 * @author zhaidw
 *
 */
public class HDFSUtil {
	private static Logs log = LogUtil.getLog(HDFSUtil.class);
	public synchronized static FileSystem getFileSystem(String ip, int port) {
		FileSystem fs = null;
		String url = "hdfs://" + ip + ":" + String.valueOf(port);
		Configuration config = new Configuration();
		config.set("fs.default.name", url);
		try {
			fs = FileSystem.get(config);
		} catch (Exception e) {
			log.error("getFileSystem failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
		return fs;
	}
	public synchronized static void listNode(FileSystem fs) {
		DistributedFileSystem dfs = (DistributedFileSystem) fs;
		try {
			DatanodeInfo[] infos = dfs.getDataNodeStats();
			for (DatanodeInfo node : infos) {
				System.out.println("HostName: " + node.getHostName() + "/n"
						+ node.getDatanodeReport());
				System.out.println("--------------------------------");
			}
		} catch (Exception e) {
			log.error("list node list failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	/**
	 * 打印系统配置
	 * 
	 * @param fs
	 */
	public synchronized static void listConfig(FileSystem fs) {
		Iterator<Entry<String, String>> entrys = fs.getConf().iterator();
		while (entrys.hasNext()) {
			Entry<String, String> item = entrys.next();
			log.info(item.getKey() + ": " + item.getValue());
		}
	}
	/**
	 * 创建目录和父目录
	 * 
	 * @param fs
	 * @param dirName
	 */
	public synchronized static void mkdirs(FileSystem fs, String dirName) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		String dir = workDir + "/" + dirName;
		Path src = new Path(dir);
		// FsPermission p = FsPermission.getDefault();
		boolean succ;
		try {
			succ = fs.mkdirs(src);
			if (succ) {
				log.info("create directory " + dir + " successed. ");
			} else {
				log.info("create directory " + dir + " failed. ");
			}
		} catch (Exception e) {
			log.error("create directory " + dir + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	/**
	 * 删除目录和子目录
	 * 
	 * @param fs
	 * @param dirName
	 */
	public synchronized static void rmdirs(FileSystem fs, String dirName) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		String dir = workDir + "/" + dirName;
		Path src = new Path(dir);
		boolean succ;
		try {
			succ = fs.delete(src, true);
			if (succ) {
				log.info("remove directory " + dir + " successed. ");
			} else {
				log.info("remove directory " + dir + " failed. ");
			}
		} catch (Exception e) {
			log.error("remove directory " + dir + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	/**
	 * 上传目录或文件
	 * 
	 * @param fs
	 * @param local
	 * @param remote
	 */
	public synchronized static void upload(FileSystem fs, String local,
			String remote) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + remote);
		Path src = new Path(local);
		try {
			fs.copyFromLocalFile(false, true, src, dst);
			log.info("upload " + local + " to  " + remote + " successed. ");
		} catch (Exception e) {
			log.error("upload " + local + " to  " + remote + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	/**
	 * 下载目录或文件
	 * 
	 * @param fs
	 * @param local
	 * @param remote
	 */
	public synchronized static void download(FileSystem fs, String local,
			String remote) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + remote);
		Path src = new Path(local);
		try {
			fs.copyToLocalFile(false, dst, src);
			log.info("download from " + remote + " to  " + local
					+ " successed. ");
		} catch (Exception e) {
			log.error("download from " + remote + " to  " + local + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	/**
	 * 字节数转换
	 * 
	 * @param size
	 * @return
	 */
	public synchronized static String convertSize(long size) {
		String result = String.valueOf(size);
		if (size < 1024 * 1024) {
			result = String.valueOf(size / 1024) + " KB";
		} else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
			result = String.valueOf(size / 1024 / 1024) + " MB";
		} else if (size >= 1024 * 1024 * 1024) {
			result = String.valueOf(size / 1024 / 1024 / 1024) + " GB";
		} else {
			result = result + " B";
		}
		return result;
	}
	/**
	 * 遍历HDFS上的文件和目录
	 * 
	 * @param fs
	 * @param path
	 */
	public synchronized static void listFile(FileSystem fs, String path) {
		Path workDir = fs.getWorkingDirectory();
		Path dst;
		if (null == path || "".equals(path)) {
			dst = new Path(workDir + "/" + path);
		} else {
			dst = new Path(path);
		}
		try {
			String relativePath = "";
			FileStatus[] fList = fs.listStatus(dst);
			for (FileStatus f : fList) {
				if (null != f) {
					relativePath = new StringBuffer()
							.append(f.getPath().getParent()).append("/")
							.append(f.getPath().getName()).toString();
					if (f.isDir()) {
						listFile(fs, relativePath);
					} else {
						System.out.println(convertSize(f.getLen()) + "/t/t"
								+ relativePath);
					}
				}
			}
		} catch (Exception e) {
			log.error("list files of " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		} finally {
		}
	}
	public synchronized static void write(FileSystem fs, String path,
			String data) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + path);
		try {
			FSDataOutputStream dos = fs.create(dst);
			dos.writeUTF(data);
			dos.close();
			log.info("write content to " + path + " successed. ");
		} catch (Exception e) {
			log.error("write content to " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	public synchronized static void append(FileSystem fs, String path,
			String data) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + path);
		try {
			FSDataOutputStream dos = fs.append(dst);
			dos.writeUTF(data);
			dos.close();
			log.info("append content to " + path + " successed. ");
		} catch (Exception e) {
			log.error("append content to " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}
	public synchronized static String read(FileSystem fs, String path) {
		String content = null;
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + path);
		try {
			// reading
			FSDataInputStream dis = fs.open(dst);
			content = dis.readUTF();
			dis.close();
			log.info("read content from " + path + " successed. ");
		} catch (Exception e) {
			log.error("read content from " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
		return content;
	}
}
