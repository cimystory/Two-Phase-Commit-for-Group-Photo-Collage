import java.lang.*;
import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;

class Com{
	int num;
	long start;
	long end;
	boolean abort;
	public Com(long start){
		this.start=start;
		this.num=0;
		this.end=0;
		this.abort=false;
	}
}

public class Server implements ProjectLib.CommitServing {
	private static ArrayList<Com> commitList;
	private static long T0;

	private static ProjectLib PL;

	public synchronized void addCommit(Com commit){
		int n = commitList.size();
		commit.num=n;
		commitList.add(commit);
	}
	
	public void startCommit( String filename, byte[] img, String[] sources ) {
		HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
		System.out.println( "Server: Got request to commit "+filename );
		int len = sources.length;
		String[] path = filename.split("/");
		System.out.println("path[0]:"+path[0]+", path[1]:"+path[1]);
		// HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
		for(int i=0; i<len; i++) {
			System.out.println(sources[i]);
			String[] str = sources[i].split(":");
			if(hm.containsKey(str[0])){
				ArrayList<String> list = hm.get(str[0]);
				list.add(str[1]);
			}else{
				ArrayList<String> list = new ArrayList<String>();
				list.add(str[1]);
				hm.put(str[0], list);
			}
		}

		long start = System.currentTimeMillis()-T0;
		Com commit = new Com(start);
		addCommit(commit);

		
		for(Map.Entry<String, ArrayList<String>> entry : hm.entrySet()){
			String key = entry.getKey();
			ArrayList<String> list = entry.getValue();
			StringBuilder sb = new StringBuilder();
			sb.append(Integer.toString(commit.num));
			for(int i=0; i<list.size(); i++){
				sb.append("#");
				sb.append(list.get(i));
			}
			// send image byte[] to usernode
			ProjectLib.Message msg = new ProjectLib.Message(key, img);
			PL.sendMessage(msg);
			// send filepaths to usernode
			msg = new ProjectLib.Message(key, sb.toString().getBytes());
			PL.sendMessage(msg);
		}
		
		try{
			Thread.sleep(5000);
		} catch(Exception e){
			System.out.println("Error!");
		}

		if(!commit.abort && commit.end!=0){
			// really commit
			try{
				System.out.println("Start copying!");
				File source = new File(filename);
				File dest = new File("Server/"+path[1]);
				FileChannel inputChannel = null;
    			FileChannel outputChannel = null;
    			inputChannel = new FileInputStream(source).getChannel();
    			outputChannel = new FileOutputStream(dest).getChannel();
    			outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
    			inputChannel.close();
    			outputChannel.close();
			}catch(Exception e){
				System.out.println("Failed to copy!");
			}
		}
	}
	
	public static void main ( String args[] ) throws Exception {
		commitList = new ArrayList<Com>();
		T0 = System.currentTimeMillis();
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			System.out.println( "Server: Got message from " + msg.addr );
			// System.out.println( "Server: Echoing message to " + msg.addr );
			// PL.sendMessage( msg );
		}
	}
}

