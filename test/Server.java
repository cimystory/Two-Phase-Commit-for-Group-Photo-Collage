import java.lang.*;
import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;

// a class that saves the info of a certain commit
class Commit{
	public int num;
	public int success;
	public int fail;
	public int userN;
	public HashMap<String, Character> users;
	public Commit(int userN){
		this.num=0;
		this.success=0;
		this.fail=0;
		this.userN=userN;
		users=new HashMap<String, Character>();
	}
}

// a class that saves the info of a certain msg
class Msg implements java.io.Serializable{
	int num;
	byte[] img;
	String[] sources;
	public Msg(int num, byte[] img, String[] sources){
		this.num=num;
		this.img=img;
		this.sources=sources;
	}
}

// a thead to send msg to a certain usernode 
class SendMsg extends Thread
{
	int T = 0; // at most resend msg T times
	ProjectLib.Message msg;
	ProjectLib PL;
	Commit commit;
	String user;
	SendMsg (ProjectLib PL, ProjectLib.Message msg, Commit commit, String user)
	{
		this.PL = PL;
		this.msg = msg;
		this.commit = commit;
		this.user = user;
	}
	public void run ()
	{
	  System.out.println ("Send msg to "+msg.addr);
	  PL.sendMessage(msg);
	  // while(T<5){ 
	  	try{
		  	Thread.sleep(1300);
	  	}catch(Exception e){
	  		e.printStackTrace();
	  	}
	  	if(commit.users.containsKey(user)) {
	  		System.out.println("Finish sending msg to "+msg.addr);
	  		System.exit(0);
	  	}
	  	// System.out.println ("Send msg to "+msg.addr+" for the "+(T+2)+" time");
	  	// PL.sendMessage(msg);
	  	// T++;
	  // }
	  commit.fail++;
	  commit.users.put(user, 'F');
	  System.out.println("Set fail by default! Fail: "+commit.fail+", Success: "+commit.success);
	}
}

public class Server implements ProjectLib.CommitServing {
	static HashMap<Integer, Commit> commitMap;
	static HashSet<String> deleted;

	private static HashSet<String> userReplies;
	private static int commitN = 0;
	private static long T0;
	private static ProjectLib PL;

	// serialize message
	public static byte[] serialize(Object msg) throws IOException {
        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
            try(ObjectOutputStream o = new ObjectOutputStream(b)){
                o.writeObject(msg);
            }
            return b.toByteArray();
        }
    }

    // add commit to commitMap
	public synchronized void addCommit(Commit commit){
		commit.num=commitN;
		commitMap.put(commitN, commit);
		commitN++;
	}

	// callback function that processes each commit
	public void startCommit( String filename, byte[] img, String[] sources ) {
		HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
		System.out.println( "Server: Got request to commit "+filename );
		int len = sources.length;
		System.out.println("source files: ");
		for(int i=0; i<len; i++) System.out.println(sources[i]);

		for(int i=0; i<len; i++) {
			// if the file has been deleted before
			if(deleted.contains(sources[i])){ 
				return;
			} 
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

		Commit commit = new Commit(hm.size());
		addCommit(commit);

		// send msg to usernodes
		for(Map.Entry<String, ArrayList<String>> entry : hm.entrySet()){
			String key = entry.getKey(); // get usernode id
			ArrayList<String> list = entry.getValue(); // get files list
			String[] userSources = new String[list.size()];
			userSources = list.toArray(userSources);
			Msg userMsg = new Msg(commit.num, img, userSources);
			ProjectLib.Message msg=null;
			try{
				msg = new ProjectLib.Message(key, serialize((Object) userMsg));
			}catch(Exception e){
				System.out.println("Error changing type!");
			}
			SendMsg sendThread = new SendMsg(PL, msg, commit, key);
			sendThread.start();
		}
		
		// wait until all the node send back voting results
		// while(commit.fail>0 || commit.fail+commit.success<commit.userN){
		// 	System.out.println("In Loop! Fail: "+commit.fail+", Success: "+commit.success+", userN: "+commit.userN);
		// 	if(commit.fail>0){
		// 		System.out.println("Abort commit "+commit.num);
		// 		// send abort msg to usernodes
		// 		for(String key : hm.keySet()){
		// 			String c = "abort"+Integer.toString(commit.num);
		// 			ProjectLib.Message commit_msg = new ProjectLib.Message(key, c.getBytes() );
		// 			PL.sendMessage(commit_msg);
		// 		}
		// 		commitMap.remove(commit.num);
		// 		return;
		// 	}
		// 	try{
		// 		Thread.sleep(300);
		// 	} catch(Exception e){
		// 		e.printStackTrace();
		// 	}
		// }

		while(commit.fail+commit.success!=commit.userN){
			System.out.println("In Loop! Fail: "+commit.fail+", Success: "+commit.success+", userN: "+commit.userN);
			try{
				Thread.sleep(300);
			} catch(Exception e){
				e.printStackTrace();
			}
		}

		System.out.println("Out of Loop! Fail: "+commit.fail+", Success: "+commit.success+", userN: "+commit.userN);

		if(commit.fail>0){
			System.out.println("Abort commit "+commit.num);
			// send abort msg to usernodes
			for(String key : hm.keySet()){
				String c = "abort"+Integer.toString(commit.num);
				ProjectLib.Message commit_msg = new ProjectLib.Message(key, c.getBytes() );
				PL.sendMessage(commit_msg);
			}
			commitMap.remove(commit.num);
			return;
		}

		System.out.println("Get all results for "+commit.num+"! success: "+commit.success+", fail: "+commit.fail+", userN: "+commit.userN);

		System.out.println("Commit "+commit.num+"!");
		for(String key : hm.keySet()){
			String c = "commit"+Integer.toString(commit.num);
			ProjectLib.Message commit_msg = new ProjectLib.Message(key, c.getBytes() );
			PL.sendMessage(commit_msg);
		}
		for(int i=0; i<len; i++) deleted.add(sources[i]);

		//really commit
		try{
			FileOutputStream fos = new FileOutputStream(filename);
			fos.write(img);
			fos.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Failed to copy!");
		}
		commitMap.remove(commit.num);

		commitMap.remove(commit.num);
		System.out.println("Commit END!!!");
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		commitMap = new HashMap<Integer, Commit>();
		deleted = new HashSet<String>();
		userReplies = new HashSet<String>();
		T0 = System.currentTimeMillis();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			String str = new String(msg.body, "UTF-8");
			System.out.println( "Server: Got message from "+msg.addr+"--content: "+str);
			if(!userReplies.add(str)) continue; // if server has received this msg before
			String[] tmp = str.split("#");
			int num = Integer.parseInt(tmp[0]);
			String user = tmp[1];
			String ret = tmp[2];
			if(commitMap.containsKey(num)){
				Commit commit = commitMap.get(num);
				if(ret.equals("S")){ // return success
					commit.users.put(user, 'S');
					commit.success++;
				}else{	// return fail
					commit.users.put(user, 'F');
					commit.fail++;
				}
			}
		}
	}
}

