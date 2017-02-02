import java.lang.*;
import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;

// the data structure of Server that is snapshoted to disk
class ServerDS implements java.io.Serializable{
	int commitN;
	HashMap<Integer, Commit> commitMap;
	public ServerDS(HashMap<Integer, Commit> commitMap, int commitN){
		this.commitMap=commitMap;
		this.commitN=commitN;
	}
}

// a class that saves the info of a certain commit
class Commit implements java.io.Serializable{
	public String filename;
	public byte[] img;
	public int num; // commit ID
	public int success; // number of successful votes
	public int fail; // number of failure votes
	public int userN; // number of userNodes
	public char decision; // 'N'-no decision yet, 'C'-commit, 'A'-abort
	public HashMap<String, Character> userResponse; // users' voting--'S':success, 'F'--failure
	public HashSet<String> users;
	public HashSet<String> ack;
	public Commit(String filename, byte[] img, int userN, HashSet users){
		this.filename=filename;
		this.img=img;
		this.num=0;
		this.success=0;
		this.fail=0;
		this.userN=userN;
		this.decision='N';
		this.users=users;
		this.ack=new HashSet<String>();
		userResponse=new HashMap<String, Character>();
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

// a thead to send prepare msg to a certain usernode 
class SendPrepare extends Thread
{
	ProjectLib.Message msg;
	ProjectLib PL;
	Commit commit;
	String user;

	SendPrepare (ProjectLib PL, ProjectLib.Message msg, Commit commit, String user)
	{
		this.PL = PL;
		this.msg = msg;
		this.commit = commit;
		this.user = user;
	}

	public void run ()
	{
		PL.sendMessage(msg);
		try{
			Thread.sleep(3000);
		}catch(Exception e){
			e.printStackTrace();
		}
		if(!commit.userResponse.containsKey(user)) {
			// timeout
			commit.fail++;
			commit.userResponse.put(user, 'F');
		}
	}
}

// a thead to send decision msg to a certain usernode 
class SendDecision extends Thread
{
	ProjectLib PL;
	ProjectLib.Message msg;
	HashMap<Integer, Commit> commitMap;
	int num;
	String user;

	SendDecision (ProjectLib PL, ProjectLib.Message msg, HashMap<Integer, Commit> commitMap, int num, String user)
	{
		this.PL = PL;
		this.msg = msg;
		this.commitMap = commitMap;
		this.num=num;
		this.user = user;
	}

	public void run ()
	{
		while(true){
			// send decision msg until receive users' ACK
			PL.sendMessage(msg);
			try{
				Thread.sleep(3000);
			}catch(Exception e){
				e.printStackTrace();
			}
			if(!commitMap.containsKey(num)){
				break;
			}
			if(commitMap.get(num).ack.contains(user)) {
				break;
			}
		}
	}
}

public class Server implements ProjectLib.CommitServing {
	private static HashMap<Integer, Commit> commitMap;
	private static int commitN = 0;
	private static ProjectLib PL;
	private static ServerDS serverDS;

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
		HashSet<String> users = new HashSet<String>();
		int len = sources.length;
		for(int i=0; i<len; i++) {
			String[] str = sources[i].split(":");
			if(hm.containsKey(str[0])){
				ArrayList<String> list = hm.get(str[0]);
				list.add(str[1]);
			}else{
				ArrayList<String> list = new ArrayList<String>();
				list.add(str[1]);
				hm.put(str[0], list);
				users.add(str[0]);
			}
		}

		Commit commit = new Commit(filename, img, hm.size(), users);
		addCommit(commit);
		saveSnapshot();

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
			}
			SendPrepare sendThread = new SendPrepare(PL, msg, commit, key);
			sendThread.start();
		}

		int n = 0;
		while(n<6 && (commit.fail+commit.success!=commit.userN)){
			try{
				Thread.sleep(500);
				n++;
			} catch(Exception e){
				e.printStackTrace();
			}
		}

		// timeout, abort
		if(commit.fail>0 || commit.fail+commit.success!=commit.userN){
			if(commit.decision=='N') server_abort(commit);
		}
	}

	// commit
	private static void server_abort(Commit commit){
		commit.decision='A';
		saveSnapshot();
		// send abort msg to usernodes
		for(String key : commit.users){
			String c = "abort"+Integer.toString(commit.num);
			ProjectLib.Message commit_msg = new ProjectLib.Message(key, c.getBytes() );
			SendDecision sendDecision = new SendDecision(PL, commit_msg, commitMap, commit.num, key);
			sendDecision.start();
		}
	}

	// abort
	private static void server_commit(Commit commit){
		commit.decision='C';
		saveSnapshot();
		try{
			FileOutputStream fos = new FileOutputStream(commit.filename);
			fos.write(commit.img);
			fos.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		PL.fsync();

		for(String key : commit.users){
			String c = "commit"+Integer.toString(commit.num);
			ProjectLib.Message commit_msg = new ProjectLib.Message(key, c.getBytes() );
			SendDecision sendDecision = new SendDecision(PL, commit_msg, commitMap, commit.num, key);
			sendDecision.start();
		}
	}

	// Snapshot to disk
	private synchronized static void saveSnapshot(){
		serverDS = new ServerDS(commitMap, commitN);
		try {
			FileOutputStream fos = new FileOutputStream("snapshot");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(serverDS);
			oos.close();
			PL.fsync();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// read snapshot from disk
	private static void readSnapshot(){
		try {
			FileInputStream fis = new FileInputStream("snapshot");
			ObjectInputStream ois = new ObjectInputStream(fis);
			Object snapshot_object = ois.readObject();
			if(snapshot_object!=null){
				serverDS = (ServerDS) snapshot_object;
				commitMap = serverDS.commitMap;
				commitN = serverDS.commitN;
				cleanUp();
			}
		} catch (FileNotFoundException e){
			// no snapshot
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// process unfinished requests before Server fails
	private static void cleanUp(){
		for(Map.Entry<Integer, Commit> entry : commitMap.entrySet()){
			int num=entry.getKey();
			Commit commit=entry.getValue();
			if(commit.decision=='N'){
				// abort
				for(String user:commit.users){
					String c = "abort"+Integer.toString(commit.num);
					ProjectLib.Message abort_msg = new ProjectLib.Message(user, c.getBytes() );
					SendDecision sendDecision = new SendDecision(PL, abort_msg, commitMap, commit.num, user);
					sendDecision.start();
				}
			}else if(commit.decision=='A'){
				// send abort msg to users that have not sent back ACK yet
				for(String user:commit.users){
					if(!commit.ack.contains(user)){
						String c = "abort"+Integer.toString(commit.num);
						ProjectLib.Message abort_msg = new ProjectLib.Message(user, c.getBytes() );
						SendDecision sendDecision = new SendDecision(PL, abort_msg, commitMap, commit.num, user);
						sendDecision.start();
					}
				}
			}else{
				// send commit msg to users that have not sent back ACK yet
				File file = new File(commit.filename);
				if(!file.exists()){
					// hasn't composed the image yet
					try{
						FileOutputStream fos = new FileOutputStream(commit.filename);
						fos.write(commit.img);
						fos.close();
					}catch(Exception e){
						e.printStackTrace();
					}
					PL.fsync();
				}
				for(String user:commit.users){
					if(!commit.ack.contains(user)){
						String c = "commit"+Integer.toString(commit.num);
						ProjectLib.Message commit_msg = new ProjectLib.Message(user, c.getBytes() );
						SendDecision sendDecision = new SendDecision(PL, commit_msg, commitMap, commit.num, user);
						sendDecision.start();
					}
				}
			}
		}
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		readSnapshot();
		if(commitMap==null) commitMap = new HashMap<Integer, Commit>();
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			String str = new String(msg.body, "UTF-8");
			String[] tmp = str.split("#");
			int num = Integer.parseInt(tmp[0]);
			String user = tmp[1];
			String ret = tmp[2];
			if(commitMap.containsKey(num)){
				Commit commit = commitMap.get(num);
				if(ret.equals("S")){ // usernode returns success
					commit.userResponse.put(user, 'S');
					commit.success++;
					if(commit.success==commit.userN) server_commit(commit);
				}else if(ret.equals("F")){	// usernode returns fail
					commit.userResponse.put(user, 'F');
					commit.fail++;
					if(commit.decision!='A')server_abort(commit);
				}else{ // usernode returns ACK
					commit.ack.add(user);
					if(commit.ack.size()==commit.userN){ // receive ACK from all usernodes
						commitMap.remove(num);
					}
				}
			}
		}
	}
}

