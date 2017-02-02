import java.lang.*;
import java.util.*;
import java.io.*;

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

// the data structure of user node that is snapshoted to disk
class UserDS implements java.io.Serializable{
	HashSet<String> deleted;
	HashMap<Integer, ArrayList<String>> locked_files;
	HashSet<String> decisions;
	public UserDS(HashSet<String> deleted, HashMap<Integer, ArrayList<String>> locked_files,
		HashSet<String> decisions){
		this.deleted=deleted;
		this.locked_files=locked_files;
		this.decisions=decisions;
	}
}

public class UserNode implements ProjectLib.MessageHandling {
	private static String myId;
	private static ProjectLib PL;
	private static HashSet<String> deleted;
	private static HashMap<Integer, ArrayList<String>> locked_files;
	private static HashSet<String> serverMsg;
	private static UserDS userDS;
	private static HashSet<String> decisions;

	public UserNode( String id ) {
		myId = id;
	}

	// deserialize byte array to an object
	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
            try(ObjectInputStream o = new ObjectInputStream(b)){
                return o.readObject();
            }
        }
    }

    // check if source files are locked or nonexist
    public static boolean checkFiles(String[] files){
    	for(int i=0; i<files.length; i++){
    		if(deleted.contains(files[i])) return false;
	    	File f = new File(files[i]);
	    	if(!f.exists()) {
	    		return false;
	    	}
    	}
    	return true;
    }

    // a callback method that receives messages from Server
	public boolean deliverMessage( ProjectLib.Message msg ) {
		String str = null;
		// if it's a decision msg sent from server
		try{
			str = new String(msg.body, "UTF-8");
			if(str.indexOf("commit")!=-1 || str.indexOf("abort")!=-1){
				decisions.add(str);
				saveSnapshot();
				int id=0;
				if(str.indexOf("commit")!=-1){
					id = Integer.parseInt(str.substring(6));
					if(locked_files.containsKey(id)){
						ArrayList<String> list = locked_files.get(id);
						for(int i=0; i<list.size(); i++){
							// delete those files
							File f = new File(list.get(i));
							try{
								f.delete();
							}catch(Exception e){
								e.printStackTrace();
							}
						}
						locked_files.remove(id);
					}
				}else if(str.indexOf("abort")!=-1){
					id = Integer.parseInt(str.substring(5));
					if(locked_files.containsKey(id)){
						ArrayList<String> list = locked_files.get(id);
						for(int i=0; i<list.size(); i++){
							deleted.remove(list.get(i)); // restore those files
						}
						locked_files.remove(id);
					}
				}
				decisions.remove(str);
				saveSnapshot();
				str = id+"#"+myId+"#A";
				ProjectLib.Message ackMsg = new ProjectLib.Message("Server", str.getBytes());
				PL.sendMessage(ackMsg);
				return true;				
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		// if it's a commit request msg sent from server
		Msg userMsg=null;
		try{
			userMsg = (Msg) deserialize(msg.body);
		}catch(Exception e){
			e.printStackTrace();
		}

		// send result of askUser back to server
		ProjectLib.Message newmsg = null; 
		if(locked_files.containsKey(userMsg.num)){ // if the commit has been processed but 
			// the server didn't receive the msg
			str = Integer.toString(userMsg.num)+"#"+myId+"#S";
		}else if(!checkFiles(userMsg.sources) || !PL.askUser(userMsg.img, userMsg.sources)){
			str = Integer.toString(userMsg.num)+"#"+myId+"#F";
		}else{
			str = Integer.toString(userMsg.num)+"#"+myId+"#S";
			ArrayList<String> list = new ArrayList<String>();
			for(int i=0; i<userMsg.sources.length; i++){
				deleted.add(userMsg.sources[i]);
				list.add(userMsg.sources[i]);
			}
			locked_files.put(userMsg.num, list);
			saveSnapshot();
		}
		newmsg = new ProjectLib.Message("Server", str.getBytes());
		PL.sendMessage(newmsg);
		return true;
	}

	// Snapshot to disk
	private synchronized static void saveSnapshot(){
		userDS = new UserDS(deleted, locked_files, decisions);
		try {
			FileOutputStream fos = new FileOutputStream("snapshot");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(userDS);
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
				userDS = (UserDS) snapshot_object;
				deleted = userDS.deleted;
				locked_files = userDS.locked_files;
				decisions = userDS.decisions;
				cleanUp();
			}
		} catch (FileNotFoundException e){
			// no snapshot
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// send remaining ACKs to Server 
	public static void cleanUp(){
		for(String str:decisions){
			int id=0;
			if(str.indexOf("commit")!=-1){
				id = Integer.parseInt(str.substring(6));
				if(locked_files.containsKey(id)){
					ArrayList<String> list = locked_files.get(id);
					for(int i=0; i<list.size(); i++){
						// delete those files
						File f = new File(list.get(i));
						try{
							f.delete();
						}catch(Exception e){
						}
					}
					locked_files.remove(id);
				}
			}else if(str.indexOf("abort")!=-1){
				id = Integer.parseInt(str.substring(5));
				if(locked_files.containsKey(id)){
					ArrayList<String> list = locked_files.get(id);
					for(int i=0; i<list.size(); i++){
						deleted.remove(list.get(i)); // restore those files
					}
					locked_files.remove(id);
				}
			}
			decisions.remove(str);
			saveSnapshot();
			String s = id+"#"+myId+"#A";
			ProjectLib.Message ackMsg = new ProjectLib.Message("Server", s.getBytes());
			PL.sendMessage(ackMsg);
		}
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		serverMsg = new HashSet<String>();
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		readSnapshot();
		if (deleted == null) deleted = new HashSet<String>();
		if (locked_files == null) locked_files = new HashMap<Integer, ArrayList<String>>();
		if (decisions == null) decisions = new HashSet<String>();
	}
}


