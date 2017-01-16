import java.lang.*;
import java.util.*;
import java.io.*;



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

public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
	private static ProjectLib PL;
	private static HashSet<String> deleted;
	private static HashMap<Integer, ArrayList<String>> files;
	private static HashSet<String> serverMsg;
	public UserNode( String id ) {
		myId = id;
	}

	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
            try(ObjectInputStream o = new ObjectInputStream(b)){
                return o.readObject();
            }
        }
    }

    public static boolean checkFiles(String[] files){
    	for(int i=0; i<files.length; i++){
    		if(deleted.contains(files[i])) return false;
	    	File f = new File(files[i]);
	    	if(!f.exists()) {
	    		System.out.println(files[i]+" doesn't exist!");
	    		return false;
	    	}
    	}
    	return true;
    }

	public boolean deliverMessage( ProjectLib.Message msg ) {
		// get current folder name
		String currentPath = System.getProperty("user.dir");
		String[] splitPath = currentPath.split("/");
		// String folder = splitPath[splitPath.length-1];
		String str = null;

		System.out.println( myId + ": Got message from " + msg.addr );

		// if it's a confirmation msg sent from server
		try{
			str = new String(msg.body, "UTF-8");
			if(str.indexOf("commit")!=-1){
				if(!serverMsg.add(str)) return true; // if user has received this msg before
				System.out.println(myId+" receive commit "+str.substring(6)+" confirmation from Server!");
				ArrayList<String> list = files.get(Integer.parseInt(str.substring(6)));
				for(int i=0; i<list.size(); i++){
					// delete those files
					File f = new File(list.get(i));
					try{
						f.delete();
					}catch(Exception e){
						e.printStackTrace();
					}
				}
				files.remove(Integer.parseInt(str.substring(6)));
				return true;
			}else if(str.indexOf("abort")!=-1){
				if(!serverMsg.add(str)) return true; // if user has received this msg before
				System.out.println(myId+" receive abort "+str.substring(5)+" confirmation from Server!");
				ArrayList<String> list = files.get(Integer.parseInt(str.substring(5)));
				for(int i=0; i<list.size(); i++){
					deleted.remove(list.get(i)); // restore those files
				}
				files.remove(Integer.parseInt(str.substring(5)));
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
		if(files.containsKey(userMsg.num)){ // if the commit has been processed but 
			// the server didn't receive the msg
			str = Integer.toString(userMsg.num)+"#"+myId+"#S";
			System.out.println("1!!!!!");
			ProjectLib.Message again = new ProjectLib.Message( "Server", "Again!!!".getBytes() );
			PL.sendMessage(again);
		}else if(!checkFiles(userMsg.sources) || PL.askUser(userMsg.img, userMsg.sources)==false){
			str = Integer.toString(userMsg.num)+"#"+myId+"#F";
			System.out.println("2!!!!!");
		}else{
			str = Integer.toString(userMsg.num)+"#"+myId+"#S";
			ArrayList<String> list = new ArrayList<String>();
			for(int i=0; i<userMsg.sources.length; i++){
				deleted.add(userMsg.sources[i]);
				list.add(userMsg.sources[i]);
			}
			files.put(userMsg.num, list);
			System.out.println("3!!!!!");
		}
		newmsg = new ProjectLib.Message("Server", str.getBytes());
		// PL = new ProjectLib( 15440, myId, new UserNode(myId));
		PL.sendMessage(newmsg);
		return true;
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		deleted = new HashSet<String>();
		files = new HashMap<Integer, ArrayList<String>>();
		UserNode UN = new UserNode(args[1]);
		serverMsg = new HashSet<String>();
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		
		// ProjectLib.Message msg = new ProjectLib.Message( "Server", "hello".getBytes() );
		// System.out.println( args[1] + ": Sending message to " + msg.addr );
		// PL.sendMessage( msg );
	}
}

