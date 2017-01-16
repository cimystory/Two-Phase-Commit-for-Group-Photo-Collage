/* Skeleton code for UserNode */

public class UserNode{
	public final String myId;
	public UserNode( String id ) {
		myId = id;
	}

	// public void handlemsg(ProjectLib.Message msg){
	// 	boolean ret = ProjectLib.askUser(msg.body, msg.)
	// }
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		// UserNode UN = new UserNode(args[1]);
		ProjectLib PL = new ProjectLib( Integer.parseInt(args[0]), args[1]);//, UN );
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			String str = new String(msg.body, "UTF-8");
			System.out.println(args[1]+": Got message from " + msg.addr);
			System.out.println("content: "+ str);
			String[] s = str.split("#");
			
			// System.out.println( "Server: Echoing message to " + msg.addr );
			// PL.sendMessage( msg );
		}
		// ProjectLib.Message msg = new ProjectLib.Message( "Server", "hello".getBytes() );
		// System.out.println( args[1] + ": Sending message to " + msg.addr );
		// PL.sendMessage( msg );
	}
}

