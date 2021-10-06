import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.IllegalAccessException;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.io.*;

public class Dstore {
    private Map<String, Method> commands = new HashMap<String, Method>();
    private final ServerSocket dstoreSocket;
    private final int cport;
    private final int timeout;
    private final String filepath;
    private Map<String, File> files = new HashMap<String, File>();
    private DstoreLogger logger;

    public static void main(String[] args) {
        //setup dstore
        try {
            int port = Integer.parseInt(args[0]);
            int cport = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            String filepath = args[3];
            Dstore store = new Dstore(port, cport, timeout, filepath);
            store.setupThreads(8);
            try {
                store.joinController(port);
            } catch(IOException e) {}
        } catch(IOException e) {
            System.err.println("DStore couldn't construct the object");
        }
        catch(NoSuchMethodException e) {
            System.err.println("DStore can't find method with the name when generating request map.");
        }
    }

    public Dstore(int port, int cport, int timeout, String filepath) throws IOException, NoSuchMethodException {
        this.filepath = filepath;
        for(File  f: (new File(filepath)).listFiles()) {
            this.files.put(f.getName(), f);
        }
        System.out.println("DSTORE CONSTRUCTOR: " + this.files.keySet().toString());
        this.timeout = timeout;
        this.dstoreSocket= new ServerSocket(port);
        this.cport = cport;
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
        this.logger = DstoreLogger.getInstance();
        initCommandsMap();
    }

    private void initCommandsMap() throws NoSuchMethodException {
        this.commands.put(Protocol.STORE_TOKEN, Dstore.class.getMethod("store", Socket.class, List.class));
        this.commands.put(Protocol.REBALANCE_STORE_TOKEN, Dstore.class.getMethod("read_file_from_other_dstore", Socket.class, List.class));
        this.commands.put(Protocol.LOAD_DATA_TOKEN, Dstore.class.getMethod("load", Socket.class, List.class));
        this.commands.put(Protocol.REMOVE_TOKEN, Dstore.class.getMethod("remove", Socket.class, List.class));
        this.commands.put(Protocol.LIST_TOKEN, Dstore.class.getMethod("list", Socket.class, List.class));
        this.commands.put(Protocol.REBALANCE_TOKEN, Dstore.class.getMethod("rebalance", Socket.class, List.class));
    }

    public void joinController(int dport) throws IOException {
        Socket controllerSocket = new Socket("localhost", this.cport);
        PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
        out.println("JOIN " + String.valueOf(dport));
        controllerSocket.close();
        out.close();
    }

    public void store(Socket clientSocket, List<String> args) {
        //check args length is 2
        if(args.size() != 2) {
            logger.log("STORE ARGS COUNT ISN'T 2");
            return;
        }

        try {
            clientSocket.setSoTimeout(this.timeout);
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            //get filename and filesize
            String filename = args.get(0);
            int filesize = 0;
            try {
                filesize = Integer.parseInt(args.get(1));
            } catch(NumberFormatException e) { logger.log("Issue when converting the filesize to an int in store."); return; }

            //send ack
            out.println(Protocol.ACK_TOKEN);
            logger.messageSent(clientSocket, Protocol.ACK_TOKEN);

            //read filecontent
            byte[] filecontent = clientSocket.getInputStream().readNBytes(filesize);
            logger.messageReceived(clientSocket, new String(filecontent, StandardCharsets.UTF_8));

            //add file to map
            //build filecontent into file
            File file = new File(this.filepath + "/" + filename);
            Path path = Paths.get(file.getPath());
            if(Files.isWritable(path)) {
                Files.write(path, filecontent);
            }
            synchronized(this.files) {
                this.files.put(filename, file);
            }


            //send store_ack with filename
            out.println(Protocol.STORE_ACK_TOKEN + " " + filename);
            logger.messageSent(clientSocket, Protocol.STORE_ACK_TOKEN + " " +filename);

            out.close();
            clientSocket.close();
        } catch(SocketTimeoutException e) {
            // Couldn't read the filecontents in time -> log and forget
            logger.log("Couldn't read filecontents sent by " + clientSocket.getPort() + " to DStore " + this.dstoreSocket.getLocalPort() + " in time.");
        } catch(IOException e) {
            logger.log("Exception caught when recieving a store message from client port " + clientSocket.getPort() + ": " + e.getMessage());
        }
    }

    public void read_file_from_other_dstore(Socket storeSocket, List<String> args) {
        //check args length is 2
        if(args.size() != 2) {
            logger.log("STORE ARGS COUNT ISN'T 2");
            return;
        }

        try {
            //get filename and filesize
            String filename = args.get(0);
            int filesize = 0;
            try {
                filesize = Integer.parseInt(args.get(1));
            } catch(NumberFormatException e) { logger.log("Issue when converting the filesize to an int in store."); return; }

            PrintWriter out = new PrintWriter(storeSocket.getOutputStream(), true);
            //storeSocket.setSoTimeout(this.timeout);

            //if dstore already has filename -> send file already exists error
            synchronized(this.files) {
                if(this.files.get(filename) != null) {
                    String alreadyExistsMsg = "DSTORE ALREADY HAS THE FILE " + filename;
                    out.println(alreadyExistsMsg);
                    logger.messageSent(storeSocket, alreadyExistsMsg);
                    out.close();
                    storeSocket.close();
                    return;
                }
            }

            //send ack
            out.println(Protocol.ACK_TOKEN);
            logger.messageSent(storeSocket, Protocol.ACK_TOKEN);

            //read filecontent
            byte[] filecontent = storeSocket.getInputStream().readNBytes(filesize);
            logger.messageReceived(storeSocket, "RECEIVED FILECONTENTS OF " + filename);


            //add file to map
            //build filecontent into file
            File file = new File(this.filepath + "/" + filename);
            Path path = Paths.get(file.getPath());
            if(Files.isWritable(path)) {
                Files.write(path, filecontent);
            }

            synchronized(this.files) {
                this.files.put(filename, file);
            }

            out.close();
            storeSocket.close();
        } catch(SocketTimeoutException e) {
            // Couldn't read the filecontents in time -> log and forget
            logger.log("Couldn't read filecontents sent by " + storeSocket.getPort() + " to DStore " + this.dstoreSocket.getLocalPort() + " in time.");
        } catch(IOException e) {
            logger.log("Exception caught when recieving a store message from client port " + storeSocket.getPort() + ": " + e.getMessage());
        }
    }

    private boolean storeSend(Socket storeSocket, File file) {
        boolean success = false;
        try {
            // Don't send if the file length is  0 or less
            if(file.length() <= 0) {
                logger.log("Can't send a file that has no length");
                return false;
            }

            //Send store to storeSocket
            PrintWriter pw = new PrintWriter(storeSocket.getOutputStream(), true);
            BufferedReader br = new BufferedReader(new InputStreamReader(storeSocket.getInputStream()));
            //storeSocket.setSoTimeout(this.timeout);

            // Send rebalance store
            pw.println(Protocol.REBALANCE_STORE_TOKEN + " " + file.getName() + " " + file.length());
            logger.messageSent(storeSocket, Protocol.REBALANCE_STORE_TOKEN + " " + file.getName() + " " + file.length());

            // Read ack message
            String ackMsg = br.readLine();
            if(ackMsg == null) {
                logger.log("Message sent to DStore during store-send was null");
            } else {
                logger.messageReceived(storeSocket, ackMsg);
                if(ackMsg.equals(Protocol.ACK_TOKEN)) {
                    // Send file_contents
                    byte[] filecontents = new byte[(int) file.length()];
                    synchronized(this.files) {
                        if(Files.isReadable(file.toPath())) {
                            filecontents = Files.readAllBytes(file.toPath());
                        }
                    }
                    storeSocket.getOutputStream().write(filecontents);
                    logger.messageSent(storeSocket, "SENT FILECONTENTS OF " + file.getName());
                    success = true;
                }
                else if(ackMsg.equals("DSTORE ALREADY HAS THE FILE " + file.getName())) {
                    logger.messageReceived(storeSocket, "DSTORE ALREADY HAS THE FILE " + file.getName());
                }
                else {
                    //Malformed message
                    logger.log("Malformed message is " + ackMsg + ". It should be " + Protocol.ACK_TOKEN + ".");
                }
            }
            br.close();
            pw.close();
            storeSocket.close();
        } catch(SocketTimeoutException e) {
            // Couldn't read ack in time -> Log and forget
            logger.log("Couldn't read " + Protocol.ACK_TOKEN + " from DStore " + storeSocket.getPort() + " to DStore " + this.dstoreSocket.getLocalPort() + " in time.");
        } catch(IOException e) {
            logger.log("Exception found when sending a store message to dstore of port " + storeSocket.getPort() + ": " + e.getMessage());
            e.printStackTrace();
        }
        return success;
    }

    public void load(Socket clientSocket, List<String> args) {
        if(args.size() != 1) {
            logger.log("LOAD ARGS COUNT ISN'T 1");
            return;
        }

        //filename
        String filename = args.get(0);

        try {
            clientSocket.setSoTimeout(this.timeout);

            //send file contents to clientSocket
            synchronized(this.files) {
                if(this.files.containsKey(filename)) {
                    byte[] filecontents = Files.readAllBytes(this.files.get(filename).toPath());
                    clientSocket.getOutputStream().write(filecontents);
                    logger.messageSent(clientSocket, new String(filecontents));
                }
            }
            clientSocket.close();
        } catch(IOException e) {
            logger.log("Exception found when reading a load message from the client of port " + String.valueOf(clientSocket.getPort()) + ": " + e.getMessage());
        }
    }

    public void remove(Socket controllerSocket, List<String> args) {
        if(args.size() != 1) {
            logger.log("LOAD ARGS COUNT ISN'T 1");
            return;
        }

        String filename = args.get(0);

        // filename is null or empty
        if(filename == null || filename == "") {
            logger.log("No filename given in remove");
        }

        try {
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream());
            controllerSocket.setSoTimeout(this.timeout);
            //remove file with the filename
            boolean containsFile = false;
            synchronized(this.files) {
                containsFile = this.files.containsKey(filename);

            }

            //send ack if the removal was successful
            if(containsFile) {
                synchronized(this.files) {
                    this.files.get(filename).delete();
                    this.files.remove(filename);
                }
                out.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                logger.messageSent(controllerSocket, Protocol.REMOVE_ACK_TOKEN  + " " + filename);
            } else {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                logger.messageSent(controllerSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            }
            out.close();
            controllerSocket.close();
        } catch(IOException e) {
            logger.log("Exception found when reading a remove message from the controller of port " + String.valueOf(controllerSocket.getPort()) + ": " + e.getMessage());
        }
    }

    public void list(Socket controllerSocket, List<String> args) {
        // There should be no arguments in args
        if(!args.isEmpty()) {
            logger.log("There should be no arguments when Controller asks for the files in DStore " + this.dstoreSocket.getLocalPort() + ".");
        }
        try {
            // send file list
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            controllerSocket.setSoTimeout(this.timeout);
            synchronized(this.files) {
                if(!this.files.isEmpty()) {
                    String filelist = String.join(" ", this.files.keySet());
                    out.println(Protocol.LIST_TOKEN + " " + filelist);
                    logger.messageSent(controllerSocket, Protocol.LIST_TOKEN + " " + filelist);
                } else {
                    System.out.println("DSTORE " + this.dstoreSocket.getLocalPort() +  " DIDN'T HAVE FILES TO LIST");
                    out.println(Protocol.LIST_TOKEN);
                    logger.messageSent(controllerSocket, Protocol.LIST_TOKEN);
                }
            }
            out.close();
            controllerSocket.close();
        } catch(IOException e) {
            logger.log("Exception found when reading a list message from the controller of port " + String.valueOf(controllerSocket.getPort()) + ": " + e.getMessage());
        }
    }

    public void rebalance(Socket controllerSocket, List<String> args) {
        Queue<String> argQueue = new LinkedList<String>(args);
        boolean delete = true;
        List<SuccessRun> methods = parseRunnableStatement_send(argQueue);
        if(methods != null) {
            for(SuccessRun method : methods) {
                if(!method.successRun())
                    delete = false;
            }
        }

        // wait whilst the threads haven't terminated
        if(argQueue.size() > 0) {
            int numFilesRemove = Integer.parseInt(argQueue.remove());
            for(int j = 0; j < numFilesRemove; j++) {
                String filename = argQueue.remove();
                removeFile(filename);
            }
        }

       // rebalance complete to controller
        try {
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            out.println(Protocol.REBALANCE_COMPLETE_TOKEN);
            logger.messageSent(controllerSocket, Protocol.REBALANCE_COMPLETE_TOKEN);
            out.close();
        } catch(IOException e) {
            logger.log("Couldn't send messages from DStore " + this.dstoreSocket.getLocalPort() + " to Controller during rebalancing");
        }
    }

    private List<SuccessRun> parseRunnableStatement_send(Queue<String> args) {
        List<SuccessRun> methods = new ArrayList<SuccessRun>();
        //first arg -> files that need to be sent
        // send files
        int numFilesSend = 0;
        try {
            numFilesSend = Integer.parseInt(args.remove());
        } catch(NumberFormatException e) {
            // Should be removing
            return null;
        }

        for(int j = 0; j < numFilesSend; j++) {
            //get file
            String filename = args.remove();

            //get portcount
            int portCount = Integer.parseInt(args.remove());

            // go though each port and send the designated file
            if(portCount > 0) {
                for(int pi = 0; pi < portCount; pi++) {
                    int port = Integer.parseInt(args.remove());
                    if(port == dstoreSocket.getLocalPort())
                        continue;
                    methods.add(new SuccessRun() {
                        public boolean successRun() {
                            try {
                                File file = files.get(filename);
                                if(file != null) {
                                    Socket storeSocket = new Socket("localhost", port);
                                    boolean success =  storeSend(storeSocket, file);
                                    storeSocket.close();
                                    return success;
                                }
                            } catch(IOException e) {
                                logger.log("Exception raised when sending a store request to dstore of port " + port + ": " + e.getMessage());
                            }
                            return false;
                        }
                    });
                }
            }
        }
        return methods;
    }

    private List<Runnable> parseRunnableStatement_remove(Queue<String> args) {
        List<Runnable> methods = new ArrayList<Runnable>();
        if(args.size() > 0) {
            int numFilesRemove = Integer.parseInt(args.remove());
            for(int j = 0; j < numFilesRemove; j++) {
                String filename = args.remove();
                methods.add(new Runnable() {
                    public void run() {
                        removeFile(filename);
                    }
                });
            }
        }
        return methods;
    }

    private void removeFile(String filename) {
        synchronized(this.files) {
            this.files.get(filename).delete();
            this.files.remove(filename);
        }
    }

   public void setupThreads(int threadCount) {
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        service.execute(new Runnable() {
            public void run() {
                while(true) {
                    runDStoreThread();
                }
            }
        });
        service.shutdown();
    }

    public void runDStoreThread() {
        String message = "";
        try {
            //accept socket
            Socket clientSocket = (Socket) this.dstoreSocket.accept();

            //read socket message and check what method to invoke
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            message = in.readLine();

            if(message == null) {
                this.logger.log("Message sent to the DStore is null");
                return;
            }

            //read protocol token and args
            List<String> serverRequest = new LinkedList<String>();
            Collections.addAll(serverRequest, message.split(" "));

            //get method
            Method serverResponse = this.commands.get(serverRequest.get(0));
            if(serverResponse == null) {
                logger.log("message sent to the DStore was null");
            }
            else {
                serverRequest.remove(0);
                serverResponse.invoke(this, clientSocket, serverRequest);
            }
        } catch(IOException e) {
            logger.log("Can't read the accepting socket's input stream: " + e.getMessage());
        } catch(IllegalAccessException | InvocationTargetException e) {
            logger.log("Can't return the socket that sent a message to dstore " + dstoreSocket.getLocalPort() + ".");
        }
    }
    interface SuccessRun {
        public boolean successRun();
    }
}
