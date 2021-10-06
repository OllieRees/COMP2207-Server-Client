import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.IllegalAccessException;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class Controller {
    private Map<String, Method> commands = new HashMap<String, Method>();
    private final ServerSocket controllerSocket;
    private final int replicationFactor;
    private final int timeout;
    private final int rebalancePeriod;
    private List<Integer> dstorePorts = new ArrayList<>();
    private Map<String, List<Integer>> filePortMap = new HashMap<String, List<Integer>>();
    private Map<String, Integer> filesizeAssoc = new HashMap<String, Integer>();
    private Map<String, Integer> fileShareCount = new HashMap<String, Integer>();
    private Map<String, Integer> reloadAttempts = new HashMap<String, Integer>();
    private Map<String, IndexState> fileState = new HashMap<String, IndexState>();
    private Semaphore rebalancingLock = new Semaphore(1, true);
    private boolean rebalancing = false;
    private ControllerLogger controllerLog;
    private ExecutorService mainThreads;
    private Thread rebalanceThread;
    private Queue<Runnable> methodQueue = new LinkedList<Runnable>();
    public enum IndexState {
        READY,
        STORE_IN_PROGRESS,
        REMOVE_IN_PROGRESS,
        LOAD_IN_PROGRESS
    }

    public static void main(String[] args) {
        //Create Controller
        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        try {
            Controller controller = new Controller(cport, replicationFactor, timeout, rebalancePeriod);
            controller.setupThreads(8);
            controller.setupRunMethod(1);
            controller.rebalanceThread = new Thread() {
                public void run() {
                    controller.runRebalanceThread();
                }
            };
            controller.rebalanceThread.start();
        } catch(IOException e) {
            System.err.println("Exception when constructing the Controller: " + e.getMessage());
        }
        catch(NoSuchMethodException e) {
            System.err.println("Exception raised when constructing the request tokens: " + e.getMessage());
        }
    }

    public Controller(int cport, int replicationFactor, int timeout, int rebalancePeriod) throws IOException, NoSuchMethodException {
        controllerSocket = new ServerSocket(cport);
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        this.controllerLog = ControllerLogger.getInstance();
        initCommandsMap();
    }

    private void initCommandsMap() throws NoSuchMethodException {
        this.commands.put(Protocol.STORE_TOKEN, Controller.class.getMethod("store", Socket.class, List.class));
        this.commands.put(Protocol.STORE_ACK_TOKEN, Controller.class.getMethod("storeAck", Socket.class, List.class));
        this.commands.put(Protocol.LOAD_TOKEN, Controller.class.getMethod("load", Socket.class, List.class));
        this.commands.put(Protocol.RELOAD_TOKEN, Controller.class.getMethod("reload", Socket.class, List.class));
        this.commands.put(Protocol.REMOVE_TOKEN, Controller.class.getMethod("remove", Socket.class, List.class));
        this.commands.put(Protocol.LIST_TOKEN, Controller.class.getMethod("list", Socket.class, List.class));
        this.commands.put(Protocol.JOIN_TOKEN, Controller.class.getMethod("startRebalancing", Socket.class, List.class));
    }

    public void store(Socket clientSocket, List<String> args) {
        if(args.size() != 2) {
            this.controllerLog.log("Need two arguments for storing file: filename and filesize");
            return;
        }

        String filename = args.get(0);

        try {
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            // filename is null
            if(filename == null || filename == "" || filename.trim() == " ")
                return;

            // Check if filename already exists in the stores
            if(this.filePortMap.containsKey(filename)) {
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                controllerLog.messageSent(clientSocket, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                return;
            }

            // update filesize assoc
            int filesize =  Integer.parseInt(args.get(1));

            if(this.fileState.get(filename) == null) {
                this.fileState.put(filename, IndexState.READY);
            }

            // If store exists in the index, and isn't programmed as removed.
            else if(this.fileState.containsKey(filename)) {
                controllerLog.log(filename + " has already been stored by the controller");
                return;
            }

            // file is now being stored
            this.fileState.put(filename, IndexState.STORE_IN_PROGRESS);

            // Get R random ports
            String storeToMsg = Protocol.STORE_TO_TOKEN + " ";
            List<Integer> ports = new ArrayList<Integer>();
            // Create message of dstore ports to send to client
            ports = this.dstorePorts.subList(0, this.replicationFactor);
            storeToMsg += String.join(" ", ports.stream().map(p -> String.valueOf(p)).collect(Collectors.toList()));

            synchronized(this.fileShareCount) {
                // Add port to list associated with filename
                if(this.fileShareCount.get(filename) == null)
                    this.fileShareCount.put(filename, 0);
            }

            // send ports to client
            storeToMsg.trim();
            out.println(storeToMsg);
            controllerLog.messageSent(clientSocket, storeToMsg);

            //wait <timeout> ms until filePortMap for filename has r unique ports
            try {
                Thread.sleep(this.timeout);
            } catch(InterruptedException e) {
                controllerLog.log("Controller couldn't recieve " + this.replicationFactor + " Store ACKs from the Dstores within " + this.timeout + "ms.");
            }

            // Check if the file has been replicated over R dstores
            synchronized(this.fileShareCount) {
                if(this.fileShareCount.get(filename) >= this.replicationFactor) {
                    //update file assoc
                    this.filesizeAssoc.put(filename, filesize);
                    this.filePortMap.put(filename, ports);
                    out.println(Protocol.STORE_COMPLETE_TOKEN);
                    controllerLog.messageSent(clientSocket, Protocol.STORE_COMPLETE_TOKEN);
                    fileState.put(filename, IndexState.READY);
                }
                else {
                    fileState.remove(filename);
                    controllerLog.log("TIMEOUT ERROR: Completing Storing");
                }
                // No longer needed
                this.fileShareCount.remove(filename);
            }
            out.close();
            clientSocket.close();
        } catch(IOException e) {
            controllerLog.log("Exception when storing " + filename + ": " + e.getMessage());
        }
    }

    public void storeAck(Socket clientSocket, List<String> args) {
        String filename = args.get(0);
        synchronized(this.fileShareCount) {
            // Add port to list associated with filename
            if(this.fileShareCount.containsKey(filename)) {
                this.fileShareCount.put(filename, this.fileShareCount.get(filename) + 1);
            }
        }
        try {
            clientSocket.close();
        } catch(IOException e) {
            controllerLog.log("Exception when closing the client's socket in storeAck: " + e.getMessage());
        }
    }

    private void load_common(Socket clientSocket, String filename) {
        try {
            clientSocket.setSoTimeout(this.timeout);
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            // filename is null
            if(filename == null)
                return;

            // check if file exists
            if(!this.filePortMap.containsKey(filename)) {
                out.write(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                controllerLog.messageSent(clientSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            // Check if the file has been removed or has never existed
            if(this.fileState.get(filename) == null) {
                out.write(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                controllerLog.messageSent(clientSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            fileState.put(filename, IndexState.LOAD_IN_PROGRESS);

            // gone through all the ports
            if(this.reloadAttempts.containsKey(filename) && this.reloadAttempts.get(filename) >= this.filePortMap.get(filename).size() - 1) {
                try {
                    out.println(Protocol.ERROR_LOAD_TOKEN);
                    controllerLog.messageSent(clientSocket, Protocol.ERROR_LOAD_TOKEN);
                    this.reloadAttempts.remove(filename);
                    out.close();
                    clientSocket.close();
                    return;
                } catch(IOException e) {
                    controllerLog.log("Exception raised when closing the Client's Socket in remove: " + e.getMessage());
                }
            }

            // get port from head and put it to the back
            int port = 0;
            synchronized(this.filePortMap) {
                if(this.filePortMap.get(filename).size() > 0) {
                    port = this.filePortMap.get(filename).remove(0);
                    this.filePortMap.get(filename).add(port);
                }
            }

            out.print(Protocol.LOAD_FROM_TOKEN + " " + String.valueOf(port) + " " + String.valueOf(filesizeAssoc.get(filename)));
            controllerLog.messageSent(clientSocket, Protocol.LOAD_FROM_TOKEN + " " + String.valueOf(port) + " " + String.valueOf(filesizeAssoc.get(filename)));
            fileState.put(filename, IndexState.READY);
        } catch(IOException e) {
            controllerLog.log("Can't connect to socket streams to load a file");
        }
    }

    public void load(Socket clientSocket, List<String> args){
        String filename = args.get(0);
        synchronized(this.reloadAttempts) {
            if(this.reloadAttempts.get(filename) != null) {
                this.reloadAttempts.remove(filename);
            }
        }
        load_common(clientSocket, filename);
    }

    public void reload(Socket clientSocket, List<String> args) {
        String filename = args.get(0);
        synchronized(this.reloadAttempts) {
            if(this.reloadAttempts.get(filename) == null) {
                this.reloadAttempts.put(filename, 0);
            }
            this.reloadAttempts.put(filename, this.fileShareCount.get(filename) + 1);
        }
        load_common(clientSocket, filename);
    }

    public void remove(Socket clientSocket, List<String> args) {
        String filename = args.get(0);
        try {
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            // filename is null - can't do do much if that's the case
            if(filename == null) {
                return;
            }

            // We don't have the file!!!
            if(!this.filePortMap.keySet().contains(filename)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                controllerLog.messageSent(clientSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            // File is already removed -> no need to remove it
            if(fileState.get(filename) == null) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                controllerLog.messageSent(clientSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            //Update index
            fileState.put(filename, IndexState.REMOVE_IN_PROGRESS);

            // Get ports associated with file
            List<Integer> ports = this.filePortMap.get(filename);

            // make sure it ends up being false
            boolean isFailure = false;

            // Write to each port
            for (Integer dstorePort : ports) {
                try {
                    Socket dstore = new Socket("localhost", dstorePort);
                    dstore.setSoTimeout(timeout);
                    PrintWriter dstoreOut = new PrintWriter(dstore.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(dstore.getInputStream()));

                    // Send remove token to dstore
                    dstoreOut.println(Protocol.REMOVE_TOKEN + " " + filename);
                    controllerLog.messageSent(dstore, Protocol.REMOVE_TOKEN + " " + filename);

                    // Read ack
                    String response = in.readLine();

                    if(response == null) {
                        controllerLog.log("Message read when removing " + filename + " from the dstores is null.");
                        isFailure = true;
                        dstore.close();
                        dstoreOut.close();
                        in.close();
                        continue;
                    }

                    controllerLog.messageReceived(dstore, response);

                    // Make sure the token is ACK or FILE_DOES_NOT_EXIST
                    if(!response.equals(Protocol.REMOVE_ACK_TOKEN + " " + filename) || !response.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename)) {
                        controllerLog.log("Malformed message read " + response + " by DStore. It should be " + Protocol.REMOVE_ACK_TOKEN + " " + filename + ".");
                        isFailure = true;
                    }

                    //close streams
                    dstore.close();
                    dstoreOut.close();
                    in.close();
                } catch(SocketTimeoutException e) {
                    // Couldn't read dstore's remove ack in time -> issue with dstore failing, or just slow speed?
                    controllerLog.log("Couldn't read " + Protocol.REMOVE_ACK_TOKEN + " in time.");
                    isFailure = true;
                } catch(IOException e) {
                    controllerLog.log("Exception raised when removing " + filename + " from the dstores: " + e.getMessage());
                    isFailure = true;
                }
            }

            // SUCCESS (assumes isFailure == false)
            if(!isFailure) {
                // remove all cases of the file from the index
                this.fileState.remove(filename); // May remove the entry if it's called on condition of all ACKS being sent
                this.filePortMap.remove(filename);
                this.filesizeAssoc.remove(filename);

                // Send ack that all dstores removed the file
                out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                controllerLog.messageSent(clientSocket, Protocol.REMOVE_COMPLETE_TOKEN);
            }
            // close sockets regardless of failure status
            out.close();
            clientSocket.close();
        } catch(IOException e) {
            controllerLog.log("Exception raised when removing " + filename + ": " + e.getMessage());
        }
    }

    public void list(Socket clientSocket, List<String> args) {
        try {
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            // Join files by space
            String fileList = Protocol.LIST_TOKEN + " ";
            for(String file : this.filePortMap.keySet()) {
                //if(!this.fileState.get(file).equals(IndexState.STORE_IN_PROGRESS) && !this.fileState.get(file).equals(IndexState.REMOVE_IN_PROGRESS)) {
                    fileList += file + " ";
                //}
            }
            fileList.trim();

            // Send over filelist
            out.println(fileList);
            controllerLog.messageSent(clientSocket, fileList);
            clientSocket.close();
            out.close();
        } catch(IOException e) {
            controllerLog.log("Runtime exception when executing list operation: " + e.getMessage());
        }
    }

    private String listRebalance(int port) {
        try {
            Socket dstoreSocket = new Socket("localhost", port);
            dstoreSocket.setSoTimeout(this.timeout);
            PrintWriter out = new PrintWriter(dstoreSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));

            out.println(Protocol.LIST_TOKEN);
            controllerLog.messageSent(dstoreSocket, Protocol.LIST_TOKEN);

            // Wait for message
            String fileList = in.readLine();
            if(fileList == null) {
                controllerLog.log("file list returned by DStore " + String.valueOf(port) + " is null.");
                return null;
            }
            controllerLog.messageReceived(dstoreSocket, fileList);
            dstoreSocket.close();
            out.close();
            in.close();
            return fileList.replaceAll("LIST", "").trim();
        } catch(SocketTimeoutException e) {
            controllerLog.log("Controller ran out of time waiting for DStore " + port + " to send its files");
        } catch(IOException e) {
            controllerLog.log("Runtime exception when executing list operation: " + e.getMessage());
        }
        return null;
    }

    private void rebalance(Socket clientSocket, List<String> requestArgs) {
        // args is either port or nothing
        if(requestArgs.size() > 1) {
            controllerLog.log("Number of arguments for rebalance are either 0 or 1 (dstore port trying to connect)");
            return;
        }

        // add dstore to the list of ports if a dstore is requesting to join
        if(requestArgs.size() == 1) {
            try {
                int port = Integer.parseInt(requestArgs.get(0));
                synchronized(this.dstorePorts) {
                    this.dstorePorts.add(port);
                }
            } catch(NumberFormatException e) { controllerLog.log("Issue with Joining: port number can't be converted to an int."); }
        }
        //ask each dstore to list their contents
        Map<Integer, List<String>> portFileMap = new HashMap<Integer, List<String>>();
        for(Integer port : this.dstorePorts) {
            // get the list of files connect to dstore of <port>
            String list = listRebalance(port);

            // if list is null leave
            if(list != null && list != "" && list != " ") {
                // turn the string into a list
                portFileMap.put(port, Arrays.asList(list.split(" ")));
            }
        }

        // recheck dstore count in case some failed
        if(this.dstorePorts.size() <= 0) {
            controllerLog.log("Can't rebalance with 0 or less dstores");
            return;
        }

        if(portFileMap == null || portFileMap.isEmpty()) {
            controllerLog.log("Port file Map is empty. No point in rebalancing.");
            return;
        }

        // get filelist
        Set<String> fileSet = new HashSet<>(portFileMap.values().stream().flatMap(List::stream).collect(Collectors.toSet()));
        fileSet.remove("");

        // No point in rebalancing if we have 0 files
        if(fileSet.size() <= 0) {
            controllerLog.log("Can't rebalance with 0 or less files");
            return;
        }

        // create new port-file mapping
        Map<Integer, Set<String>> newPortFileMapping = modifyPortFileMapping_cycle(new ArrayList<Integer>(portFileMap.keySet()), fileSet);

        // SYNTAX: <NUM. FILES TO SEND> <SENT_FILES> <NUM. FILES TO DELETE> <DELETED_FILES>
        // <SENT_FILES> = <FILENAME> <NUM. PORTS TO SEND TO> <PORTS>
        // <PORTS> = <PORT> <PORTS> | <PORT>
        // <DELETED_FILES> = <FILENAME> <DELETED_FILES> | <FILENAME>
        // METHOD:
        // Cycle through each file f in portFileMap[port]:
        // Get all the ports that have file f in newPortFileMap[port] and add them to the message
        // For all the files that are in portFileMap[port] but NOT newPortFileMap[port], add them to the remove section.
        // send each dstore their files to remove and files to send
        //ExecutorService dstoreThreads = Executors.newFixedThreadPool(4);
        for(Integer dstorePort : portFileMap.keySet()) {
            try {
                // No point in running it if it has no files
                if(portFileMap.get(dstorePort).isEmpty() || portFileMap.get(dstorePort).contains("")) {
                    continue;
                }

                // For all the files in oldPortMap[port] send the files to all the ports that have it in their newPortMap (regardless of it gives an exception)
                int numFilesToSend = 0;
                List<String> addFilesList = new ArrayList<String>();
                int numFilesToDelete = 0;
                List<String> removeFilesList = new ArrayList<String>();
                for(String filename : portFileMap.get(dstorePort)) {
                    if(filename == null || filename == "" || filename == "\n") { continue; }
                    // Get ports with the current file
                    Set<String> ports = newPortFileMapping.entrySet().stream().filter(map -> map.getValue().contains(filename)).map(map -> map.getKey().toString()).collect(Collectors.toSet());
                    if(ports.size() > 0) {
                        numFilesToSend++;
                        // Compose message of file <ports>
                        addFilesList.add(filename);
                        addFilesList.add(String.valueOf(ports.size()));
                        addFilesList.addAll(ports);
                    }
                    if(!newPortFileMapping.get(dstorePort).contains(filename)) {
                        numFilesToDelete++;
                        removeFilesList.add(filename);
                    }
                }

                // Create send message
                String addFilesMsg = "";
                if(numFilesToSend > 0) {
                    addFilesList.add(0, String.valueOf(numFilesToSend));
                    addFilesMsg = " " + String.join(" ", addFilesList);
                }

                // Create remove message
                String removeFilesMsg = "";
                if(numFilesToDelete > 0) {
                    removeFilesList.add(0, String.valueOf(numFilesToDelete));
                    removeFilesMsg = " " + String.join(" ", removeFilesList);
                }

                // nothing to add nor remove
                if(numFilesToSend < 1 && numFilesToDelete < 1) {
                    controllerLog.log("Can't send " + Protocol.REBALANCE_TOKEN + " to Dstore " + dstorePort + " if there's no files to send or remove");
                    continue;
                }

                // Setup writing objects
                Socket dstoreSocket = new Socket("localhost", dstorePort);
                dstoreSocket.setSoTimeout(this.timeout);
                PrintWriter out = new PrintWriter(dstoreSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));

                // write rebalance message
                out.println(Protocol.REBALANCE_TOKEN + addFilesMsg + removeFilesMsg);
                controllerLog.messageSent(dstoreSocket, Protocol.REBALANCE_TOKEN + addFilesMsg + removeFilesMsg);

                // get ACK
                String completeMsg = in.readLine();
                if(completeMsg != null) {
                    controllerLog.messageReceived(dstoreSocket, completeMsg);
                    if(!completeMsg.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                        controllerLog.log("Didn't recieve the rebalance complete token from DStore " + dstorePort + ".");
                    } else {
                        // add new files from newPortFileMapping
                        for(String file : newPortFileMapping.get(dstorePort)) {
                            if(this.filePortMap.get(file) == null) {
                                this.filePortMap.put(file, new ArrayList<Integer>());
                            }
                            this.filePortMap.get(file).add(dstorePort);
                            this.fileState.put(file, IndexState.READY); // just in case it wasn't added in there already
                        }
                        // remove files from portFileMap and (I assume) filePortMap that aren't in newPortFileMapping
                        for(String file : portFileMap.get(dstorePort)) {
                            if(!newPortFileMapping.get(dstorePort).contains(file)) {
                                if(this.filePortMap.get(file) != null && this.filePortMap.get(file).contains(dstorePort)) {
                                    this.filePortMap.get(file).remove(dstorePort);
                                }
                            }
                        }
                    }
                }
                out.close();
                in.close();
                dstoreSocket.close();
            } catch(SocketTimeoutException e) {
                controllerLog.log("Ran out of time waiting for " + Protocol.REBALANCE_COMPLETE_TOKEN + ".");
            } catch(IOException e) {
                controllerLog.log("Exception raised when writing rebalancing statement to Dstore " + dstorePort + ". ERROR: " + e.getMessage());
            }
        }
    }

    // Alternative system: Represent the dstores as a circle, and for each file, put the file in the first R dstores, moving clockwise around the circle
    private Map<Integer, Set<String>> modifyPortFileMapping_cycle(List<Integer> ports, Set<String> fileReference) {
        int portCount = ports.size();

        // populate new hashmap with ports and empty lists
        Map<Integer, Set<String>> newPortFileMapping = new HashMap<Integer, Set<String>>();
        for(Integer port : ports) {newPortFileMapping.put(port, new HashSet<String>());}

        // for each file -> start at a point in the cycle, continue along until you've done R insertions
        int startingIndex = 0;
        for(String file : fileReference) {
            // insert into dstore
            for(int i = 0; i < this.replicationFactor; i++) {
                int portIndex = (startingIndex + i) % portCount;
                int port = ports.get(portIndex);

                // add file in port if port doesn't have over RF/N files
                Set<String> files = newPortFileMapping.get(port);
                //int upperbound = (int) java.lang.Math.ceil((this.replicationFactor * fileReference.size())/portCount);
                //if(files.size() < upperbound) {
                files.add(file);
                newPortFileMapping.put(port, files);
                //}
            }
            startingIndex = (startingIndex + this.replicationFactor) % portCount;
        }
        return newPortFileMapping;
    }

    public void startRebalancing(Socket clientSocket, List<String> args) {
        //synchronized(this.rebalancingLock) {
            //this.rebalancing = true;
            //this.rebalancingLock.acquireUninterruptibly(this.methodQueueThreadCount);
            rebalance(clientSocket, args);
            //this.rebalancingLock.release(this.methodQueueThreadCount);
            //this.rebalancing = false;
        //}
    }

    private void runRebalanceThread() {
        while(true) {
            try {
                Thread.sleep(this.rebalancePeriod);
            } catch(InterruptedException e) {
                controllerLog.log("Exception raised when sleeping in the runRebalanceThread method");
            }
            synchronized(this.rebalancingLock) {
                startRebalancing(null, new ArrayList<String>());
            }
        }
    }

    private void rebalanceSemTake() {
        try {
            while(this.rebalancing) {}
            this.rebalancingLock.acquire();
        } catch(InterruptedException e) {}
    }

    public void setupRunMethod(int threadcount) {
        ExecutorService service = Executors.newFixedThreadPool(threadcount);
        service.execute(new Runnable() {
            public void run() {
                runMethodQueue();
            }
        });
        service.shutdown();
    }

    private void runMethodQueue() {
        while(true) {
            Runnable method = null;
            synchronized(this.methodQueue) {
                if(!this.methodQueue.isEmpty()) {
                    method = this.methodQueue.remove();
                }
            }
            if(method != null) {
                synchronized(this.rebalancingLock) {
                    method.run();
                }
            }
        }
    }

    public void setupThreads(int threadCount) {
        this.mainThreads = Executors.newFixedThreadPool(threadCount);
        this.mainThreads.execute(new Runnable() {
            public void run() {
                while(true) {
                    runControllerThread();
                }
            }
        });
        this.mainThreads.shutdown();
    }

    private void runControllerThread() {
        String message = "";
        try {
            Socket clientSocket = (Socket) controllerSocket.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            message = in.readLine();

            if(message == null) {
                controllerLog.log("Message sent to the controller is null");
                return;
            }

            System.out.println(message);

            //read protocol token and args
            List<String> serverRequest = new ArrayList<String>(Arrays.asList(message.split(" ")));

            //get method
            Method serverResponse = this.commands.get(serverRequest.get(0));

            // if server response is null
            if(serverResponse == null) {
                controllerLog.log("Server request is null");
            }

            // if it's not join send a not enough dstores request
            if(!serverResponse.getName().equals("startRebalancing")) {
                synchronized(this.dstorePorts) {
                    if(this.dstorePorts.size() < this.replicationFactor) {
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        controllerLog.messageSent(clientSocket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                }
            }


            // service the request
            else {
                serverRequest.remove(0);
                // special!!! needs to be processed concurrently to store
                if(serverResponse.getName().equals("storeAck")) {
                    this.storeAck(clientSocket, serverRequest);
                }
                else {
                    Controller controller = this;
                    synchronized(this.methodQueue) {
                        this.methodQueue.add(new Runnable() {
                            public void run() {
                                try {
                                    if(serverResponse.getName().equals("startRebalancing")) {
                                        serverResponse.invoke(controller, clientSocket, serverRequest);
                                    } else {
                                        //rebalanceSemTake();
                                        serverResponse.invoke(controller, clientSocket, serverRequest);
                                        //rebalancingLock.release();
                                    }
                                } catch(IllegalAccessException | InvocationTargetException e) {
                                    controllerLog.log("Exception raised in matching the request to the requestMap in the controller thread loop");
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                }
            }
        } catch(IOException e) {
            controllerLog.log("Exception raised in the controller thread loop: " + e.getMessage());
        }
    }
}
