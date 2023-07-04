package logging.service.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    String serverHost;
    int serverPort;
    int maxBytes;

    int workerThreadSize;

    ExecutorService workerThreadPool;
    ServerSocketChannel serverSocketChannel;
    Utility utility = new Utility();
    Map<SocketChannel, Queue<ByteBuffer>> writeQueue = new ConcurrentHashMap<>();
    Map<SocketChannel, String> connections = new ConcurrentHashMap<>();

    public Server(){
        this.serverHost = "localhost";
        this.serverPort = 7777;
        this.maxBytes = 524288; /* 512KB */
        this.workerThreadSize = 1;
        init();
    }

    public Server(String serverHost, int serverPort, int maxBytes, int workerThreadSize){
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.maxBytes = maxBytes;
        this.workerThreadSize = workerThreadSize;
        init();
    }

    private void init(){
        try {
            workerThreadPool = Executors.newFixedThreadPool(workerThreadSize);
            Selector selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(serverHost, serverPort));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true){
                try {
                    selector.select();
                    Iterator<SelectionKey> selectionKeyIterator = selector.selectedKeys().iterator();
                    while(selectionKeyIterator.hasNext()){
                        SelectionKey key = selectionKeyIterator.next();
                        if(key.isValid() && key.isAcceptable()){
                            accept(key);
                        }
                        if(key.isValid() && key.isReadable()){
                            read(key);
                        }
                        if(key.isValid() && key.isWritable()){
                            write(key);
                        }
                    }
                }catch (IOException ex){
                    System.out.println("Exception in selector.select()");
                }
            }
        } catch (IOException ex) {
            System.out.println("IOException in init(): "+ex.getMessage());
        }
    }


    private void accept(SelectionKey selectionKey){
        try {
            SocketChannel clientSocket = serverSocketChannel.accept();
            if (clientSocket != null) {
                selectionKey.attach(null);
                clientSocket.configureBlocking(false);
                clientSocket.register(selectionKey.selector(), SelectionKey.OP_READ +SelectionKey.OP_WRITE);
                addNewClient(clientSocket);
            }
        } catch (IOException ex) {
            System.out.println("Exception in Accept(): "+ex.getMessage());
            removeClosedClient(selectionKey);
        }
    }

    private void read(SelectionKey selectionKey){
        try{
            SocketChannel clientSocket = (SocketChannel) selectionKey.channel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(maxBytes);
            int bufferSize = clientSocket.read(buffer);
            if(bufferSize == -1){
                removeClosedClient(selectionKey);
            }else if(bufferSize != 0){
                workerThreadPool.submit(() -> {
                    buffer.flip();
                    process(selectionKey, utility.ByteBufferToString(buffer, StandardCharsets.UTF_8));
                    buffer.clear();
                });
            }
        } catch (ClosedChannelException ex){
            System.out.println("ClosedChannelException in Read(): "+ex.getMessage());
            removeClosedClient(selectionKey);
        } catch (IOException ex){
            System.out.println("IOException in Read(): "+ex.getMessage());
            removeClosedClient(selectionKey);
        } catch (Exception ex){
            System.out.println("Exception in Read(): "+ex.getMessage());
            removeClosedClient(selectionKey);
        }
    }

    private void write(SelectionKey selectionKey){
        SocketChannel clientSocket = (SocketChannel) selectionKey.channel();
        try {
            Queue<ByteBuffer> messageQueue = writeQueue.get(clientSocket);
            ByteBuffer buffer;
            while ((buffer = messageQueue.peek()) != null){
                clientSocket.write(buffer);
                if(buffer.hasRemaining()){
                    clientSocket.configureBlocking(false);
                    clientSocket.register(selectionKey.selector(), SelectionKey.OP_READ);
                    return;
                }else{
                    messageQueue.poll();
                }
            }
        }catch (Exception ex){
            System.out.println("Exception in Write(): "+ex.getMessage());
        }
    }

    private void addNewClient(SocketChannel clientSocket){
        writeQueue.put(clientSocket, new LinkedList<>());
    }

    private void removeClosedClient(SelectionKey key){
        try {
            SocketChannel clientSocket = (SocketChannel)key.channel();
            key.cancel();
            connections.remove(clientSocket);
            writeQueue.remove(clientSocket);
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void process(SelectionKey key, String message) {
        message = message.trim();
        if (message.length() > 0) {
             writeQueue.get((SocketChannel) key.channel()).add(utility.StringToByteBuffer(message, StandardCharsets.UTF_8));
        }
    }

}
