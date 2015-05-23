package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import static edu.buffalo.cse.cse486586.simpledht.Ring.handleNetworkRequests;

/**
 * Created by anand on 3/30/15.
 */
public class NetworkTasks {

    final static String join_req="joinreq ",changepredsuc="predsuc ",changepred="pred ",changesuc="suc ", queryresponse="queryresponse ",
            forwardreq="forward ",get_all_keys="getallkeys ",allkeys="allkeys ", highlow="highlow ",forwardquery="forwardq ",forwarddelete="forwarddelete ";
    static ServerSocket SERVER_SOCKET=null;
    static String REMOTE_PORT[] = null;

    public static void initNetworkValues(){

        REMOTE_PORT=new String[5];
        REMOTE_PORT[0]=  "11108"; //5554 - avd3
        REMOTE_PORT[1] = "11112"; //5556 - avd0
        REMOTE_PORT[2] = "11116"; //5558 - avd1
        REMOTE_PORT[3] = "11120"; //5560 - avd2
        REMOTE_PORT[4] = "11124"; //5562 - avd4

        try {
            SERVER_SOCKET = new ServerSocket(10000);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    public static class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort = msgs[1];

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = msgs[0];
                Log.d("network", msgToSend);
                /*
                 * TODO: Fill in your client code that sends out a message.
                 * Send data to client
                 *
                 */

                PrintWriter out=new PrintWriter(socket.getOutputStream(),true);
                out.print(msgToSend);

                out.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e("TAG", "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("TAG", "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }


    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    public static class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        SimpleDhtProvider provider;
        Context context;
        ServerTask(Context ctx, SimpleDhtProvider sp){
            context=ctx;
            provider=sp;
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             * Accept connection from client.
             * As long as data is received, send it to onProgressUpdate
             */
            ServerSocket serverSocket = sockets[0];
            String text;
            try {


                while(true)
                {
                    Socket clientSocket=serverSocket.accept();
                    BufferedReader br=new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    text=br.readLine();
                    if(text!=null) {
                        Log.e("TAG", "msg="+text);
                        publishProgress(text.trim());
                    }
                    br.close();
                    clientSocket.close();
                }



            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

            String strReceived = strings[0].trim();

            handleNetworkRequests(provider, context, strReceived);

            return;
        }
    }

}
