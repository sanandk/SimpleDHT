package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static edu.buffalo.cse.cse486586.simpledht.NetworkTasks.*;

/**
 * Created by anand on 3/30/15.
 */
public class Ring {

    public static String lowest=null,highest=null;
    static public String MYPORT=null;
    static String predecessor =null, successor =null;
    static int nodecnt=1;
    static MatrixCursor DHTcursor=null;
    static Map<String, String>[] DHT;
    static String answer=null;

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    static int getIndex(String port)
    {
        int index=Integer.parseInt(port)-11108;
        index/=4;
        return index;
    }

    static String getmyPort(Context context){
        if(MYPORT==null) {
            TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
            MYPORT = Integer.parseInt(tel.getLine1Number().substring((tel.getLine1Number().length() - 4)))* 2+"";
        }
        return MYPORT;
    }

    static void init(Context context)
    {
        DHT=new Map[5];

        initNetworkValues();

        lowest=getmyPort(context);
        highest=getmyPort(context);

        if(!getmyPort(context).equals(REMOTE_PORT[0]))
        {
            Log.d("packet","initiate request");
            new NetworkTasks.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, join_req+getmyPort(context), REMOTE_PORT[0]);
        }
    }

    static boolean strContains(String source, String subItem){
        String pattern = "\\b"+subItem+"\\b";
        Pattern p=Pattern.compile(pattern);
        Matcher m=p.matcher(source);
        return m.find();
    }

    static void validateRing(Context context){
        /* Complete the ring */
        if(successor ==null) {
            Log.d("NODEJOIN", "set successor of " + getmyPort(context) + " as low " + highest);
            successor = highest;
        }
        if(predecessor ==null) {
            Log.d("NODEJOIN","set predecessor of "+getmyPort(context)+" as high "+lowest);
            predecessor = lowest;
        }

        Log.d("highlow",highest+","+lowest+","+nodecnt);
        Log.d("MYSPREDSUC", predecessor +","+ successor);
    }

    static boolean checkRingCondtion(Context context, int i, String key) throws NoSuchAlgorithmException
    {
        switch(i)
        {
            case 1:
                return ((highest.equals(getmyPort(context)) && lowest.equals(getmyPort(context))) ||
                        ((genHash(key).compareTo(genHash(Integer.parseInt(predecessor)/2+""))>0) && genHash(key).compareTo(genHash(Integer.parseInt(getmyPort(context))/2+""))<=0) || // && genHash(Integer.parseInt(getmyPort(context))/2+"").compareTo((Integer.parseInt(predecessor)/2+"")) >0
                        (highest.equals(getmyPort(context)) && (genHash(key).compareTo(genHash(Integer.parseInt(predecessor)/2+""))>=0)) );
            case 2:
                return (highest.equals(getmyPort(context)) && (genHash(key).compareTo(genHash(Integer.parseInt(getmyPort(context))/2+""))<=0));
            case 3:
                return (genHash(key).compareTo(genHash(Integer.parseInt(predecessor)/2+""))<=0) && (genHash(key).compareTo(genHash(Integer.parseInt(getmyPort(context))/2+""))<0);
            default:
                return false;
        }
    }

    static void handleNetworkRequests(SimpleDhtProvider provider, Context context, String strReceived){

        if(strContains(strReceived,join_req))
        {
            String port=strReceived.split(join_req)[1];
            if(getmyPort(context).equals("11108"))
            {
                ++nodecnt;
            }
            Log.d("str,port",strReceived+","+port);
            try {

                if (genHash(Integer.parseInt(highest) / 2 + "").compareTo(genHash(Integer.parseInt(port) / 2 + "")) > 0)
                    highest=port;
                if (genHash(Integer.parseInt(lowest) / 2 + "").compareTo(genHash(Integer.parseInt(port) / 2 + "")) < 0)
                    lowest=port;
                for(int i=0;i<5;i++)
                    if(!REMOTE_PORT[i].equals(getmyPort(context)))
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, highlow + highest + "," + lowest+","+nodecnt, REMOTE_PORT[i]);  // tell highlow to all
                if(predecessor ==null) {
                    if (genHash(Integer.parseInt(getmyPort(context)) / 2 + "").compareTo(genHash(Integer.parseInt(port) / 2 + "")) > 0) {
                        Log.d("NODEJOIN", "set successor of " + getmyPort(context) + " as " + port);
                        predecessor = port;
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, changesuc + getmyPort(context), port);  // tell predecessor to self
                        return;
                    }
                }
                if(successor ==null){
                    if (genHash(Integer.parseInt(getmyPort(context)) / 2 + "").compareTo(genHash(Integer.parseInt(port) / 2 + "")) < 0) {
                        successor = port;
                        Log.d("NODEJOIN","set predecessor of "+getmyPort(context)+" as "+port);
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, changepred  + getmyPort(context), port);  // tell predecessor sec to self
                        return;
                    }
                }
                if(predecessor !=null) {
                    if (genHash(Integer.parseInt(getmyPort(context)) / 2 + "").compareTo(genHash(Integer.parseInt(port) / 2 + "")) > 0) {
                        if (genHash(Integer.parseInt(predecessor) / 2 + "").compareTo(genHash(Integer.parseInt(port) / 2 + "")) > 0) {
                            Log.d("NODEJOIN", "forward join for " + port + " to " + successor);
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, join_req + port, predecessor);    // forward req to successor
                        } else {
                            Log.d("NODEJOIN", "set successor of " + getmyPort(context) + " as " + port);
                            Log.d("NODEJOIN", "set predsuc of " + port + " as " + getmyPort(context) + "," + successor);
                            Log.d("NODEJOIN", "set predecessor of " + successor + " as " + port);
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, changepredsuc + predecessor + "," + getmyPort(context), port);  // tell predecessor sec to new
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, changesuc + port, predecessor); // tell change in predecessor to ex successor
                            predecessor = port;
                        }
                        return;
                    }
                }
                // else case
                Log.d("NODEJOIN", "forward join for " + port + " to " + successor);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, join_req + port, successor);    // forward req to successor

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else if(strContains(strReceived,changepred))
        {
            predecessor =strReceived.split(changepred)[1];
        }
        else if(strContains(strReceived,changesuc))
        {
            successor =strReceived.split(changesuc)[1];
        }
        else if(strContains(strReceived,changepredsuc))
        {
            String [] predsuc=strReceived.split(changepredsuc)[1].split(",");
            predecessor =predsuc[0];
            successor =predsuc[1];
        }
        else if(strContains(strReceived,forwardreq))
        {
            String [] msgs=strReceived.split(forwardreq)[1].split(",");
            ContentValues cv= new ContentValues();
            cv.put("key", msgs[0]);
            cv.put("value", msgs[1]);
            provider.insert(null, cv);
        }
        else if(strContains(strReceived,forwardquery))
        {
            String [] data=strReceived.split(forwardquery)[1].split(",");
            String restoredText = provider.process_query(data[1],data[0]);
        }
        else if(strContains(strReceived,forwarddelete))
        {
            String key=strReceived.split(forwarddelete)[1];
            provider.delete(null, key, null);
        }
        else if(strContains(strReceived,queryresponse))
        {
            answer=strReceived.split(queryresponse)[1];
            Log.d("qr",answer);
        }
        else if(strContains(strReceived,highlow))
        {
            String [] data=strReceived.split(highlow)[1].split(",");
            try {
                if (genHash(Integer.parseInt(highest) / 2 + "").compareTo(genHash(Integer.parseInt(data[0]) / 2 + "")) > 0)
                    highest=data[0];
                if (genHash(Integer.parseInt(lowest) / 2 + "").compareTo(genHash(Integer.parseInt(data[1]) / 2 + "")) < 0)
                    lowest=data[1];
                nodecnt=Integer.parseInt(data[2]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else if(strContains(strReceived,get_all_keys))
        {
            String sendto=strReceived.split(get_all_keys)[1];
            Log.d("sendsto",sendto+"."+successor);
            int forsuc=0;
            if(!sendto.equals(successor)) {
                forsuc=1;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, get_all_keys + sendto, successor);
            }

            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, allkeys + getmyPort(context) + "," + forsuc + "|" + provider.getAllKeys(true), sendto);

        }
        else if(strContains(strReceived,allkeys))
        {
            String[] keyArr=strReceived.split(allkeys)[1].split("\\|");
            String[] spl=keyArr[0].split(",");
            String recvfrom=spl[0];
            int wait=0;


            int ind=getIndex(recvfrom);
            DHT[ind]=new HashMap<String,String>();
            for(int i=1;i<keyArr.length;i++)
            {
                String[] data=keyArr[i].split(",");
                DHT[ind].put(data[0],data[1]);
            }

            for(int i=0;i<nodecnt;i++)
                if(i!=getIndex(getmyPort(context)) && DHT[i]==null) {
                    wait = 1;
                    break;
                }

            Log.d("wr",wait+","+recvfrom);
            if(wait==0)
            {
                MatrixCursor cursor = (MatrixCursor) provider.getAllKeys(false);
                for(int i=0;i<5;i++)
                    if(i!=getIndex(getmyPort(context)) && DHT[i]!=null)
                        for(Map.Entry<String,?> entry : DHT[i].entrySet())
                            cursor.addRow(new String[]{entry.getKey(), entry.getValue().toString()});
                DHTcursor=cursor;
            }
        }

    }

}
