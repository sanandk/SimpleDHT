package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import static edu.buffalo.cse.cse486586.simpledht.Ring.*;
import static edu.buffalo.cse.cse486586.simpledht.NetworkTasks.*;

public class SimpleDhtProvider extends ContentProvider {

    private static String spname="DHT";
    SharedPreferences pref;

    void deleteFromStorage(String selection){
        Log.v("delete", selection);
        SharedPreferences.Editor editor = pref.edit();
        editor.remove(selection);
        editor.commit();
    }

    void insertIntoStorage(String key, String value){
        Log.v("insert", key + "," + value);
        SharedPreferences.Editor editor = pref.edit();
        editor.putString(key, value);
        editor.commit();
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        try {
            if( checkRingCondtion(getContext(), 1, selection) )
            {
                deleteFromStorage(selection);
            }
            else if(checkRingCondtion(getContext(), 2, selection))
            {
                deleteFromStorage(selection);
            }
            else if(checkRingCondtion(getContext(), 3, selection))
            {
                Log.v("q forwarded to successor", selection + "," + getmyPort(getContext())+ successor);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwarddelete+selection, successor);
            }
            else
            {
                Log.v("q forwarded to successor", selection + "," + getmyPort(getContext())+ successor);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwarddelete+selection, successor);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        validateRing(getContext());

        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");

            if(checkRingCondtion(getContext(), 1, key))
            {
                insertIntoStorage(key,value);
            }
            else if(checkRingCondtion(getContext(), 2, key))
            {
                insertIntoStorage(key,value);
            }
            else if (checkRingCondtion(getContext(), 3, key))
            {
                Log.v("forwarded to successor", key + "," + getmyPort(getContext())+ successor);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardreq+key+","+value, successor);
            }
            else
            {
                Log.v("forwarded to successor", key + "," + getmyPort(getContext())+ successor);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardreq+key+","+value, successor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uri;
    }


    @Override
    public boolean onCreate() {

        pref= getContext().getSharedPreferences(spname, getContext().MODE_PRIVATE);

        init(getContext());

        /*
         * Create a server socket as well as a thread (AsyncTask) that listens on the server
         * port.
         *
         * AsyncTask is a simplified thread construct that Android provides. Please make sure
         * you know how it works by reading
         * http://developer.android.com/reference/android/os/AsyncTask.html
         */


        new ServerTask(getContext(), this).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, SERVER_SOCKET);

        return false;
    }



    Object getAllKeys(boolean serialize)
    {
        StringBuffer sb=new StringBuffer();
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        Map<String,?> keys = pref.getAll();

        for(Map.Entry<String,?> entry : keys.entrySet()){
            Log.d("starquery",entry.getKey()+","+entry.getValue());
            if(serialize) {
                sb.append(entry.getKey());
                sb.append(",");
                sb.append(entry.getValue());
                sb.append("|");
            }
            else
                cursor.addRow(new String[]{entry.getKey(), entry.getValue().toString()});
        }
        if(serialize)
            return sb.toString();
        else
            return cursor;
    }

    public String process_query(String forwhom,String selection){
        String restoredText=null;
        try {
            if( checkRingCondtion(getContext(), 1, selection) )
            {
                restoredText = pref.getString(selection, "null");
                Log.v("query", selection + "," + restoredText);

                if(forwhom!=null)   // if query is not initiated by me
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, queryresponse+restoredText, forwhom);
            }
            else if(checkRingCondtion(getContext(), 2, selection))
            {
                restoredText = pref.getString(selection, "null");
                Log.v("query", selection + "," + restoredText);

                if(forwhom!=null)   // if query is not initiated by me
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, queryresponse+restoredText, forwhom);
            }
            else if(checkRingCondtion(getContext(), 3, selection))
            {
                Log.v("q forwarded to successor", selection + "," + getmyPort(getContext())+ successor);
                if(forwhom==null)
                    forwhom=getmyPort(getContext());
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery+selection+","+forwhom, successor);
            }
            else
            {
                Log.v("q forwarded to successor", selection + "," + getmyPort(getContext())+ successor);
                if(forwhom==null)
                    forwhom=getmyPort(getContext());
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery+selection+","+forwhom, successor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return restoredText;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        Log.d("qsel",selection);
        if(selection.equals("\"*\""))
        {
            DHTcursor=null;
            DHT=new Map[5];

            if(lowest.equals(getmyPort(getContext())) && highest.equals(getmyPort(getContext())))
            {
                cursor=(MatrixCursor) getAllKeys(false);
            }
            else {
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, get_all_keys + getmyPort(getContext()),successor);

                try {
                    while (DHTcursor == null)
                        Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                cursor = DHTcursor;
            }
        }
        else if(selection.equals("\"@\""))
        {
            cursor=(MatrixCursor) getAllKeys(false);
        }
        else {
            String restoredText = process_query(null,selection);
            if(restoredText==null) {    // forwarded query
                try {
                    while (answer == null)  // wait for reply
                        Thread.sleep(500);
                    Log.d("queryans","Thread woke up!"+answer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                restoredText = answer;
                answer=null;
            }
            Log.d("queryans",restoredText);
            cursor.addRow(new String[]{selection, restoredText});
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


}
