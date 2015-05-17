package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    // SQLite-DB variables
    public static MessageOpenHelper dbHelper;
    public static SQLiteDatabase db;
    public static String TABLE = "messages";
    public static final Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");

    // Network variables
    public static final int SERVER_PORT = 10000;
    public static String[] nodes = new String[]{"5554", "5556", "5558", "5560", "5562"};  // id's of all nodes in network
    public static int N = nodes.length;    // number of nodes in network
    String[] rnodes = new String[]{"5562", "5556", "5554", "5558", "5560"};
    String[] hnodes = new String[]{"177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "208f7f72b198dadd244e61801abe1ec3a4857bc9",
        "33d6357cfaaf0f72991b0ecd8c56da066613c089", "abf0fd8db03e5ecb199a9b82929e9db79b909643", "c25ddd596aa7c81fa12378fa725f706d54325d12"};
    public static HashMap<String, Boolean> nodeStatus = new HashMap<>();

    // This node variables
    public static String node = null;
    public static String hashnode = null;
    public static int nodeindex = -1;
    public static final int K = 3;

    // Neighboring node variables
    public static HashMap<String, String[]> parentsMap = new HashMap<>();
    public static HashMap<String, String[]> hashparentsMap = new HashMap<>();
    public static HashMap<String, String[]> replicasMap = new HashMap<>();
    public static HashMap<String, String[]> hashreplicasMap = new HashMap<>();

    // Query variables
    public static Cursor publicCursor;
    public static HashMap<String, Boolean> queryResponseMap = new HashMap<>();
    public static int starResponseCount = 0;
    public static ArrayList<HashMap<String, String>> starResultMap = new ArrayList<>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if (selection.equals("\"@\"")) {
            db.delete(TABLE, null, null);
            Log.v("DELETE", selection);
        } else if (selection.equals("\"*\"")) {

        } else {
            String key = selection;
            String hashkey = null;
            try {
                hashkey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Exception in hashing delete key: " + key, e);
            }

            String destNode = getDataCoordinator(hashkey);
            String replicaNode1 = replicasMap.get(destNode)[0];
            String replicaNode2 = replicasMap.get(destNode)[1];

            if (destNode.equals(node) || replicaNode1.equals(node) || replicaNode2.equals(node)) {
                db.delete(TABLE, "key=\"" + key + "\"", null);
                Log.v("DELETE", selection);
            }

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", destNode, key);
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
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashkey = null;

        if (key == null || key.length() < 1) {
            System.out.println("node-" + node + ": INVALID INSERT KEY!");
            return uri;
        }

        try {
            hashkey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException in hashing insert key: " + key, e);
        }

        String destNode = getDataCoordinator(hashkey);
        String replicaNode1 = replicasMap.get(destNode)[0];
        String replicaNode2 = replicasMap.get(destNode)[1];

        //System.out.println("node-" + node + " | insert -> destNode-" + destNode + ", hashkey: " + hashkey + ", key: " + key);

        if (destNode.equals(node) || replicaNode1.equals(node) || replicaNode2.equals(node)) {
            ContentValues cv = new ContentValues();
            cv.put("key", key);
            cv.put("value", value);
            cv.put("coordinator", destNode);
            long row = db.insertWithOnConflict(TABLE, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
            if (row == -1) {
                System.out.println("node-" + node + " | Invalid Insert key: " + key + ", coordinator: " + destNode);
            }
            Log.v("INSERT", "node-" + node + " | coordinator: " + destNode + ", key: " + key);

            //System.out.println("node-" + node + " | Inserted key: " + key + ", coordinator: " + destNode);
        }

        ClientTask ct = new ClientTask();
        ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", node, destNode, key, value);

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        dbHelper = new MessageOpenHelper(getContext());
        db = dbHelper.getWritableDatabase();
        //db.delete(TABLE, null, null);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        node = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        try {
            hashnode = genHash(node);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Exception in onCreate() hashing node: " + node, e);
        }
        computeNeighbors();
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "recovery_query");
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket", e);
            return false;
        }

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
        builder.setTables(TABLE);
        Cursor c = null;

        if (selection == null || selection.length() < 1) {
            System.out.println("node-" + node + " | Invalid Query selection: " + selection);
        }

        if (selection.equals("\"@\"")) {
            // @ query

            //System.out.println("node-" + node + " | query -> selection: " + selection);

            String[] proj = new String[]{"key", "value"};
            c = builder.query(db, proj, null, selectionArgs,  null, null, null);

        } else if (selection.equals("\"*\"")) {
            // * query

            //System.out.println("node-" + node + " | query -> selection: " + selection);

            String[] proj = new String[]{"key", "value"};
            Cursor localCursor = builder.query(db, proj, null, selectionArgs,  null, null, null);
            queryResponseMap.put("\"*\"", false);
            ClientTask ct = new ClientTask();
            ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "star", "\"*\"");

            try {
                //Integer res = ct.get(2, TimeUnit.SECONDS);
                Integer res = ct.get();
            } catch (InterruptedException e) {
                Log.e(TAG, "InterruptedException in ct.get() on node-" + node + " for query key: " + "\"*\"", e);
            } catch (ExecutionException e) {
                Log.e(TAG, "ExecutionException in ct.get() on node-" + node + " for query key: " + "\"*\"", e);
            } /*catch (TimeoutException e) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "star", "\"*\"");
            }*/

            Cursor[] cursorArray = new Cursor[]{localCursor, publicCursor};
            c = new MergeCursor(cursorArray);
            publicCursor = null;

        } else {
            // key query
            String key = selection;
            String hashkey = null;

            try {
                hashkey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Exception in hashing query key: " + key, e);
            }

            String destNode = getDataCoordinator(hashkey);
            String parentNode2 = parentsMap.get(destNode)[1];
            String replicaNode1 = replicasMap.get(destNode)[0];
            String replicaNode2 = replicasMap.get(destNode)[1];

            System.out.println("node-" + node + " | query -> hashkey: " + hashkey + ", destNode: " + destNode + ", key: " + key);

            if (destNode.equals(node) || replicaNode1.equals(node) || replicaNode2.equals(node)) {

                System.out.println("node-" + node + " | Firing Query destNode: " + destNode + ", replica1: " + replicaNode1 + ", replica2: " + replicaNode2 + ", key: " + key);

                String[] proj = new String[]{"key", "value"};
                c = builder.query(db, proj, "key=\"" + key + "\"", null, null, null, null);

                if (c != null) {
                    //System.out.println("node-" + " | Query Fired destNode: " + destNode + ", key: " + key + ", c count: " + c.getCount());
                }
                System.out.println("node-" + " | Query Fired destNode: " + destNode + ", key: " + key);

            } else {
                queryResponseMap.put(key, false);
                ClientTask ct = new ClientTask();
                ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", destNode, key);

                try {
                    Integer res = ct.get(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Log.e(TAG, "InterruptedException in ct.get() on node-" + node + " for query key: " + key, e);
                } catch (ExecutionException e) {
                    Log.e(TAG, "ExecutionException in ct.get() on node-" + node + " for query key: " + key, e);
                } catch (TimeoutException e) {

                    ClientTask ct2 = new ClientTask();
                    ct2.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", parentNode2, key);

                    try {
                        Integer res2 = ct2.get();
                    } catch (InterruptedException ee) {
                        Log.e(TAG, "InterruptedException in ct.get() on node-" + node + " for query key: " + key, ee);
                    } catch (ExecutionException ee) {
                        Log.e(TAG, "ExecutionException in ct.get() on node-" + node + " for query key: " + key, ee);
                    }

                }
                c = publicCursor;
                publicCursor = null;
            }
        }

        if (c != null) {
            c.moveToFirst();
        }
        //Log.v("query", selection);

		return c;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /* ServerTask */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... params) {
            ServerSocket serverSocket = params[0];
            Socket client = null;

            while (!serverSocket.isClosed()) {
                try {
                    client = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(client.getInputStream());
                    Message msg = (Message) in.readObject();

                    if (msg.getType().equals("insert")) {

                        //System.out.println("node-" + node + " | RECVD-Insert from node: " + msg.getNode() + ", key: " + msg.getData().get("key") + ", coordinator: " + msg.getData().get("coordinator"));

                        publishProgress(
                                msg.getType(),
                                msg.getData().get("key"),
                                msg.getData().get("value"),
                                msg.getData().get("coordinator")
                        );
                    } else if (msg.getType().equals("query")) {

                        System.out.println("node-" + node + " | RECVD-Query from node: " + msg.getNode() + ", key: " + msg.getData().get("key"));

                        publishProgress(
                                msg.getType(),
                                msg.getData().get("key"),
                                msg.getNode()
                        );
                    } else if (msg.getType().equals("query_response")) {

                        System.out.println("node-" + node + " | RECVD-QueryResponse from node: " + msg.getNode() + ", key: " + msg.getData().get("key"));

                        publishProgress(
                                msg.getType(),
                                msg.getData().get("key"),
                                msg.getData().get("value"),
                                msg.getNode()
                        );
                    } else if (msg.getType().equals("delete")) {

                        //System.out.println("node-" + node + " | RECVD-Delete from node: " + msg.getNode() + ", key: " + msg.getData().get("key"));

                        publishProgress(
                                msg.getType(),
                                msg.getData().get("key"),
                                msg.getNode()
                        );
                    } else if (msg.getType().equals("recovery_query")) {

                        System.out.println("node-" + node + " | RECVD-RecoveryQuery from node: " + msg.getNode() + ", coordinator: " + msg.getData().get("coordinator"));

                        publishProgress(
                                msg.getType(),
                                msg.getData().get("coordinator"),
                                msg.getNode()
                        );
                    } else if (msg.getType().equals("recovery_response")) {

                        System.out.println("node-" + node + " | RECVD-RecoveryResponse from node: " + msg.getNode() + ", coordinator: " + msg.getData().get("coordinator"));

                        publishProgress(
                                msg.getType(),
                                msg.getData().get("coordinator"),
                                msg.getData().get("value"),
                                msg.getNode(),
                                msg.getData().get("cursor_size")
                        );
                    }
                } catch (IOException e) {
                    Log.e(TAG, "IOException on ServerSocket node-" + node, e);
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "ClassNotFoundException on ServerSocket node-" + node, e);
                }
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            if (strings[0].equals("insert")) {
                /*
                0: msg.getType(),
                1: msg.getData().get("key"),
                2: msg.getData().get("value"),
                3: msg.getData().get("coordinator")
                 */

                ContentValues cv = new ContentValues();
                cv.put("key", strings[1]);
                cv.put("value", strings[2]);
                cv.put("coordinator", strings[3]);
                long row = db.insertWithOnConflict(TABLE, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
                if (row == -1) {
                    System.out.println("node-" + node + " | NOT Inserted key: " + strings[1] + ", coordinator: " + strings[3]);
                }
                Log.v("INSERT", "node-" + node + " | key: " + strings[1]);

                //System.out.println("node-" + node + " | Inserted key: " + strings[1] + ", coordinator: " + strings[3]);

            } else if (strings[0].equals("query")) {
                /*
                0: msg.getType(),
                1: msg.getData().get("key"),
                2: msg.getNode()
                 */

                String key = strings[1];
                String originNode = strings[2];
                SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
                builder.setTables(TABLE);
                Cursor cursor;

                System.out.println("node-" + node + " | Processing Query from node: " + originNode + ", key: " + key);

                if (key.equals("\"*\"")) {
                    String[] proj = new String[]{"key", "value"};
                    cursor = builder.query(db, proj, null, null,  null, null, null);

                    if (!cursor.moveToFirst()) {

                        System.out.println("node-" + node + " | EMPTY CURSOR returned for query key: " + key);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query_response", originNode, key, "", "star");

                    } else {
                        ArrayList<HashMap<String, String>> cursorMap = getListMapFromCursor(cursor);
                        StringBuilder sb = new StringBuilder();

                        for (int i = 0; i < cursorMap.size(); i++) {
                            HashMap<String, String> tempMap = cursorMap.get(i);
                            sb.append(tempMap.get("key"));
                            sb.append(":");
                            sb.append(tempMap.get("value"));

                            if (i < cursorMap.size()-1) {
                                sb.append("_");
                            }
                        }

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query_response", originNode, key, sb.toString(), "star");
                    }

                } else {

                    //System.out.println("node-" + node + " | Querying for key: " + key);

                    String[] proj = new String[]{"key", "value"};
                    cursor = builder.query(db, proj, "key=\"" + key + "\"", null, null, null, null);

                    if (!cursor.moveToFirst()) {
                        System.out.println("node-" + node + " | EMPTY CURSOR returned for query key: " + key);
                    }

                    if (originNode.equals(node)) {
                        publicCursor = cursor;
                        queryResponseMap.put(key, true);
                    } else {
                        ArrayList<HashMap<String, String>> cursorMap = getListMapFromCursor(cursor);
                        String value = cursorMap.get(0).get("value");

                        System.out.println("node-" + node + " | Processed Query from node: " + originNode + ", for key: " + key + ", value: " + value);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query_response", originNode, key, value);
                    }
                }

            } else if (strings[0].equals("query_response")) {
                /*
                0: msg.getType(),
                1: msg.getData().get("key"),
                2: msg.getData().get("value")
                3: msg.getNode()
                 */

                if (strings[1].equals("\"*\"")) {

                    starResponseCount++;
                    String result = strings[2];
                    String originNode = strings[3];

                    //System.out.println("node-" + node + " | Processing QueryResponse from node: " + originNode + ", for key: " + strings[1] + ", ResponseCount: " + starResponseCount);
                    //System.out.println("node-" + node + " | Result: " + result);

                    if (starResponseCount < N && result.length() > 0) {
                        String[] tuples = result.split("_");
                        for (String tuple: tuples) {
                            String[] tupleArray = tuple.split(":");
                            HashMap<String, String> tempMap = new HashMap<>();
                            tempMap.put("key", tupleArray[0]);
                            tempMap.put("value", tupleArray[1]);
                            starResultMap.add(tempMap);
                        }
                    } else if (result.length() == 0) {

                        System.out.println("node-" + node + " | EMPTY QUERY RESPONSE from node: " + originNode + ", for key: " + strings[1]);

                    }

                    if (starResponseCount == N-1) {
                        if (starResultMap.size() > 0) {
                            publicCursor = getCursorFromListMap(starResultMap);
                        }
                        starResultMap.clear();
                        starResponseCount = 0;

                        //System.out.println("node-" + node + " | Processed QueryResponse from node: " + originNode + ", for key: " + strings[1]);

                        queryResponseMap.put("\"*\"", true);
                    }

                } else {
                    HashMap<String, String> map = new HashMap<>();
                    map.put("key", strings[1]);
                    map.put("value", strings[2]);
                    ArrayList<HashMap<String, String>> listMap = new ArrayList<>();
                    listMap.add(map);
                    publicCursor = getCursorFromListMap(listMap);
                    queryResponseMap.put(strings[1], true);
                }
            } else if (strings[0].equals("delete")) {
                /*
                0: msg.getType(),
                1: msg.getData().get("key"),
                2: msg.getNode()
                 */

                String key = strings[1];

                if (key.equals("\"*\"")) {
                    db.delete(TABLE, null, null);
                } else {
                    db.delete(TABLE, "key=\"" + key + "\"", null);
                }

            } else if (strings[0].equals("recovery_query")) {
                /*
                0: msg.getType(),
                1: msg.getData().get("coordinator"),
                2: msg.getNode()
                 */

                String coord = strings[1];
                String originNode = strings[2];
                SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
                builder.setTables(TABLE);
                Cursor cur = builder.query(db, null, "coordinator=\"" + coord + "\"", null, null, null, null);

                //System.out.println("node-" + node + " | Processing RecoveryQuery from node: " + originNode + ", for coordinator: " + coord);
                //System.out.println("---- cur count: " + cur.getCount());

                if (cur.moveToFirst()) {
                    ArrayList<HashMap<String, String>> cursorMap = getListMapFromCursor(cur);
                    int cmSize = cursorMap.size();

                    //System.out.println("---- cmSize: " + cmSize);

                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < cursorMap.size(); i++) {
                        HashMap<String, String> tempMap = cursorMap.get(i);
                        StringBuilder tempSb = new StringBuilder();
                        String k = tempMap.get("key");
                        String v = tempMap.get("value");
                        String c = tempMap.get("coordinator");
                        sb.append(k);
                        tempSb.append(k);
                        sb.append(":");
                        tempSb.append(":");
                        sb.append(v);
                        tempSb.append(v);
                        sb.append(":");
                        tempSb.append(":");
                        sb.append(c);
                        tempSb.append(c);

                        //System.out.println("query tuple -> " + tempSb.toString());

                        if (i < cursorMap.size()-1) {
                            sb.append("_");
                        }
                    }

                    //System.out.println("node-" + node + " | Processed RecoveryQuery from node-" + originNode + ", for coordinator: " + coord);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "recovery_response", originNode, coord, sb.toString(), "" + cmSize + "");
                }

            } else if (strings[0].equals("recovery_response")) {
                /*
                0: msg.getType(),
                1: msg.getData().get("coordinator"),
                2: msg.getData().get("value"),
                3: msg.getNode()
                4: msg.getData().get("cursor_size")
                 */

                String coord = strings[1];
                String result = strings[2];
                //String originNode = strings[3];
                //int cursorSize = Integer.parseInt(strings[4]);
                //int localInserts = 0;
                //int counter = 0;
                ArrayList<HashMap<String, String>> recoveryResponseMap = new ArrayList<>();

                //System.out.println("node-" + node + " | Processing RecoveryResponse from node: " + originNode + ", for coordinator: " + coord);

                String[] tuples = result.split("_");

                for (String tuple: tuples) {

                    //System.out.println("response tuple -> " + tuple);

                    String[] tupleArray = tuple.split(":");
                    if (tupleArray.length != 3) {
                        System.out.println("node-" + node + " | coord: " + coord + ", tupleArray length: " + tupleArray.length);
                    }
                    String k = tupleArray[0];
                    String v = tupleArray[1];
                    String c = tupleArray[2];
                    HashMap<String, String> tempMap = new HashMap<>();
                    tempMap.put("key", k);
                    tempMap.put("value", v);
                    tempMap.put("coordinator", c);
                    recoveryResponseMap.add(tempMap);
                    //counter++;
                }

                //System.out.println("node-" + node + " | Recovery Inserting from node: " + originNode + ", for coordinator: " + coord);

                for (HashMap<String, String> insertMap : recoveryResponseMap) {
                    ContentValues cv = new ContentValues();
                    if (!insertMap.containsKey("key") || !insertMap.containsKey("value") || !insertMap.containsKey("coordinator")) {
                        System.out.println("node-" + node + " | INVALID insertMap!");
                    }
                    cv.put("key", insertMap.get("key"));
                    cv.put("value", insertMap.get("value"));
                    cv.put("coordinator", insertMap.get("coordinator"));
                    long row = db.insertWithOnConflict(TABLE, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
                    if (row == -1) {
                        System.out.println("node-" + node + " | NOT Recovery Inserted key: " + insertMap.get("key") + ", coordinator: " + insertMap.get("coordinator"));
                    }
                    Log.v("RECOVERY INSERT", "node-" + node + " | key: " + insertMap.get("key"));
                    //localInserts++;

                    //System.out.println("node-" + node + " | Recovery Inserted key: " + insertMap.get("key") + ", coordinator: " + insertMap.get("coordinator"));

                }

                //System.out.println("cursor_size: " + cursorSize + ", counter: " + counter + ", inserted: " + localInserts);

                //System.out.println("node-" + node + " | Processed RecoveryResponse from node-" + originNode + ", for coordinator: " + coord);

            }
        }
    }

    /* ClientTask */
    private class ClientTask extends AsyncTask<String, Void, Integer> {

        @Override
        protected Integer doInBackground(String... msgs) {
            if (msgs[0].equals("insert")) {
                // 0: "insert" | 1: node | 2: destNode | 3: key | 4: value

                HashMap<String, String> map = new HashMap<>();
                map.put("key", msgs[3]);
                map.put("value", msgs[4]);
                map.put("coordinator", msgs[2]);
                Message msg = new Message("insert", msgs[1], map);

                String coordinatorNode = msgs[2];
                String replicaNode1 = replicasMap.get(coordinatorNode)[0];
                String replicaNode2 = replicasMap.get(coordinatorNode)[1];

                int coordinatorPort = Integer.parseInt(coordinatorNode) * 2;
                int replicaPort1 = Integer.parseInt(replicaNode1) * 2;
                int replicaPort2 = Integer.parseInt(replicaNode2) * 2;
                int[] ports;
                if (coordinatorNode.equals(node)) {
                    ports = new int[]{replicaPort1, replicaPort2};
                } else if (replicaNode1.equals(node)) {
                    ports = new int[]{coordinatorPort, replicaPort2};
                } else if (replicaNode2.equals(node)) {
                    ports = new int[]{coordinatorPort, replicaPort1};
                } else {
                    ports = new int[]{coordinatorPort, replicaPort1, replicaPort2};
                }
                String currNode;

                for (int port : ports) {
                    currNode = String.valueOf((port / 2));
                    try {

                        //System.out.println("node-" + node + " | SEND-Insert to node: " + currNode + ", key: " + msgs[3] + ", coordinator: " + msgs[2]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        out.writeObject(msg);
                        out.flush();
                        out.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception in sending insert to node-" + currNode, e);
                    } catch (IOException e) {
                        Log.e(TAG, "IOException in sending insert to node-" + currNode, e);
                    }
                }

            } else if (msgs[0].equals("query")) {
                // 0: "query" | 1: destNode | 2: key

                HashMap<String, String> map = new HashMap<>();
                map.put("key", msgs[2]);
                Message msg = new Message("query", node, map);
                boolean starQuery;
                int[] ports;
                String currNode;
                int c = 0;
                int r1 = 0;
                int r2 = 0;
                if (msgs[1].equals("star") && msgs[2].equals("\"*\"")) {
                    // * query
                    starQuery = true;
                    ArrayList<Integer> tempPortList = new ArrayList<>();
                    for (String rn : rnodes) {
                        if (!rn.equals(node)) {
                            int rnPort = Integer.parseInt(rn) * 2;
                            tempPortList.add(rnPort);
                        }
                    }
                    ports = new int[tempPortList.size()];
                    for (int i = 0; i < ports.length; i++) {
                        ports[i] = tempPortList.get(i);
                    }

                } else {
                    // key query
                    starQuery = false;
                    c = Integer.parseInt(msgs[1]) * 2;
                    r1 = Integer.parseInt(replicasMap.get(msgs[1])[0]) * 2;
                    r2 = Integer.parseInt(replicasMap.get(msgs[1])[1]) * 2;
                    ports = new int[]{r2};
                }

                for (int port : ports) {
                    currNode = String.valueOf((port / 2));
                    try {

                        System.out.println("node-" + node + " | SEND-query to node: " + currNode + ", key: " + msgs[2]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        out.writeObject(msg);
                        out.flush();
                        out.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception in sending query to node-" + currNode, e);
                    } catch (IOException e) {
                        Log.e(TAG, "IOException in sending query to node-" + currNode, e);

                        System.out.println("node-" + node + " | IOException in sending Query to node: " + currNode + ", key: " + msgs[2]);

                        if (!starQuery) {
                            currNode = String.valueOf((r1 / 2));
                            try {

                                System.out.println("node-" + node + " | SEND-REquery to node: " + currNode + ", key: " + msgs[2]);

                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), r1);
                                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                out.flush();
                                out.writeObject(msg);
                                out.flush();
                                out.close();
                                socket.close();
                            } catch (UnknownHostException ee) {
                                Log.e(TAG, "Exception in sending re-query to node-" + currNode, ee);
                            } catch (IOException ee) {
                                Log.e(TAG, "Exception in sending re-query to node-" + currNode, ee);

                                System.out.println("node-" + node + " | IOException in sending ReQuery to node: " + currNode + ", key: " + msgs[2]);

                                if (!starQuery) {
                                    currNode = String.valueOf((c / 2));
                                    try {

                                        System.out.println("node-" + node + " | SEND-RE-REquery to node: " + currNode + ", key: " + msgs[2]);

                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), c);
                                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                        out.flush();
                                        out.writeObject(msg);
                                        out.flush();
                                        out.close();
                                        socket.close();
                                    } catch (UnknownHostException eee) {
                                        Log.e(TAG, "Exception in sending re-re-query to node-" + currNode, eee);
                                    } catch (IOException eee) {
                                        Log.e(TAG, "Exception in sending re-re-query to node-" + currNode, eee);

                                        System.out.println("node-" + node + " | IOException in sending ReReQuery to node: " + currNode + ", key: " + msgs[2]);
                                    }
                                }
                            }
                        } else {
                            starResponseCount++;
                            if (starResponseCount == N-1) {
                                if (starResultMap.size() > 0) {
                                    publicCursor = getCursorFromListMap(starResultMap);
                                }
                                starResultMap.clear();
                                starResponseCount = 0;

                                System.out.println("node-" + node + " | Processed * QueryResponse for key: " + msgs[2]);

                                queryResponseMap.put("\"*\"", true);
                            }
                        }
                    }
                }

                while (!queryResponseMap.get(msgs[2])) {
                    // wait for query response
                }

            } else if (msgs[0].equals("query_response")) {
                // 0: "query_response" | 1: originNode | 2: key | 3: value/result | 4: coord/"star"

                HashMap<String, String> map = new HashMap<>();
                map.put("key", msgs[2]);
                map.put("value", msgs[3]);

                Message msg = new Message("query_response", node, map);
                int port = Integer.parseInt(msgs[1]) * 2;

                try {

                    System.out.println("node-" + node + " | SEND-QueryResponse to node: " + msgs[1] + ", key: " + msgs[2]);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    out.writeObject(msg);
                    out.flush();
                    out.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Exception in sending query_response to node-" + msgs[1], e);
                } catch (IOException e) {
                    Log.e(TAG, "IOException in sending query_response to node-" + msgs[1], e);
                    System.out.println("node-" + node + " | IOException in sending QueryResponse to node: " + msgs[1] + ", key: " + msgs[2]);
                }

            } else if (msgs[0].equals("delete")) {
                // 0: "delete" | 1: destNode | 2: key

                HashMap<String, String> map = new HashMap<>();
                map.put("key", msgs[2]);
                Message msg = new Message("delete", node, map);

                String coordinatorNode = msgs[1];
                String replicaNode1 = replicasMap.get(coordinatorNode)[0];
                String replicaNode2 = replicasMap.get(coordinatorNode)[1];

                int coordinatorPort = Integer.parseInt(coordinatorNode) * 2;
                int replicaPort1 = Integer.parseInt(replicaNode1) * 2;
                int replicaPort2 = Integer.parseInt(replicaNode2) * 2;
                int[] ports;
                if (coordinatorNode.equals(node)) {
                    ports = new int[]{replicaPort1, replicaPort2};
                } else if (replicaNode1.equals(node)) {
                    ports = new int[]{coordinatorPort, replicaPort2};
                } else if (replicaNode2.equals(node)) {
                    ports = new int[]{coordinatorPort, replicaPort1};
                } else {
                    ports = new int[]{coordinatorPort, replicaPort1, replicaPort2};
                }
                String currNode;

                for (int port: ports) {
                    currNode = String.valueOf((port / 2));
                    try {

                        //System.out.println("node-" + node + " | SEND-Delete to node: " + currNode + ", key: " + msgs[2]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        out.writeObject(msg);
                        out.flush();
                        out.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception in sending delete to node-" + currNode, e);
                    } catch (IOException e) {
                        Log.e(TAG, "IOException in sending delete to node-" + currNode, e);

                        //System.out.println("IOException in sending delete to node-" + currNode);
                    }
                }

            } else if (msgs[0].equals("recovery_query")) {
                // 0: "recovery_query"

                HashMap<String, String> parent1Map = new HashMap<>();
                HashMap<String, String> parent2Map = new HashMap<>();
                HashMap<String, String> replica1Map = new HashMap<>();
                parent1Map.put("coordinator", parentsMap.get(node)[0]);
                parent2Map.put("coordinator", parentsMap.get(node)[1]);
                replica1Map.put("coordinator", node);
                Message parent1Msg = new Message("recovery_query", node, parent1Map);
                Message parent2Msg = new Message("recovery_query", node, parent2Map);
                Message replica1Msg = new Message("recovery_query", node, replica1Map);
                int parent1 = Integer.parseInt(parentsMap.get(node)[0]) * 2;
                int parent2 = Integer.parseInt(parentsMap.get(node)[1]) * 2;
                int replica1 = Integer.parseInt(replicasMap.get(node)[1]) * 2;
                //int replica2 = Integer.parseInt(replicasMap.get(node)[1]) * 2;
                int[] ports = new int[]{parent1, parent2, replica1};
                Message[] msgArray = new Message[]{parent1Msg, parent2Msg, replica1Msg};
                String currNode;
                int count = 0;

                for (int port : ports) {
                    currNode = String.valueOf((port / 2));
                    Message msg = msgArray[count];
                    try {

                        System.out.println("node-" + node + " | SEND-RecoveryQuery to node: " + currNode + ", coordinator: " + msg.getData().get("coordinator"));

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        out.writeObject(msg);
                        out.flush();
                        out.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception in sending recovery query to node-" + currNode, e);
                        //System.out.println("node-" + node + " | IOException in sending RecoveryQuery to node: " + currNode);
                    } catch (IOException e) {
                        Log.e(TAG, "IOException in sending recovery query to node-" + currNode, e);
                    }
                    count++;
                }
            } else if (msgs[0].equals("recovery_response")) {
                // 0: "recovery_response" | 1: originNode | 2: coord | 3: result/value | 4: cursorMapsize

                HashMap<String, String> map = new HashMap<>();
                map.put("coordinator", msgs[2]);
                map.put("value", msgs[3]);
                map.put("cursor_size", msgs[4]);
                Message msg = new Message("recovery_response", node, map);
                int port = Integer.parseInt(msgs[1]) * 2;

                try {

                    System.out.println("node-" + node + " | SEND-RecoveryResponse to node: " + msgs[1] + ", coordinator: " + msgs[2]);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    out.writeObject(msg);
                    out.flush();
                    out.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Exception in sending recovery response to node-" + msgs[1], e);
                } catch (IOException e) {
                    Log.e(TAG, "IOException in sending recovery response to node-" + msgs[1], e);
                    //System.out.println("node-" + node + " | IOException in sending RecoveryResponse to node: " + msgs[1]);
                }

            }

            return 0;
        }

    }

    public String getDataCoordinator(String hkey) {
        if (hkey == null || hkey.length() < 1) {
            System.out.println("INVALID hkey: " + hkey);
            return null;
        }

        // check if this node is coordinator
        String[] hashparents = hashparentsMap.get(node);
        if (node.equals(rnodes[0])) {
            // first node in the ring
            if (hkey.compareTo(hashnode) < 0) {
                return node;
            } else if (hkey.compareTo(hashnode) > 0 && hkey.compareTo(hashparents[1]) > 0) {
                return node;
            }
        } else if (hkey.compareTo(hashnode) < 0 && hkey.compareTo(hashparents[1]) > 0) {
            return node;
        }

        for (int i = 0; i < hnodes.length; i++) {
            if (i != nodeindex) {
                if (i == 0) {
                    if (hkey.compareTo(hnodes[i]) < 0) {
                        return rnodes[i];
                    } else if (hkey.compareTo(hnodes[i]) > 0 && hkey.compareTo(hnodes[hnodes.length-1]) > 0) {
                        return rnodes[i];
                    }
                } else {
                    if (hkey.compareTo(hnodes[i]) < 0 && hkey.compareTo(hnodes[i-1]) > 0) {
                        return rnodes[i];
                    }
                }
            }
        }

        return null;
    }

    public ArrayList<HashMap<String, String>> getListMapFromCursor(Cursor cr) {

        ArrayList<HashMap<String, String>> crMap = new ArrayList<>();
        int keyIndex = cr.getColumnIndex("key");
        int valueIndex = cr.getColumnIndex("value");
        int coordIndex = cr.getColumnIndex("coordinator");
        boolean hasCoordinator = true;
        if (coordIndex == -1) {
            hasCoordinator = false;
        }

        while (!cr.isAfterLast()) {
            HashMap<String, String> map = new HashMap<>();
            map.put("key", cr.getString(keyIndex));
            map.put("value", cr.getString(valueIndex));
            if (hasCoordinator) {
                map.put("coordinator", cr.getString(coordIndex));
            }
            crMap.add(map);
            cr.moveToNext();
        }
        //System.out.println("crMap size:: " + crMap.size());

        return crMap;
    }

    public Cursor getCursorFromListMap(ArrayList<HashMap<String, String>> cMap) {
        boolean hasCoordinator;
        HashMap<String, String> tmpMap = cMap.get(0);
        if (tmpMap.containsKey("coordinator")) {
            hasCoordinator = true;
        } else {
            hasCoordinator = false;
        }
        String[] colNames;
        if (hasCoordinator) {
            colNames = new String[]{"key", "value", "coordinator"};
        } else {
            colNames = new String[]{"key", "value"};
        }
        MatrixCursor mc = new MatrixCursor(colNames);

        for (HashMap<String, String> tupleMap : cMap) {
            String key = tupleMap.get("key");
            String value = tupleMap.get("value");
            String[] row;

            if (hasCoordinator) {
                String coordinator = tupleMap.get("coordinator");
                row = new String[]{key, value, coordinator};
            } else {
                row = new String[]{key, value};
            }

            mc.addRow(row);
        }

        return mc;
    }

    public void computeNeighbors() {

        for (int i = 0; i < rnodes.length; i++) {
            String thisNode = rnodes[i];
            if (thisNode.equals(node)) {
                nodeindex = i;
            }
            String[] parents = new String[2];
            String[] hashparents = new String[2];
            String[] replicas = new String[2];
            String[] hashreplicas = new String[2];
            switch (i) {
                case 0:
                    parents[0] = rnodes[3];
                    hashparents[0] = hnodes[3];
                    parents[1] = rnodes[4];
                    hashparents[1] = hnodes[4];

                    replicas[0] = rnodes[1];
                    hashreplicas[0] = hnodes[1];
                    replicas[1] = rnodes[2];
                    hashreplicas[1] = hnodes[2];
                    break;
                case 1:
                    parents[0] = rnodes[4];
                    hashparents[0] = hnodes[4];
                    parents[1] = rnodes[0];
                    hashparents[1] = hnodes[0];

                    replicas[0] = rnodes[2];
                    hashreplicas[0] = hnodes[2];
                    replicas[1] = rnodes[3];
                    hashreplicas[1] = hnodes[3];
                    break;
                case 2:
                    parents[0] = rnodes[0];
                    hashparents[0] = hnodes[0];
                    parents[1] = rnodes[1];
                    hashparents[1] = hnodes[1];

                    replicas[0] = rnodes[3];
                    hashreplicas[0] = hnodes[3];
                    replicas[1] = rnodes[4];
                    hashreplicas[1] = hnodes[4];
                    break;
                case 3:
                    parents[0] = rnodes[1];
                    hashparents[0] = hnodes[1];
                    parents[1] = rnodes[2];
                    hashparents[1] = hnodes[2];

                    replicas[0] = rnodes[4];
                    hashreplicas[0] = hnodes[4];
                    replicas[1] = rnodes[0];
                    hashreplicas[1] = hnodes[0];
                    break;
                case 4:
                    parents[0] = rnodes[2];
                    hashparents[0] = hnodes[2];
                    parents[1] = rnodes[3];
                    hashparents[1] = hnodes[3];

                    replicas[0] = rnodes[0];
                    hashreplicas[0] = hnodes[0];
                    replicas[1] = rnodes[1];
                    hashreplicas[1] = hnodes[1];
                    break;
                default:
                    parents[0] = rnodes[i-2];
                    hashparents[0] = hnodes[i-2];
                    parents[1] = rnodes[i-1];
                    hashparents[1] = hnodes[i-1];

                    replicas[0] = rnodes[i+1];
                    hashreplicas[0] = hnodes[i+1];
                    replicas[1] = rnodes[i+2];
                    hashreplicas[1] = hnodes[i+2];
                    break;
            }
            parentsMap.put(thisNode, parents);
            hashparentsMap.put(thisNode, hashparents);
            replicasMap.put(thisNode, replicas);
            hashreplicasMap.put(thisNode, hashreplicas);
            nodeStatus.put(thisNode, true);
        }
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("NODE: " + node);
        //  System.out.println("REPLICAS: " + replicasMap.get(node)[0] + ", " + replicasMap.get(node)[1]);
        System.out.println("-----------------------------------------------------------------------------------------");
        return;
    }

}
