package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by jesal on 4/17/15.
 */
public class Message implements Serializable {

    private String type;
    private String node;
    private HashMap<String, String> datamap;
    //private ArrayList<HashMap<String, String>> cursormap;

    //public Message(String t, String n, HashMap<String, String> dm, ArrayList<HashMap<String, String>> cm) {
    public Message(String t, String n, HashMap<String, String> dm) {
        type = t;
        node = n;
        datamap = dm;
        //cursormap = cm;
    }

    public String getType() {
        return type;
    }

    public HashMap<String, String> getData() {
        return datamap;
    }

    public String getNode() {
        return node;
    }

    /*public ArrayList<HashMap<String, String>> getCursorMap() {
        return cursormap;
    }*/

    @Override
    public String toString() {
        //return "Message (\n" + "\ttype: " + type + "\n\tnode: " + node + "\n\tdatamap: " + datamap + "\n\tcursormap: " + cursormap + "\n)";
        return "Message (\n" + "\ttype: " + type + "\n\tnode: " + node + "\n\tdatamap: " + datamap + "\n)";
    }

}
