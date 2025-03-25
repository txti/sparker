/*
 * Decompiled with CFR 0.152.
 */
package SparkER.DataLoaders;

import SparkER.DataStructures.EntityProfile;
import SparkER.DataStructures.IdDuplicates;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;


public final class SerializedLoader {
    public static HashSet<IdDuplicates> loadSerializedGroundtruth(String fileName) {
        Object obj = null;
        try {
            FileInputStream file = new FileInputStream(fileName);
            BufferedInputStream buffer = new BufferedInputStream(file);
            ObjectInputStream input = new ObjectInputStream(buffer);
            obj = input.readObject();
            input.close();
        }
        catch (IOException iOException) {
            System.err.println(fileName);
            iOException.printStackTrace();
        }
        catch (ClassNotFoundException classNotFoundException) {
            System.err.println(fileName);
            classNotFoundException.printStackTrace();
        }
        return (HashSet) obj;
    }

    public static ArrayList<EntityProfile> loadSerializedDataset(String fileName) {
        Object obj = null;
        try {
            FileInputStream file = new FileInputStream(fileName);
            BufferedInputStream buffer = new BufferedInputStream(file);
            ObjectInputStream input = new ObjectInputStream(buffer);
            obj = input.readObject();
            input.close();
        }
        catch (IOException iOException) {
            System.err.println(fileName);
            iOException.printStackTrace();
        }
        catch (ClassNotFoundException classNotFoundException) {
            System.err.println(fileName);
            classNotFoundException.printStackTrace();
        }
        return (ArrayList) obj;
    }
}
