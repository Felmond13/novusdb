package com.NovusDB;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import java.io.File;

/**
 * NovusDB Java Driver — JNA wrapper over the NovusDB C shared library.
 *
 * <pre>
 * Usage:
 *   NovusDB db = new NovusDB("ma_base.dlite");
 *   String result = db.exec("SELECT * FROM users");
 *   System.out.println(result); // JSON string
 *
 *   long id = db.insertJSON("users", "{\"name\": \"Alice\", \"age\": 30}");
 *   String[] collections = db.collections();
 *
 *   db.close();
 * </pre>
 *
 * Requires JNA on the classpath:
 *   https://github.com/java-native-access/jna
 */
public class NovusDB implements AutoCloseable {

    /**
     * JNA interface mapping to the NovusDB C API.
     */
    public interface NovusDBLib extends Library {
        long NovusDB_open(String path);
        int NovusDB_close(long handle);
        String NovusDB_exec(long handle, String sql);
        long NovusDB_insert_json(long handle, String collection, String jsonStr);
        String NovusDB_collections(long handle);
        String NovusDB_error(long handle);
        String NovusDB_dump(long handle);
        void NovusDB_free(String ptr);
    }

    private static NovusDBLib lib;
    private long handle;

    /**
     * Open a NovusDB database.
     *
     * @param dbPath Path to the .dlite database file.
     */
    public NovusDB(String dbPath) {
        this(dbPath, null);
    }

    /**
     * Open a NovusDB database with an explicit library path.
     *
     * @param dbPath  Path to the .dlite database file.
     * @param libPath Optional explicit path to the shared library directory.
     */
    public NovusDB(String dbPath, String libPath) {
        if (lib == null) {
            if (libPath != null) {
                System.setProperty("jna.library.path", libPath);
            }
            String libName = findLibraryName();
            lib = Native.load(libName, NovusDBLib.class);
        }

        this.handle = lib.NovusDB_open(dbPath);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to open database: " + dbPath);
        }
    }

    /**
     * Execute a SQL query.
     *
     * @param sql SQL query string.
     * @return JSON string: {"docs":[...], "rows_affected":N, "last_insert_id":N}
     * @throws RuntimeException if the query fails.
     */
    public String exec(String sql) {
        String raw = lib.NovusDB_exec(handle, sql);
        if (raw != null && raw.contains("\"error\"")) {
            throw new RuntimeException("Query error: " + raw);
        }
        return raw;
    }

    /**
     * Insert a raw JSON document into a collection.
     *
     * @param collection Collection name.
     * @param jsonStr    JSON document string.
     * @return Inserted document ID.
     */
    public long insertJSON(String collection, String jsonStr) {
        long id = lib.NovusDB_insert_json(handle, collection, jsonStr);
        if (id < 0) {
            String err = lastError();
            throw new RuntimeException(err != null && !err.isEmpty() ? err : "insertJSON failed");
        }
        return id;
    }

    /**
     * List all collections.
     *
     * @return JSON array string: ["col1", "col2", ...]
     */
    public String collections() {
        return lib.NovusDB_collections(handle);
    }

    /**
     * Get the last error message.
     */
    public String lastError() {
        return lib.NovusDB_error(handle);
    }

    /**
     * Get the full SQL dump of the database.
     */
    public String dump() {
        return lib.NovusDB_dump(handle);
    }

    /**
     * Close the database connection.
     */
    @Override
    public void close() {
        if (handle != 0) {
            lib.NovusDB_close(handle);
            handle = 0;
        }
    }

    private static String findLibraryName() {
        // JNA resolves platform-specific names automatically:
        //   "NovusDB" -> NovusDB.dll (Windows), libNovusDB.so (Linux), libNovusDB.dylib (macOS)
        // Check common locations
        String[] searchDirs = {
            ".",
            System.getProperty("user.dir"),
            new File(NovusDB.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent(),
        };

        String fileName = Platform.isWindows() ? "NovusDB.dll"
                         : Platform.isMac() ? "libNovusDB.dylib"
                         : "libNovusDB.so";

        for (String dir : searchDirs) {
            if (dir != null && new File(dir, fileName).exists()) {
                System.setProperty("jna.library.path",
                    System.getProperty("jna.library.path", "") + File.pathSeparator + dir);
                break;
            }
        }

        return "NovusDB";
    }

    /**
     * CLI usage example.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java NovusDB <database.dlite> [sql]");
            System.exit(1);
        }

        try (NovusDB db = new NovusDB(args[0])) {
            if (args.length > 1) {
                StringBuilder sql = new StringBuilder();
                for (int i = 1; i < args.length; i++) {
                    if (sql.length() > 0) sql.append(" ");
                    sql.append(args[i]);
                }
                System.out.println(db.exec(sql.toString()));
            } else {
                System.out.println("Connected to " + args[0]);
                System.out.println("Collections: " + db.collections());
            }
        }
    }
}
