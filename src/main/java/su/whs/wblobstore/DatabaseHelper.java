package su.whs.wblobstore;

import android.database.DatabaseErrorHandler;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.os.Build;
import android.util.Log;

import java.io.File;

/**
 * Created by igor n. boulliev on 01.01.16.
 */
public abstract class DatabaseHelper {
    private static final String TAG="DatabaseHelper";
    private SQLiteDatabase mDB;
    private boolean mIsInitializing = false;
    private File mDBF;
    private SQLiteDatabase.CursorFactory mFactory;
    private int mVersion;
    private DatabaseErrorHandler mDatabaseErrorHandler;
    private boolean DEBUG_STRICT_READONLY = false;
    private boolean mEnableWriteAheadLogging = false;

    public DatabaseHelper(String path, String name, SQLiteDatabase.CursorFactory factory, int version) {
        mDBF = new File(path,name+".sqlite");
        mFactory = factory;
        mVersion = version;
        mDB = getDatabaseLocked(false);
    }

    public void onConfigure(SQLiteDatabase db) {}

    protected abstract void onCreate(SQLiteDatabase db);
    protected abstract void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion);
    protected void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {}
    protected void onOpen(SQLiteDatabase db) {}

    public SQLiteDatabase getReadableDatabase() { return getDatabaseLocked(false); }
    public SQLiteDatabase getWritableDatabase() { return getDatabaseLocked(true); }

    private synchronized SQLiteDatabase getDatabaseLocked(boolean writable) {
        if (mDB != null) {
            if (!mDB.isOpen()) {
                // Darn!  The user closed the database by calling mDatabase.close().
                mDB = null;
            } else if (!writable || !mDB.isReadOnly()) {
                // The database is already open for business.
                return mDB;
            }
        }

        if (mIsInitializing) {
            throw new IllegalStateException("getDatabase called recursively");
        }

        SQLiteDatabase db = mDB;

        try {
            mIsInitializing = true;

            if (db != null) {
                if (writable && db.isReadOnly()) {
                    db.close();
                    db = SQLiteDatabase.openDatabase(mDBF.getAbsolutePath(),mFactory,SQLiteDatabase.OPEN_READWRITE);
                }
            } else if (mDB == null) {
                db = SQLiteDatabase.openOrCreateDatabase(mDBF.getAbsolutePath(),mFactory);
            } else { // db not null
                try {
                    if (DEBUG_STRICT_READONLY && !writable) {
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
                            db = SQLiteDatabase.openDatabase(mDBF.getAbsolutePath(), mFactory,
                                    SQLiteDatabase.OPEN_READONLY, mDatabaseErrorHandler);
                        } else {
                            db = SQLiteDatabase.openDatabase(mDBF.getAbsolutePath(), mFactory,
                                    SQLiteDatabase.OPEN_READONLY);
                        }
                    } else {
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
                            db = SQLiteDatabase.openOrCreateDatabase(mDBF.getAbsolutePath(),
                                    mFactory, mDatabaseErrorHandler);
                        } else {
                            db = SQLiteDatabase.openOrCreateDatabase(mDBF.getAbsolutePath(),
                                    mFactory);
                        }
                    }
                } catch (SQLiteException ex) {
                    if (writable) {
                        throw ex;
                    }
                    Log.e(TAG, "Couldn't open " + mDBF.getAbsolutePath()
                            + " for writing (will try read-only):", ex);
                    final String path = mDBF.getAbsolutePath();
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
                        db = SQLiteDatabase.openDatabase(path, mFactory,
                                SQLiteDatabase.OPEN_READONLY, mDatabaseErrorHandler);
                    } else {
                        db = SQLiteDatabase.openDatabase(path,mFactory,SQLiteDatabase.OPEN_READONLY);
                    }
                }
            }

            onConfigure(db);

            final int version = db.getVersion();
            if (version != mVersion) {
                if (db.isReadOnly()) {
                    throw new SQLiteException("Can't upgrade read-only database from version " +
                            db.getVersion() + " to " + mVersion + ": " + mDBF.getAbsolutePath());
                }

                db.beginTransaction();
                try {
                    if (version == 0) {
                        onCreate(db);
                    } else {
                        if (version > mVersion) {
                            onDowngrade(db, version, mVersion);
                        } else {
                            onUpgrade(db, version, mVersion);
                        }
                    }
                    db.setVersion(mVersion);
                    db.setTransactionSuccessful();
                } finally {
                    db.endTransaction();
                }
            }

            onOpen(db);

            if (db.isReadOnly()) {
                Log.w(TAG, "Opened " + mDBF.getAbsolutePath() + " in read-only mode");
            }

            mDB = db;
            return db;
        } finally {
            mIsInitializing = false;
            if (db != null && db != mDB) {
                db.close();
            }
        }
    }
}
