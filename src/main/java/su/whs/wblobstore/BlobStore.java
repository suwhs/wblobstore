package su.whs.wblobstore;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.text.TextUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

/**
 * Created by igor n. boulliev on 31.12.15.
 */
public class BlobStore {

    public InputStream getCachedStream(String url) {
        Key k = getKey(url);
        if (k.Load()) {
            try {
                return k.Get();
            } catch (FileNotFoundException e) {
            }
        }
        return null;
    }

    public Key getKey(String url) {
        return new Key(url);
    }

    public OutputStream openCacheStream(String url) throws IOException {
        Key k = new Key(url);
        if (k.Load()) return null;
        return k.Put();
    }

    public void attachProperty(String url, String name, String value) throws IOException {
        Key k = new Key(url);
        if (k.Load()) {
            k.Add(name,value);
            k.Commit();
        }
    }

    public String getAttachedProperty(String key, String name) {
        Key k = new Key(key);
        if (k.Load()) {
            Key p = k.Child(name);
            if (p!=null) {
                return p.StringValue();
            }
        }
        return null;
    }

    public enum LOCATION {
        INTERNAL,
        EXTERNAL,
        PATH
    }

    private Context mContext;
    private File mFileStoreDir;
    private File mCacheStoreDir;
    private LOCATION mLocation;
    private String mPath;
    private boolean mStateNominal = true;
    private boolean mFirstRun = false;
    private DatabaseHelper mDatabaseHelper;
    private File mDataBaseDir;

    public BlobStore(Context context, LOCATION location, String path) {
        mContext = context;
        SharedPreferences prefs = context.getSharedPreferences("whs.blobstore.cfg",Context.MODE_PRIVATE);
        String migrateFromPath = null;
        boolean migrateRequired = false;
        if (prefs.contains("location")) {
            int loc = prefs.getInt("location",0);
            LOCATION storedLocation = LOCATION.EXTERNAL;
            switch(loc) {
                case 0:
                    storedLocation = LOCATION.EXTERNAL;
                    break;
                case 1:
                    storedLocation = LOCATION.INTERNAL;
                    break;
                case 2:
                    if (prefs.contains("path")) {
                        String oldPath = prefs.getString("path",null);
                        if (!TextUtils.isEmpty(oldPath)) {
                            migrateRequired = true;
                        } else {
                            migrateFromPath = path;
                        }
                    } else {
                        storedLocation = LOCATION.EXTERNAL;
                    }
                    break;
            }
            if (storedLocation!=location||migrateRequired) {
                migrateBlobStore(location,path,prefs);
            }
            mLocation = storedLocation;
            mPath = prefs.getString("path",null);
            /* */

        } else {
            mFirstRun = true;
            mLocation = location;
            mPath = path;
            storeLocationAndPath(prefs,location,path);
        }

        switch (mLocation) {
            case EXTERNAL:

        }

    }

    protected void migrateBlobStore(LOCATION newLocation, String newPath, SharedPreferences prefs) {
        throw new IllegalStateException("migration not implemented yet");
    }

    protected void externalStoreNotAvailable(Context context) {
        if (mFirstRun) {
            initInternalLocation(context);
        }
    }

    private void initInternalLocation(Context context) {
        mLocation = LOCATION.INTERNAL;
        mFileStoreDir = new File(context.getFilesDir(),".bstore");
        mCacheStoreDir = new File(context.getCacheDir(),".bcache");
        mDataBaseDir = new File(context.getFilesDir(),".databases");
        if (!mFileStoreDir.exists()) {
            if(!mFileStoreDir.mkdirs()) {
                mStateNominal = false;
            }
        }
        if (!mCacheStoreDir.exists()) {
            if (!mCacheStoreDir.mkdirs()) {
                mStateNominal = false;
            }
        }
        if (!mDataBaseDir.exists()) {
            if (!mDataBaseDir.mkdirs()) {
                mStateNominal = false;
            }
        }
    }

    private void initExternalLocation(Context context) {
        mLocation = LOCATION.EXTERNAL;
        mFileStoreDir = new File(context.getExternalFilesDir(null),".bstore");
        mCacheStoreDir = new File(context.getExternalCacheDir(),".bcache");
        mDataBaseDir = new File(context.getExternalFilesDir(null),".databases");

        if (!mFileStoreDir.exists()) {
            if(!mFileStoreDir.mkdirs()) {
                externalStoreNotAvailable(context);
                return;
            }
        }
        if (!mCacheStoreDir.exists()) {
            if (!mCacheStoreDir.mkdirs()) {
                externalStoreNotAvailable(context);
                return;
            }
        }
        if (!mDataBaseDir.exists()) {
            if (!mDataBaseDir.mkdirs()) {
                externalStoreNotAvailable(context);
                return;
            }
        }
        mDatabaseHelper = getDatabaseHelper();
    }

    private void storeLocationAndPath(SharedPreferences prefs, LOCATION location, String path) {
        SharedPreferences.Editor e = prefs.edit();
        int loc = 0;
        switch (location) {
            case INTERNAL:
                loc = 1;
                break;
            case PATH:
                loc = 2;
                break;

        }
        e.putInt("location",loc);
        if (loc==2) e.putString("path",path);
        e.commit();
    }

    public Context getContext() { return mContext; }

    /* BlobStoreDatabase */
    private static int DB_VERSION = 1;

    private DatabaseHelper getDatabaseHelper() {
        return new DatabaseHelper(mDataBaseDir.getAbsolutePath(),"blobstore", null, DB_VERSION) {
            @Override
            protected void onCreate(SQLiteDatabase db) {
                db.execSQL("create table pairs(_id integer primary key, parent integer, keyhash integer, keyname text, value text, flags integer(1), modified integer");
                db.execSQL("create index pairs_idx on pairs(parent,keyhash)");
            }

            @Override
            protected void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

            }
        };
    }



    public class Key {
        static final int VALUE_FILE = 0;
        static final int VALUE_DATA = 1;
        static final int VALUE_EMPTY = 2;
        static final int VALUE_PERSISTENT = 4;
        private long mId = -1;
        private Key mParent;
        private String mName;
        private int mFlags = 2;
        private String mValue;
        private int mModified;
        private Key[] mChilds = null;

        public Key(String name) {
            mName = name;
            mId = -1;
        }

        public boolean Load() {
            SQLiteDatabase db = mDatabaseHelper.getReadableDatabase();
            Cursor c = db.query("pairs",new String[]{"_id","keyname","flags","modified","value"},"parent=? AND keyhash=? AND keyname=?",
                    new String[] {String.valueOf(mParent==null? 0 : mParent.mId), String.valueOf(mName.hashCode()), mName},null,null,null);
            if (c.moveToFirst()) {
                fromCursor(c);
            } else {
                c.close();
                return false;
            }
            c.close();
            return true;
        }

        public Key(Key parent, String name) {
            this(name);
            mParent = parent;
        }

        public Key(Cursor cursor) {
            fromCursor(cursor);
        }

        private void fromCursor(Cursor cursor) {
            mId = cursor.getLong(cursor.getColumnIndex("_id"));
            mName = cursor.getString(cursor.getColumnIndex("keyname"));
            mFlags = cursor.getInt(cursor.getColumnIndex("flags"));
            mModified = cursor.getInt(cursor.getColumnIndex("modified"));
            mValue = cursor.getString(cursor.getColumnIndex("value"));
        }

        public Key(Key parent, Cursor cursor) {
            this(cursor);
            mParent = parent;
        }

        public Key getParent() { return mParent; }
        public boolean hasParent() { return mParent!=null; }

        /**
         * delete key and it's descedant
         */

        public void Delete() {
            if (mId<0) return;
            Key[] childs = Childs();
            if (childs!=null) for(Key c : childs) c.Delete();
            if ((mFlags & VALUE_FILE)>0) {
                if ((mFlags & VALUE_PERSISTENT)>0) {
                    delete_file(String.format("%s.dat",mId));
                } else {
                    delete_cache(String.format("%s.dat",mId));
                }
            }
            if (mParent!=null) mParent.invalidateChild();
            SQLiteDatabase db = mDatabaseHelper.getWritableDatabase();
            db.delete("pairs", "_id=?", new String[]{String.valueOf(mId)});
        }


        public void Put(InputStream stream, int flags) throws IOException {
            OutputStream out;
            if ((flags & VALUE_PERSISTENT)>0) {
                out = get_file_output_stream(String.format("%s.dat",mId));
            } else {
                out = get_cache_output_stream(String.format("%s.dat",mId));
            }
            byte[] buffer = new byte[65535];

            for(int read = stream.read(buffer);read>-1; read = stream.read(buffer)) {
                out.write(buffer,0,read);
            }
            out.close();
        }

        public void Put(InputStream stream) throws IOException {
            if ((mFlags & VALUE_FILE)==0) {
                mFlags = VALUE_FILE | (mFlags & VALUE_PERSISTENT);
                Commit();
            }
            Put(stream, VALUE_FILE);
        }

        public OutputStream Put() throws IOException {
            if ((mFlags & VALUE_FILE)==0) {
                mFlags = VALUE_FILE | (mFlags & VALUE_PERSISTENT);
                Commit();
            }
            if ((mFlags & VALUE_PERSISTENT)>0) {
                return get_file_output_stream(String.format("%s.dat",mId));
            }
            return get_cache_output_stream(String.format("%s.dat",mId));
        }

        public void Put(String data) {
            mValue = data;
            if ((mFlags&VALUE_EMPTY)>0) {
                mFlags = VALUE_DATA | (mFlags & VALUE_PERSISTENT);
            }
            Commit();
        }

        public InputStream Get() throws FileNotFoundException {
            if ((mFlags&VALUE_DATA)>0) {
                return new ByteArrayInputStream(mValue.getBytes());
            }
            if ((mFlags&VALUE_PERSISTENT)>0) {
                return get_file_input_stream(String.format("%s.dat", mId));
            }
            return get_cache_input_stream(String.format("%s.dat", mId));
        }

        public Key Add(String name, InputStream value) throws IOException {
            return Add(name, value, VALUE_FILE);
        }

        public Key Add(String name, InputStream value, int flags) throws IOException {
            invalidateChild();
            if (mId<0) {
                mId = Commit();
            }
            Key result = new Key(this,name);
            result.Put(value);
            return result;
        }

        public Key Add(String name, String data) throws IOException {
            return Add(name, new ByteArrayInputStream(data.getBytes()), VALUE_DATA);
        }

        public Key[] Childs() {
            synchronized (this) {
                if (mChilds!=null) return mChilds;
            }
            SQLiteDatabase db = mDatabaseHelper.getReadableDatabase();

            Cursor c = db.query("pairs", new String[]{"keyname", "value", "flags", "modified"}, "parent=?",
                    new String[]{String.valueOf(mId)}, null, null, null);
            if (c.moveToFirst()) {
                Key[] result = new Key[c.getCount()];
                for(int i=0; i<c.getCount(); i++) {
                    result[i] = new Key(this,c);
                }
                return result;
            }
            return null;
        }

        protected synchronized void invalidateChild() {
            if (mChilds!=null) mChilds = null;
            if (mParent!=null) mParent.invalidateChild();
        }

        public long Commit() {
            SQLiteDatabase db = mDatabaseHelper.getWritableDatabase();
            ContentValues cv = new ContentValues();
            cv.put("modified",(new Date()).getTime());
            cv.put("flags", mFlags);
            if (mId<0) {
                if (mParent!=null) {
                    if (mParent.mId<0) mParent.Commit();
                    cv.put("parent",mParent.mId);
                }
                mId = db.insert("pairs",null,cv);
            } else {
                cv.put("keyname",mName);
                cv.put("keyhash",mName.hashCode());
                db.update("pairs",cv,"_id=?",new String[] { String.valueOf(mId)});
            }
            return mId;
        }


        public Key Child(String name) {
            Key[] childs = Childs();
            for (Key k : childs) {
                if (name.equals(k.mName)) {
                    return k;
                }
            }
            return null;
        }

        public String StringValue() {
            return mValue;
        }

        public String GetName() {
            return mName;
        }
    }

    private void delete_file(String name) {
        File del = new File(mFileStoreDir,name);
        if (del.exists()) {
            del.delete();
        }
    }

    private void delete_cache(String name) {
        File del = new File(mCacheStoreDir,name);
        if (del.exists())
            del.delete();
    }

    private InputStream get_file_input_stream(String name) throws FileNotFoundException {
        return new FileInputStream(new File(mFileStoreDir,name));
    }

    private InputStream get_cache_input_stream(String name) throws FileNotFoundException {
        return new FileInputStream(new File(mCacheStoreDir,name));
    }

    private OutputStream get_file_output_stream(String name) throws IOException {
        File out = new File(mFileStoreDir,name);
        if (out.createNewFile())
            return new FileOutputStream(out);
        throw new IOException();
    }

    private OutputStream get_cache_output_stream(String name) throws IOException {
        File out = new File(mCacheStoreDir,name);
        if (out.createNewFile())
            return new FileOutputStream(out);
        throw new IOException();
    }
}
