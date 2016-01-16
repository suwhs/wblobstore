package su.whs.wblobstore;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.text.TextUtils;
import android.util.Log;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.WeakHashMap;

/**
 * Created by igor n. boulliev on 31.12.15.
 */
public class BlobStore {

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
    private WeakHashMap<String,Key> mKeysCache = new WeakHashMap<String,Key>();

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
                initExternalLocation(context);
                break;
            case INTERNAL:
                initInternalLocation(context);
                break;
            default:
                initExternalLocation(context);
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

    public synchronized Key getKey(String url) {
        if (mKeysCache.containsKey(url)) {
            return mKeysCache.get(url);
        }
        Key result = new Key(url);
        mKeysCache.put(url,result);
        return result;
    }

    public synchronized void removeKey(String url) {
        Key k;
        if (mKeysCache.containsKey(url)) {
            k = mKeysCache.remove(url);
        } else {
            k = new Key(url);
        }
        if (k.mName==null) {
            Log.e("BS","key name are null!");
        }
        if (k.Load())
            k.Delete();
    }


    public OutputStream openCacheStream(String url) throws IOException {
        Key k = getKey(url);
        if (!k.Load()) k.Commit();
        return k.Put();
    }

    public void attachProperty(String url, String name, String value) throws IOException {
        Key k = getKey(url);
        if (k.Load()) {
            k.Add(name,value);
            k.Commit();
        }
    }

    public String getAttachedProperty(String url, String name) {
        Key k = getKey(url);
        if (k.Load()) {
            Key p = k.Child(name);
            if (p!=null) {
                return p.StringValue();
            }
        }
        return null;
    }

    public String getDatabasesPath() {
        return mDataBaseDir.getAbsolutePath();
    }

    public SQLiteDatabase getDatabase() {
        return mDatabaseHelper.getReadableDatabase();
    }


    /* BlobStoreDatabase */
    private static int DB_VERSION = 1;

    private DatabaseHelper getDatabaseHelper() {
        return new DatabaseHelper(mDataBaseDir.getAbsolutePath(),"blobstore", null, DB_VERSION) {
            @Override
            protected void onCreate(SQLiteDatabase db) {
                db.execSQL("create table pairs(_id integer primary key, parent integer default 0, keyhash integer, keyname text, value text, flags integer(1), modified integer)");
                db.execSQL("create index pairs_idx on pairs(parent,keyhash)");
            }

            @Override
            protected void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

            }
        };
    }



    public class Key {
        static final int VALUE_FILE = 1;
        static final int VALUE_DATA = 2;
        static final int VALUE_EMPTY = 4;
        static final int VALUE_PERSISTENT = 8;
        private long mId = -1;
        private Key mParent;
        private String mName;
        private int mFlags = VALUE_EMPTY;
        private String mValue;
        private int mModified;
        // private Key[] mChilds = null;
        private HashMap<String,Key> mChilds = null; // new HashMap<String,Key>();

        private Key(String name) {
            if (name==null) {
                Log.e("BS:KEY", "name are null");
            }
            mName = name;
            mId = -1;
        }

        public Key(Key key, String name, int flags) {
            this(key,name);
            mFlags = flags;
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

        private Key(Key parent, String name) {
            this(name);
            mParent = parent;
        }

        private Key(Cursor cursor) {
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
            if (mChilds!=null) {
                for(Key c : mChilds.values()) c.Delete();
            }
            if ((mFlags & VALUE_FILE)==VALUE_FILE) {
                if ((mFlags & VALUE_PERSISTENT)>0) {
                    delete_file(String.format("%s.dat",mId));
                } else {
                    delete_cache(String.format("%s.dat",mId));
                }
            }
            SQLiteDatabase db = mDatabaseHelper.getWritableDatabase();
            db.delete("pairs", "_id=?", new String[]{String.valueOf(mId)});
        }


        public void Put(InputStream stream, int flags) throws IOException {
            if (mId<0) throw new IllegalStateException();
            if ((flags & VALUE_FILE) == 0) throw new IllegalStateException();
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
            if (mId<0) throw new IllegalStateException();
            Put(stream, mFlags);
        }

        public OutputStream Put() throws IOException {
            if (mId<0) throw new IllegalStateException();
            if ((mFlags & VALUE_DATA)==VALUE_DATA) {
                // mFlags = VALUE_DATA | (mFlags & VALUE_PERSISTENT);
                return new OutputStream() {
                    private ByteArrayOutputStream os = new ByteArrayOutputStream();
                    @Override
                    public void write(int oneByte) throws IOException {
                        os.write(oneByte);
                    }

                    @Override
                    public void close() {
                        mValue = new String(os.toByteArray());
                    }
                };

            }
            if ((mFlags & VALUE_PERSISTENT)>0) {
                return get_file_output_stream(String.format("%s.dat",mId));
            }
            return get_cache_output_stream(String.format("%s.dat",mId));
        }

        public void Put(String data) {
            if ((mFlags & VALUE_DATA)==0) throw new IllegalStateException();
            mValue = data;
        }

        public InputStream Get() throws FileNotFoundException {
            if ((mFlags&VALUE_DATA)==VALUE_DATA) {
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
            Key result = new Key(this,name,flags);
            result.Load();
            synchronized (this) {
                if (mChilds==null) mChilds = new HashMap<String,Key>();
                mChilds.put(name,result);
            }
            result.Put(value);
            return result;
        }

        public Key Add(String name, String data) throws IOException {
            if (mId<0) {
                mId = Commit();
            }
            Key result = new Key(this,name,VALUE_DATA);
            result.Load();
            synchronized (this) {
                if (mChilds == null) mChilds = new HashMap<String,Key>();
                mChilds.put(name,result);
            }
            result.Put(data);
            return result;
        }

        public HashMap<String,Key> Childs() {
            synchronized (this) {
                if (mChilds!=null) return mChilds;
            }
            mChilds = new HashMap<String,Key>();
            SQLiteDatabase db = mDatabaseHelper.getReadableDatabase();

            Cursor c = db.query("pairs", new String[]{"_id","keyname", "value", "flags", "modified"}, "parent=?",
                    new String[]{String.valueOf(mId)}, null, null, null);
            if (c.moveToFirst()) {
                // Key[] result = new Key[c.getCount()];
                do{
                    Key cc = new Key(this,c);
                    mChilds.put(cc.GetName(),cc);
                } while(c.moveToNext());
                return mChilds;
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
            if ((mFlags & VALUE_DATA)>0) {
                cv.put("value",mValue);
            }
            if (mId<0) {
                if (mParent!=null) {
                    if (mParent.mId<0) mParent.Commit();
                    cv.put("parent",mParent.mId);
                }
                cv.put("keyname",mName);
                cv.put("keyhash",mName.hashCode());
                mId = db.insert("pairs",null,cv);
            } else {
                db.update("pairs",cv,"_id=?",new String[] { String.valueOf(mId)});
            }
            return mId;
        }


        public Key Child(String name) {
            if (mChilds==null) {
                mChilds = Childs();
            }
            if (mChilds!=null&&mChilds.containsKey(name)) return mChilds.get(name);
            return null;
        }

        public String StringValue() {
            return mValue;
        }

        public String GetName() {
            return mName;
        }

        public boolean Contains(String name) {
            return mChilds!=null && mChilds.containsKey(name);
        }

        public boolean Exists() {
            return Load();
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
        throw new IOException(); // file exists
    }
}
