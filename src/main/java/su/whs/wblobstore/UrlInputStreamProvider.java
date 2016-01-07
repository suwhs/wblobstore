package su.whs.wblobstore;

import android.support.v4.BuildConfig;
import android.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import su.whs.streamcache.StreamCache;

/**
 * Created by igor n. boulliev on 01.01.16.
 */

/**
 * Provide ICacheAccessor implementation for http resources, with caching support
 */

public class UrlInputStreamProvider implements StreamCache.StreamsProvider {
    private static final String TAG="UrlStream";
    private String mUrl;
    private BlobStore mBlobStore;
    private int mConnectionTimeout = 3000;
    private int mReadTimeout = 1000;
    private String mMimeType;
    private long mLength;
    private String mReferer = null;
    private String mUserAgent = null;

    public UrlInputStreamProvider(BlobStore bs, String url) {
        mUrl = url;
        mBlobStore = bs;
    }

    public UrlInputStreamProvider(BlobStore bs, String url, String referer) {
        this(bs,url);
        mReferer = referer;
    }

    @Override
    public InputStream getSourceInputStream() {
        if (BuildConfig.DEBUG) {
            Log.d(TAG, String.format("fetch '%s'",mUrl));
        }
        URL url = null;
        try {
            url = new URL(mUrl);
        } catch (MalformedURLException e) {
            if (BuildConfig.DEBUG) {
                Log.e(TAG, String.format("mailformed url '%s'",mUrl));
            }
            return null;
        }
        URLConnection conn = null;
        try {
            conn = url.openConnection();
        } catch (IOException e) {
            if (BuildConfig.DEBUG) {
                Log.e(TAG, String.format("connection error '%s'->'%s'",mUrl,e));
            }
            return null;
        }
        conn.setConnectTimeout(mConnectionTimeout);
        conn.setReadTimeout(mReadTimeout);

        if (conn instanceof HttpURLConnection) {
            HttpURLConnection http = (HttpURLConnection)conn;


            if (mReferer!=null) {
                http.setRequestProperty("Referer",mReferer);
            }

            try {
                conn.connect();
            } catch (IOException e) {
                if (BuildConfig.DEBUG) {
                    Log.e(TAG, String.format("fetch i/o error '%s'->'%s'",mUrl,e));
                }

                return null;
            }

            mMimeType = http.getContentType();
            mLength = http.getContentLength();
            try {
                mBlobStore.attachProperty(mUrl,"Content-Length",String.valueOf(mLength));
            } catch (IOException e) {
                // could not store content-length
                if (BuildConfig.DEBUG) {
                    Log.e(TAG, String.format("attach property error '%s'->'%s'",mUrl,e));
                }

            }
            try {
                mBlobStore.attachProperty(mUrl,"Content-Type",mMimeType);
            } catch (IOException e) {
                // could not store content-type
                if (BuildConfig.DEBUG) {
                    Log.e(TAG, String.format("attach property error '%s'->'%s'",mUrl,e));
                }
            }
        } else {
            try {
                conn.connect();
            } catch (IOException e) {
                if (BuildConfig.DEBUG) {
                    Log.e(TAG, String.format("fetch i/o error '%s'->'%s'",mUrl,e));
                }

                return null;
            }
        }
        try {
            return conn.getInputStream();
        } catch (IOException e) {
            if (BuildConfig.DEBUG) {
                Log.e(TAG, String.format("fetch open InputStream error '%s'->'%s'",mUrl,e));
            }
        }
        return null;
    }

    @Override
    public InputStream getCacheInputStream() {
        InputStream is = mBlobStore.getCachedStream(mUrl);
        if (is!=null) {
            if (BuildConfig.DEBUG) {
                Log.d(TAG, String.format("get cached stream for '%s'",mUrl));
            }
            String l = mBlobStore.getAttachedProperty(mUrl,"Content-Length");
            String m = mBlobStore.getAttachedProperty(mUrl,"Content-Type");
            mMimeType = m;
            Log.d(TAG,String.format("restore properties for '%s' -> (%s;%s)",mUrl,l,m));
            try {
                mLength = Long.parseLong(l);
            } catch (NumberFormatException e) {
                mLength = -1;
            }
        } else if (BuildConfig.DEBUG) {
            Log.w(TAG,String.format("could not open cached stream for '%s'",mUrl));
        }
        return is;
    }

    @Override
    public OutputStream getCacheOutputStream() {
        try {
            OutputStream out = mBlobStore.openCacheStream(mUrl);
            Log.d(TAG,String.format("cache stream for '%s' ready",mUrl));
            return out;
        } catch (IOException e) {
            Log.e(TAG,String.format("error create cache stream for '%s'->'%s'",mUrl,e));
            return null;
        }
    }

    public String getMimeType() { return mMimeType; }
    public long getLength() { return mLength; }
}
