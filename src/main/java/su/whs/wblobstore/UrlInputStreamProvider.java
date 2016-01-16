package su.whs.wblobstore;

import android.support.v4.BuildConfig;
import android.util.Log;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;

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
    private int mConnectionTimeout = 15000;
    private int mReadTimeout = 10000;
    private String mMimeType;
    private long mLength;
    private String mReferer = null;
    private String mUserAgent = null;

    public UrlInputStreamProvider(@NotNull BlobStore bs, @NotNull String url) {
        mUrl = url;
        mBlobStore = bs;
    }

    public UrlInputStreamProvider(@NotNull BlobStore bs, @NotNull String url, @NotNull String referer) {
        this(bs,url);
        mReferer = referer;
    }

    @Override
    public InputStream getSourceInputStream() {
        HttpParams httpParameters = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParameters, getConnectionTimeout());
        HttpConnectionParams.setSoTimeout(httpParameters, getConnectionTimeout());
        HttpClient client = new DefaultHttpClient(httpParameters);

        HttpGet request = new HttpGet(mUrl);
        if (mReferer!=null)
            request.addHeader("Referer",mReferer);
        HttpResponse response;
        try {
            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            Header mime = response.getFirstHeader("Content-Type");
            if (mime!=null) {
                String[] val = mime.getValue().split(";");// [0].trim();
                if (val!=null && val.length>0) {
                    String mimeType = val[0].trim();
                    mBlobStore.attachProperty(mUrl,"Content-Type",mimeType);
                }

            }
            BufferedHttpEntity bufferedEntity = new BufferedHttpEntity(entity);
            InputStream inputStream = bufferedEntity.getContent();
            return inputStream;
        } catch (ClientProtocolException e) {
            Log.e(TAG, "error read http stream:" + e);
            e.printStackTrace();
            onReadError();
        } catch (SocketTimeoutException e) {
            onConnectionError();
        } catch (IOException e) {
            Log.e(TAG,"error read http stream:"+e);
            e.printStackTrace();
            onReadError();
        }
        return null;
    }

    @Override
    public InputStream getCacheInputStream() {
        InputStream is = mBlobStore.getCachedStream(mUrl);
        if (is!=null) {
            Log.d(TAG, String.format("get cached stream for '%s'",mUrl));
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
    protected int getConnectionTimeout() { return mConnectionTimeout; }
    protected int getReadTimeout() { return mReadTimeout; }
    protected void onConnectionError() {

    }

    protected String getUrl() { return mUrl; }
    protected void onReadError() {

    }

    protected void onCacheCreateError() {

    }

    protected void onCacheReadError() {

    }
}
