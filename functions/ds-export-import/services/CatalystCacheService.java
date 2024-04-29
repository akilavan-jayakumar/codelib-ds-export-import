package services;

import com.zc.component.cache.ZCCache;

public class CatalystCacheService {
    private final String segment;

    public CatalystCacheService(String segment) {
        this.segment = segment;
    }

    public String getValue(String key) throws Exception {
        return ZCCache.getInstance().getSegment(segment).getCacheValue(key);
    }

    public void putValue(String key, String value) throws Exception {
        ZCCache.getInstance().getSegment(segment).putCacheValue(key, value);
    }

}
