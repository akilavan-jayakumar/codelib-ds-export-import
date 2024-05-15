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

    public String getValue(String key, String defaultValue) throws Exception {
        String value = ZCCache.getInstance().getSegment(segment).getCacheValue(key);
        return value != null ? value : defaultValue;

    }

    public void putValue(String key, String value) throws Exception {
        ZCCache.getInstance().getSegment(segment).putCacheValue(key, value);
    }

    public void deleteValue(String key) throws Exception {
        ZCCache.getInstance().getSegment(segment).deleteCacheValue(key);
    }

    public static class ImportFileMetaService {
    }
}
