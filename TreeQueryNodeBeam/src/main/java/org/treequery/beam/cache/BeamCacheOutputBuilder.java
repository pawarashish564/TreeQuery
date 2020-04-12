package org.treequery.beam.cache;

import org.treequery.model.CacheTypeEnum;

import java.util.NoSuchElementException;

public class BeamCacheOutputBuilder {

    public static BeamCacheOutputInterface createBeamCacheOutputImpl(CacheTypeEnum cacheTypeEnum, String dataFolder){
        BeamCacheOutputInterface beamCacheOutputInterface;

        switch(cacheTypeEnum){
            case FILE:
                beamCacheOutputInterface = (dataFolder == null || dataFolder.length()==0)?
                        new FileBeamCacheOutputImpl():
                        new FileBeamCacheOutputImpl(dataFolder);
                break;
            case REDIS:
                beamCacheOutputInterface = new RedisCacheOutputImpl();
                break;
            default:
                throw new NoSuchElementException();
        }

        return beamCacheOutputInterface;
    }
}
