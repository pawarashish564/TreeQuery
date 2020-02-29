package io.exp.treequery.model;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Getter;

import java.util.Iterator;
import java.util.List;

public interface JoinAble {
    public List<Key> getKeys() ;

    @Builder
    static class Key {
        @Getter
        int left;
        @Getter
        int right;
        List<KeyColumn> columnLst;

        public Iterator<KeyColumn> getColumnIterator(){
            return columnLst.iterator();
        }
    }
    @Builder
    @Getter
    static class KeyColumn{
        String leftColumn;
        String rightColumn;
    }

}
