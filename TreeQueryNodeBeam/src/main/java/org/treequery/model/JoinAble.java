package org.treequery.model;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.Iterator;
import java.util.List;

public interface JoinAble {
    public List<JoinKey> getJoinKeys() ;

    static class JoinKey {
        @Getter @NonNull
        int leftInx;
        @Getter @NonNull
        int rightInx;
        @Getter @NonNull
        String leftLabel;
        @Getter @NonNull
        String rightLabel;
        @NonNull
        List<KeyColumn> columnLst;

        JoinKey(int leftInx, int rightInx, String leftLabel, String rightLabel, List<KeyColumn> columnLst) {
            this.leftInx = leftInx;
            this.rightInx = rightInx;
            this.leftLabel = leftLabel;
            this.rightLabel = rightLabel;
            this.columnLst = columnLst;
        }

        public static JoinKeyBuilder builder() {
            return new JoinKeyBuilder();
        }

        public Iterator<KeyColumn> getColumnIterator(){
            return columnLst.iterator();
        }

        public static class JoinKeyBuilder {
            private int leftInx;
            private int rightInx;
            private String leftLabel;
            private String rightLabel;
            private List<KeyColumn> columnLst;

            JoinKeyBuilder() {
            }

            public JoinKeyBuilder leftInx(int leftInx) {
                this.leftInx = leftInx;
                return this;
            }

            public JoinKeyBuilder rightInx(int rightInx) {
                this.rightInx = rightInx;
                return this;
            }

            public JoinKeyBuilder leftLabel(String leftLabel) {
                this.leftLabel = leftLabel;
                return this;
            }

            public JoinKeyBuilder rightLabel(String rightLabel) {
                this.rightLabel = rightLabel;
                return this;
            }

            public JoinKeyBuilder columnLst(List<KeyColumn> columnLst) {
                this.columnLst = columnLst;
                return this;
            }

            public JoinKey build() {
                return new JoinKey(leftInx, rightInx, leftLabel, rightLabel, columnLst);
            }

            public String toString() {
                return "JoinAble.JoinKey.JoinKeyBuilder(leftInx=" + this.leftInx + ", rightInx=" + this.rightInx + ", leftLabel=" + this.leftLabel + ", rightLabel=" + this.rightLabel + ", columnLst=" + this.columnLst + ")";
            }
        }
    }
    @Getter
    static class KeyColumn{
        String leftColumn;
        String rightColumn;

        KeyColumn(String leftColumn, String rightColumn) {
            this.leftColumn = leftColumn;
            this.rightColumn = rightColumn;
        }

        public static KeyColumnBuilder builder() {
            return new KeyColumnBuilder();
        }

        public static class KeyColumnBuilder {
            private String leftColumn;
            private String rightColumn;

            KeyColumnBuilder() {
            }

            public KeyColumnBuilder leftColumn(String leftColumn) {
                this.leftColumn = leftColumn;
                return this;
            }

            public KeyColumnBuilder rightColumn(String rightColumn) {
                this.rightColumn = rightColumn;
                return this;
            }

            public KeyColumn build() {
                return new KeyColumn(leftColumn, rightColumn);
            }

            public String toString() {
                return "JoinAble.KeyColumn.KeyColumnBuilder(leftColumn=" + this.leftColumn + ", rightColumn=" + this.rightColumn + ")";
            }
        }
    }

}
