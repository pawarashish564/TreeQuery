package io.exp.treequery.model;

public interface Command {
    String execute();
    void undo(String id);

}
