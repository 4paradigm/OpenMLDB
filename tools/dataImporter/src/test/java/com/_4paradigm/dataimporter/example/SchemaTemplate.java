package com._4paradigm.dataimporter.example;

public class SchemaTemplate<T> {
    private T T;

    public SchemaTemplate(T t) {
        this.T = t;
    }

    public T get() {
        return this.T;
    }
}
