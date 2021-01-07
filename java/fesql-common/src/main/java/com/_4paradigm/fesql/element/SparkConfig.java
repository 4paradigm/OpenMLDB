package com._4paradigm.fesql.element;

import lombok.Data;

import java.util.*;

@Data
public class SparkConfig {
    private String sql = "";
    private Map<String, String> tables = new LinkedHashMap<>();
    private List<String> sparkConfig = new ArrayList<>();
    private String outputPath = "";
    private String instanceFormat = "parquet";



}
