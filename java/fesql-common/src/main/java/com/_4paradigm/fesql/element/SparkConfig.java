package com._4paradigm.fesql.element;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SparkConfig {
    private String sql = "";
    private Map<String, String> tables = new HashMap<>();
    private List<String> sparkConfig = new ArrayList<>();
    private String outputPath = "";
    private String instanceFormat = "parquet";


}
