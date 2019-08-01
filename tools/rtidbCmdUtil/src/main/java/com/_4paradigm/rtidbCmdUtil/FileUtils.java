package com._4paradigm.rtidbCmdUtil;

import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Deprecated
public class FileUtils {

    public static int getColumnDesc(List<String> lines, int index, NS.TableInfo.Builder tableBuilder) {
        Common.ColumnDesc.Builder builder = Common.ColumnDesc.newBuilder();
        int i = index;
        for (; i < lines.size(); i++) {
            if (lines.get(i) == null || lines.get(i).trim().equals("")) continue;
            String cur = lines.get(i);
            if (cur.trim().startsWith("column_key")) {
                return i;
            } else if (cur.trim().startsWith("column_desc")) {
                continue;
            } else if (cur.contains("}")) {
                tableBuilder.addColumnDescV1(builder.build());
                builder.clear();
            } else {
                String[] strs = cur.split(":");
                String str1 = strs[0].trim();
                String str2 = strs[1].trim();
                if (str1.equals("name")) {
                    str2 = str2.substring(1, str2.length() - 1).trim();
                    builder.setName(str2);
                } else if (str1.equals("type")) {
                    str2 = str2.substring(1, str2.length() - 1).trim();
                    builder.setType(str2);
                } else if (str1.equals("add_ts_idx")) {
                    builder.setAddTsIdx(Boolean.valueOf(str2));
                } else if (str1.equals("is_ts_col")) {
                    builder.setIsTsCol(Boolean.valueOf(str2));
                } else if (str1.equals("ttl")) {
                    builder.setTtl(Long.valueOf(str2));
                }
            }
        }
        return i;
    }

    public static int getColumnKey(List<String> lines, int index, NS.TableInfo.Builder tableBuilder) {
        Common.ColumnKey.Builder builder = Common.ColumnKey.newBuilder();
        int i = index;
        for (; i < lines.size(); i++) {
            if (lines.get(i) == null || lines.get(i).trim().equals("")) continue;
            String cur = lines.get(i);
            if (cur.trim().startsWith("column_key")) {
                continue;
            } else if (cur.contains("}")) {
                tableBuilder.addColumnKey(builder.build());
                builder.clear();
            } else {
                String[] strs = cur.split(":");
                String str1 = strs[0].trim();
                String str2 = strs[1].trim();
                str2 = str2.substring(1, str2.length() - 1);
                if (str1.equals("index_name")) {
                    builder.setIndexName(str2);
                } else if (str1.equals("col_name")) {
                    builder.addColName(str2);
                    while (lines.get(i + 1).split(":")[0].trim().equals("col_name") && i < lines.size()) {
                        str2 = lines.get(i + 1).split(":")[1].trim();
                        str2 = str2.substring(1, str2.length() - 1);
                        builder.addColName(str2);
                        i++;
                    }
                } else if (str1.equals("ts_name")) {
                    builder.addTsName(str2);
                    while (lines.get(i + 1).split(":")[0].trim().equals("ts_name") && i < lines.size()) {
                        str2 = lines.get(i + 1).split(":")[1].trim();
                        str2 = str2.substring(1, str2.length() - 1);
                        builder.addTsName(str2);
                        i++;
                    }
                }
            }
        }
        return i;
    }

    public static NS.TableInfo getTableInfo(String fileName) {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (lines == null) {
            return null;
        }
        for (int i = 0; i < lines.size(); i++) {
            String cur = lines.get(i);
            if (cur == null || cur.trim().equals("")) {
                continue;
            }
            if (cur.trim().startsWith("column_desc")) {
                i = getColumnDesc(lines, i, builder) - 1;
            } else if (cur.trim().startsWith("column_key")) {
                i = getColumnKey(lines, i, builder) - 1;
            } else {
                String second = cur.split(":")[1].trim();
                if (cur.trim().startsWith("name")) {
                    second = second.substring(1, second.length() - 1);
                    builder.setName(second);
                } else if (cur.trim().startsWith("ttl_type")) {
                    second = second.substring(1, second.length() - 1);
                    builder.setTtlType(second);
                } else if (cur.trim().startsWith("ttl")) {
                    builder.setTtl(Long.parseLong(second));
                } else if (cur.trim().startsWith("partition_num")) {
                    builder.setPartitionNum(Integer.parseInt(second));
                } else if (cur.trim().startsWith("replica_num")) {
                    builder.setReplicaNum(Integer.parseInt(second));
                } else if (cur.trim().startsWith("compress_type")) {
                    second = second.substring(1, second.length() - 1);
                    builder.setCompressType(NS.CompressType.valueOf(second));
                } else if (cur.trim().startsWith("key_entry_max_height")) {
                    builder.setKeyEntryMaxHeight(Integer.parseInt(second));
                }
            }
        }
        NS.TableInfo table;
        try {
            table = builder.build();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        return table;
    }



    public static void main(String[] args) {
        System.out.println("\"aaa\"");
    }
}
