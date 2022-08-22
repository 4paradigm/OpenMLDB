/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.java_sdk_test.util;

import com._4paradigm.openmldb.test_common.model.CaseFile;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 该工具类用于把fesql的case从yml输出到csv中，以便于梳理case
 * 该工具类目前单独运行，不作为自动化框架的依赖
 * @author zhaowei
 * @date 2021/1/13 11:24 AM
 */
public class CaseOutputUtil {

    public static void fromYmlToCsv(String path,String outPath){
        File outFile = new File(outPath);
        if(!outFile.exists()){
            outFile.mkdirs();
        }
        List<String> ymlPaths = new ArrayList<>();
        findAllYml(path,ymlPaths);
        Map<String, List<String>> map = ymlPaths.stream().collect(Collectors.groupingBy(p -> p.substring(0, p.lastIndexOf("/"))));
//        map.entrySet().stream().forEach(m->genCsvByPath(m.getValue(),outPath));
        map.entrySet().stream().forEach(m->genExcel(m.getKey(),m.getValue(),outPath));
    }

    public static void genCsvByPath(List<String> paths,String outPath){
        paths.forEach(p->{
            List<SQLCase> sqlCases = getCase(p);
            String fileName = p.substring(p.lastIndexOf("/"),p.lastIndexOf("."));
            String filePath = outPath+fileName+".csv";
            genCsv(sqlCases,filePath);
        });
    }

    public static void genCsv(List<SQLCase> sqlCases,String filePath){
        try (PrintWriter out = new PrintWriter(filePath)){
            sqlCases.forEach(sc->{
                System.out.println(sc.getId()+","+sc.getDesc());
                out.println(sc.getId()+","+sc.getDesc());
            });
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void genExcel(String dPath,List<String> casePath,String outPath){
        String excelName = "";
        if(dPath.contains("/")){
            excelName = dPath.substring(dPath.lastIndexOf("/")+1);
        }else{
            excelName = dPath;
        }
        Workbook workbook = new HSSFWorkbook();
        casePath.forEach(cp->{
            String sheetName = cp.substring(cp.lastIndexOf("/")+1,cp.lastIndexOf("."));
            Sheet sheet = workbook.createSheet(sheetName);
            Row row = sheet.createRow(0);
            Cell titleCell1 = row.createCell(0);
            titleCell1.setCellType(CellType.STRING);
            titleCell1.setCellValue("id");
            Cell titleCell2 = row.createCell(1);
            titleCell2.setCellType(CellType.STRING);
            titleCell2.setCellValue("desc");
            List<SQLCase> sqlCases = getCase(cp);
            for(int i=0;i<sqlCases.size();i++){
                row = sheet.createRow(i+1);
                SQLCase sc = sqlCases.get(i);
                Cell cell1 = row.createCell(0);
                cell1.setCellType(CellType.STRING);
                cell1.setCellValue(sc.getId());
                Cell cell2 = row.createCell(1);
                cell2.setCellType(CellType.STRING);
                cell2.setCellValue(sc.getDesc());
            }
        });
        try (FileOutputStream out = new FileOutputStream(outPath+"/"+excelName+".xlsx")){
            workbook.write(out);
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<SQLCase> getCase(String path){
        CaseFile dp = null;
        try {
            dp = CaseFile.parseCaseFile(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return dp.getCases();
    }

    public static void findAllYml(String path,List<String> ymlAll){
        File file = new File(path);
        File[] ymlFiles = file.listFiles(f -> f.isFile() && (f.getName().endsWith(".yaml") || f.getName().endsWith(".yml")));
        if(ymlFiles!=null) {
            List<String> ymlPaths = Arrays.stream(ymlFiles).map(f -> f.getAbsolutePath()).collect(Collectors.toList());
            ymlAll.addAll(ymlPaths);
        }
        File[] directorys = file.listFiles(f -> f.isDirectory());
        if(directorys!=null) {
            Arrays.stream(directorys).forEach(d -> findAllYml(d.getAbsolutePath(), ymlAll));
        }
    }



    public static void main(String[] args) {
        fromYmlToCsv("/Users/zhaowei/code/4paradigm/OpenMLDB/cases/function","./out");
    }
}
