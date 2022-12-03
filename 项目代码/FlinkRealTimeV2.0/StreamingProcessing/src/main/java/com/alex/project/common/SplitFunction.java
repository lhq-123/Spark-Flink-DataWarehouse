package com.alex.project.common;

import com.alex.project.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
//        for (String s : str.split(" ")) {
//            collect(Row.of(s, s.length()));
//        }

        List<String> list = null;
        try {
            list = KeywordUtil.splitKeyWord(str);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}
