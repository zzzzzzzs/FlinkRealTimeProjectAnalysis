package com.me.gmall.realtime.app.func;


import com.me.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/22
 * Desc:  自定义分词函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    // TODO 怎么运行的？？？
    public void eval(String str) {
        List<String> keywordList = KeywordUtil.analyze(str);
        for (String keyword : keywordList) {
            collect(Row.of(keyword));
        }
    }
}

