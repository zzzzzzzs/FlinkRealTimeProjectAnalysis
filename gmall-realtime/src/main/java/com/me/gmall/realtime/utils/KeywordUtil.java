package com.me.gmall.realtime.utils;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/22
 * Desc: 分词工具类
 */
public class KeywordUtil {
    public static List<String> analyze(String text) {
        List keywordList = new ArrayList();
        StringReader sr = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
        Lexeme lexeme = null;
        try {
            while ((lexeme=ikSegmenter.next())!=null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywordList;
    }

    public static void main(String[] args) {
        List<String> list = analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
        System.out.println(list);
    }
}
