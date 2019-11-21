package com.zy.projectcase.realtimeprocessword;

import com.huaban.analysis.jieba.JiebaSegmenter;

public class JiebaGetter {
//    static JiebaSegmenter
    static JiebaGetter instance;
    JiebaSegmenter jieba;
    static {
        instance = new JiebaGetter();
    }

    private JiebaGetter(){
        jieba = new JiebaSegmenter();
    }
    public synchronized static JiebaGetter getInstance(){
        if(instance ==null){
            instance = new JiebaGetter();
        }
        return instance;
    }

    public JiebaSegmenter getJieba() {
        return jieba;
    }
}
