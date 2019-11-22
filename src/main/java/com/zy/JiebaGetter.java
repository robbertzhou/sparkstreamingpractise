package com.zy;

import com.huaban.analysis.jieba.JiebaSegmenter;
import org.apache.spark.serializer.KryoSerializer;

public class JiebaGetter {
//    static JiebaSegmenter
    static JiebaGetter instance;
    JiebaSegmenter jieba;
    KryoSerializer ll;
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
