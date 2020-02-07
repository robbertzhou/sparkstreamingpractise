package com.zy.sparkvolumn;

import org.apache.spark.sql.SparkSession;

/**
 * @create 2020-02-07
 * @author zhouyu
 * @desc spark工具类
 */
public class SparkUtil {
    public static SparkSession getSession(){
        return SparkSession.builder().master("local[2]").getOrCreate();
    }
}
