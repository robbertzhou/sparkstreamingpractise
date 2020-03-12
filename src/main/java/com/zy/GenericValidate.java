package com.zy;

import java.util.ArrayList;

/**
 * create 2020-03-11
 * author zhouyu
 * desc 泛型验证
 */
public class GenericValidate {
    public static void main(String[] args) throws Exception{
        ArrayList<Integer> arrayList3 = new ArrayList<Integer>();
        arrayList3.add(1);
        //NoSuchMethodException: java.util.ArrayList.add(java.lang.Integer),只能用Object.class,证明是类型擦除
        arrayList3.getClass().getMethod("add",Integer.class).invoke(arrayList3,22);
        for (Integer ii : arrayList3){
            System.out.println(ii);
        }
    }
}
