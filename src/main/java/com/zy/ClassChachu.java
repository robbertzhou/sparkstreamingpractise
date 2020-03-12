package com.zy;

import java.util.ArrayList;

/**
 * create 2020-03-11
 * author zhouyu
 * desc java类型擦除（泛型）
 */
public class ClassChachu {
    public static void main(String[] args) throws Exception{
        ArrayList<String> arrayList1 = new ArrayList<>();
        arrayList1.add("abc");

        ArrayList<Integer> arrayList2 = new ArrayList<>();
        arrayList2.add(33);

        System.out.println(arrayList1.getClass() == arrayList2.getClass());
    }
}
