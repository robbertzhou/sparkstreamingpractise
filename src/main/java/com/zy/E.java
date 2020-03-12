package com.zy;

/**
 * create 2020-03-11
 * author zhouyu
 * desc 在泛型中并不存在具体的类型
 */
public class E<T> {
    T t;
    public E(T t){
        this.t = t;
    }

    public void add(T t){
        this.t = t;
    }

    public static void main(String[] args) throws Exception{
        E e = new E("A");
        System.out.println(e.t.getClass());
        e.add(33);
        System.out.println(e.t.getClass());
    }
}
