package scwf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

/**
 * Created by root on 14-7-13.
 */
public class Conf {
    private String aaa;
    public String getA() {return aaa;}
    public Conf() {aaa = "hahaha";}
    public Conf(Conf other) {
        synchronized(other) {
            this.aaa = other.toString();
            System.out.println("111");
            System.out.println(aaa);

        }

        synchronized(Conf.class) {
            System.out.println("222");
            System.out.println(aaa);

        }
    }
}
