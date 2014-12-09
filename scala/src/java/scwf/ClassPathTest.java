package scwf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

/**
 * Created by w00228970 on 2014/12/7.
 */
public class ClassPathTest {
    public static void main(String[] args) throws IOException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Enumeration<URL> urls = loader.getResources("yarn-site.xml");

        Configuration conf = new Configuration();
        while(urls.hasMoreElements()) {
            URL url = urls.nextElement();
            System.out.println(url);
            conf.addResource(new Path("F:\\share\\hadoop-2.4.0\\etc\\hadoop\\yarn-site.xml"));
            conf.addResource(url);
        }
        System.out.println(conf.get("yarn.resourcemanager.address", "no value"));
    }
}
