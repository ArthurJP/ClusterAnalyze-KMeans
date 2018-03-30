package my.KMeans;

/**
 * Created by 张俊鹏 on 2017/6/9.
 */
    import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.regex.*;

    public class test {
        static String SendGet(String url) {
            // 定义一个字符串用来存储网页内容
            String result = "";
            // 定义一个缓冲字符输入流
            BufferedReader in = null;

            try {
                // 将string转成url对象
                URL realUrl = new URL(url);
                // 初始化一个链接到那个url的连接
                URLConnection connection = realUrl.openConnection();
                // 开始实际的连接
                connection.connect();
                // 初始化 BufferedReader输入流来读取URL的响应
                in = new BufferedReader(new InputStreamReader(connection.getInputStream(),"UTF-8"));
                // 用来临时存储抓取到的每一行的数据
                String line;
                while ((line = in.readLine()) != null) {
                    // 遍历抓取到的每一行并将其存储到result里面
                    result += line;
                }
            } catch (Exception e) {
                System.out.println("发送GET请求出现异常！" + e);
                e.printStackTrace();
            }
            // 使用finally来关闭输入流
            finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
            return result;
        }

        static ArrayList<String> RegexString(String targetStr, String patternStr) {
            // 定义一个样式模板，此中使用正则表达式，括号中是要抓的内容

            //预定义一个ArrayList来存储结果
            ArrayList<String> results=new ArrayList<String>();
            // 相当于埋好了陷阱匹配的地方就会掉下去
            Pattern pattern = Pattern.compile(patternStr);
            // 定义一个matcher用来做匹配
            Matcher matcher = pattern.matcher(targetStr);
            // 如果找到了
            boolean isFind=matcher.find();
            while (isFind) {
                results.add(matcher.group(1));
                isFind=matcher.find();
            }
            return results;
        }

        public static void main(String[] args) {

            // 定义即将访问的链接
            String url = "http://data.stats.gov.cn/easyquery.htm?cn=E0103";
            // 访问链接并获取页面内容
            String result = SendGet(url);
            System.out.println(result);
            ArrayList<String> results= RegexString(result, "question_link.+?>(.+?)</a>");
            // 打印结果
            System.out.println(results);
        }
    }

