package teleporter.integration.compress;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by joker on 15/10/16
 */
public class Md5Util {

    /***
     * MD5加码 生成32位md5码
     */
    public static String string2MD5(String s) {
        if (s == null) {
            return null;
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes());
            byte[] md5hash = md.digest();
            StringBuilder builder = new StringBuilder();
            for (byte b : md5hash) {
                builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Cannot find digest algorithm");
            System.exit(1);
        }
        return "";
    }
}

