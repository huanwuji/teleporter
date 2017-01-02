package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.util.VersionInfo;

/**
 * Created by huanwuji
 * date 2016/12/20.
 */
public class TeleporterHBaseConfiguration extends HBaseConfiguration {
    public TeleporterHBaseConfiguration() {
    }

    private static void customCheckDefaultsVersion(Configuration conf) {
        if (!conf.getBoolean("hbase.defaults.for.version.skip", false)) {
            String defaultsVersion = conf.get("hbase.defaults.for.version");
            String thisVersion = VersionInfo.getVersion();
            if (!thisVersion.equals(defaultsVersion)) {
                throw new RuntimeException("hbase-default.xml file seems to be for an older version of HBase (" + defaultsVersion + "), this version is " + thisVersion);
            }
        }
    }

    public void checkConfiguration() {
        customCheckDefaultsVersion(this);
        HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(this);
    }
}
