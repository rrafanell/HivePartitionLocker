package billy.hivepartitionlocker;

import com.beust.jcommander.JCommander;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HivePartitionLocker {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(HivePartitionLocker.class);
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static Properties props;

    public static void main(String[] args) throws Exception {
        props = getProperties();

        if (args.length > 0) {
           Settings settings = new Settings();
           new JCommander(settings, args);
            executeService(settings);
        } else {
            log.error("Missing command parameter");
        }

    }

    private static void executeService(Settings settings) throws Exception {
        HiveConf hiveConf = new HiveConf();
        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);

        if(settings.lockTable != null){
            DateTime executionTime = new DateTime();
            logExecuteInfo(executionTime, settings.lockTable);
            lock(client, settings.lockTable, "spark", "stats");
        }else if(settings.unlockTable != null){
            unlock(client, 0L);
        }else {
            log.error("Unknown Command");
        }
    }

    private static Long lock(HiveMetaStoreClient client, String tableName, String username, String hostname) throws MetaException, InterruptedException {
        LockResponse response = null;
        List<LockComponent> list = new ArrayList<>();
        LockComponent component = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "default");
        component.setTablename(tableName);
        list.add(component);
        LockRequest lockRequest = new LockRequest(list, username, hostname);
        try {
            log.info("* Locking "+tableName+" table...");
            response = client.lock(lockRequest);
            log.info("   - Response Id: "+response.getLockid());
            log.info("");

            Thread.sleep(40 * 1000);

            log.info("* Unlocking "+tableName+" table...");
            client.unlock(response.getLockid());
            log.info("   - Unlocked!");
            log.info("");
        } catch (TException e) {
            e.printStackTrace();
        }
        return response.getLockid();
    }

    private static void unlock(HiveMetaStoreClient client, long id) {
        /*List<LockComponent> list = new ArrayList<>();
        LockComponent component = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "default");
        component.setTablename("impressions");
        list.add(component);
        LockRequest lockRequest = new LockRequest(list, "spark", "stats");
        try {
            client.lock(lockRequest);
        } catch (TException e) {
            e.printStackTrace();
        }
        */
    }

    private static void logExecuteInfo(DateTime executionTime, String table) throws ConfigurationException {
        PropertiesConfiguration buildProps = new PropertiesConfiguration();
        buildProps.load(HivePartitionLocker.class.getClassLoader().getResourceAsStream("version.properties"));
        String version = (String) buildProps.getProperty("package.version");
        String buildNumber = (String) buildProps.getProperty("package.build");

        log.info("");
        log.info("***************************************");
        log.info("*     Hive Partition Locker Stats     *");
        log.info("***************************************");
        log.info("");
        log.info("* Starting hive-partition-locker v" + version + " (build " + buildNumber + ") at: " + executionTime.toString(DATETIME_FORMATTER));
        log.info("");
        log.info(" * Using:");
        log.info("");
        log.info("     * Hive locker details: ");
        log.info("        - Table name: " + table);
        log.info("        - User name: " + props.getProperty("hive.user"));
        log.info("        - Host name: " + props.getProperty("hive.host"));
        log.info("");
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            InputStream stream = HivePartitionLocker.class.getClassLoader().getResourceAsStream("configuration.properties");
            props.load(stream);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }

        return props;
    }
}
