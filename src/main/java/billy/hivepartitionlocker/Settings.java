package billy.hivepartitionlocker;

import com.beust.jcommander.Parameter;

public class Settings {

    @Parameter(names = "-lock", description = "Creates a shared lock to a Hive table", required = false)
    String lockTable = null;

    @Parameter(names = "-unlock", description = "Unlocks a Hive table", required = false)
    String unlockTable = null;
}