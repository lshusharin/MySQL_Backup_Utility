
import sys
import ConfigParser

from backUpUtilityModules.classes.MySQL_BUU import MYSQL_operator


if __name__ == "__main__":
    try:
        config_name = sys.argv[1]
    except IndexError:
        sys.stdout("You have to set a config")
        sys.exit()

    config_file = ConfigParser.ConfigParser()
    config_file.read(config_name)

    backup_folder = config_file.get('Main', 'backup_folder')
    db_user = config_file.get('Main', 'db_user')
    db_password = config_file.get('Main', 'db_password')
    s3_repo = config_file.get('Main', 's3_repo')
    incremental = config_file.getboolean('Main', 'incremental')
    timer = config_file.get('Main', 'timer')

    initia = MYSQL_operator(backup_folder, s3_repo, db_user, db_password)
    initia.mysql_full_backup()
    if incremental:
        initia.mysql_incremental_backup()
    else:
        initia.mysql_apply_back_up_log()

    initia.move_to_s3()
