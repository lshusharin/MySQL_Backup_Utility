import sys
import ConfigParser
import datetime
from backUpUtilityModules.classes.MySQL_BUU import MYSQL_operator


if __name__ == "__main__":
    try:
        config_name = sys.argv[1]
    except IndexError:
        sys.stdout("You have to set a config")
        sys.exit()
    last_time_up = datetime.datetime.now()
    first_start = True
    while True:

        config_file = ConfigParser.ConfigParser()
        config_file.read(config_name)

        backup_folder = config_file.get('Main', 'backup_folder')
        db_user = config_file.get('Main', 'db_user')
        db_password = config_file.get('Main', 'db_password')
        s3_repo = config_file.get('Main', 's3_repo')
        incremental = config_file.getboolean('Main', 'incremental')
        timer = config_file.get('Main', 'timer')

        if timer != "None" and incremental:
            timer = datetime.timedelta(seconds=int(timer))

            if first_start:
                initia = MYSQL_operator(backup_folder, s3_repo, user=db_user, passwd=db_password)
                initia.mysql_full_backup()
                initia.mysql_incremental_backup()
                initia.move_to_s3()

                last_time_up = datetime.datetime.now()
            elif datetime.datetime.now() - last_time_up > timer:
                initia = MYSQL_operator(backup_folder, s3_repo, user=db_user, passwd=db_password)
                initia.mysql_full_backup()
                initia.mysql_incremental_backup()
                initia.move_to_s3()

                last_time_up = datetime.datetime.now()
            else:
                continue
        else:
            initia = MYSQL_operator(backup_folder, s3_repo, user=db_user, passwd=db_password)
            initia.mysql_full_backup()

            initia.mysql_apply_back_up_log()

            initia.move_to_s3()
            break