#Entry Point

from backUpUtilityModules.classes.MySQL_BUU import MYSQL_operator


if __name__ == "__main__":
    initia = MYSQL_operator("/home/leonidshusharin/bu", "root", "vfr800")
    initia.mysql_full_backup()
    # initia.mysql_incremental_backup()
    initia.move_to_s3()