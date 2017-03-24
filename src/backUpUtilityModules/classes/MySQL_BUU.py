import MySQLdb
import argparse
import subprocess
import os
import pwd
import datetime
import boto3
import shutil


# innobackupex --user=root --password=vfr800 /home/leonidshusharin/bu
# then
# innobackupex --user=root --password=vfr800 --apply-log /home/leonidshusharin/bu/2016-11-23_15-42-28/


class MYSQL_operator(object):
    last_time_up = 0
    backup_folder = None
    user = None
    passwd = None
    bp_start_time = None
    last_incremental_backup = None

    def __init__(self, folder, s3_repo, s3_profile_name='default', user='root', passwd='vfr800', s3_folder_name=None):

        self.backup_folder = os.path.abspath(folder)
        self.s3_repo_name = s3_repo
        self.s3_profile = s3_profile_name
        self.user = user
        self.passwd = passwd
        self.s3_folder_name = s3_folder_name
        print self.backup_folder
        pass

    def set_dedicated_backup(self, day=0, hour=0, minute=0, seconds=0):
        pass

    def mysql_full_backup(self):

        self.bp_start_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # if self.full_b_up_start_time is None:
        #     self.full_b_up_start_time = self.bp_start_time

        path = os.path.abspath(os.path.join(self.backup_folder, self.bp_start_time))

        if not os.path.exists(path):
            os.makedirs(path)  # creates with default perms 0777


        cmd_full_backup = "innobackupex " + str(path) + " --user=" + self.user + " --password=" + self.passwd +" --no-timestamp"

        # print cmd_full_backup
        # print cmd_full_backup_apply_log
        run_b_up = subprocess.Popen(cmd_full_backup, stdout=subprocess.PIPE, shell=True)
        run_b_up.wait()
        print "==================================================FULL BUP FINISHED==========================================================="

    def mysql_apply_back_up_log(self):

        path = self.backup_folder + "/" + self.bp_start_time + "/"

        cmd_full_backup_apply_log = "innobackupex --apply-log --redo-only " + str(path) + " --user=" + self.user + " --password=" + self.passwd

        run_b_up_log = subprocess.Popen(cmd_full_backup_apply_log, stdout=subprocess.PIPE, shell=True)
        run_b_up_log.wait()
        print "==================================================FULL BUP LOG FINISHED==========================================================="

    def mysql_apply_incremental_log(self):
        path_backup = self.backup_folder + "/" + self.bp_start_time + "/"
        path_incremental = self.backup_folder + "/incremental/" + self.last_incremental_backup + "/"
        cmd_apply_log = "innobackupex --apply-log --redo-only " + str(path_backup) + " --incremental-dir=" + str(path_incremental) + " --user=" + self.user + " --password=" + self.passwd + " --no-timestamp"

        run_log = subprocess.Popen(cmd_apply_log, stdout=subprocess.PIPE, shell=True)
        run_log.wait()
        print "==================================================FULL INCR LOG APPLIED==========================================================="

    def mysql_incremental_backup(self):

        self.last_incremental_backup = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path_backup = self.backup_folder + "/" + self.bp_start_time + "/"
        path_incremental = self.backup_folder + "/incremental/" + self.last_incremental_backup + "/"

        if not os.path.exists(path_incremental):
            os.makedirs(path_incremental)

        cmd_incremental_b_up = "innobackupex --incremental " + path_incremental + " --incremental-basedir=" + path_backup + " --user=" + self.user + " --password=" + self.passwd + " --no-timestamp"

        run_b_up = subprocess.Popen(cmd_incremental_b_up, stdout=subprocess.PIPE, shell=True)
        run_b_up.wait()
        print "==================================================INCREMENTAL FINISHED==========================================================="

        self.mysql_apply_back_up_log()

        self.mysql_apply_incremental_log()


    def move_to_s3(self):
        s3 = boto3.client('s3')
        print os.path.abspath(os.path.join(self.backup_folder, str(self.bp_start_time)))
        # name = str(self.last_incremental_backup) if self.last_incremental_backup else str(self.bp_start_time)
        name = str(self.last_incremental_backup) if self.last_incremental_backup else str(self.bp_start_time)
        filename = str(name + ".tar.gz")
        execute = 'tar -cf ' + os.path.abspath(os.path.join(self.backup_folder, filename)) + ' ' + os.path.abspath(os.path.join(self.backup_folder, name))
        print execute
        subprocess.Popen(execute, stdout=subprocess.PIPE, shell=True).wait()
        print "Upload to S3"
        # shutil.make_archive(name, 'gztar', os.path.abspath(os.path.join(self.backup_folder, self.bp_start_time)))
        s3.upload_file(os.path.join(self.backup_folder, filename), "repository-mysql-backuper", self.s3_folder_name+"/"+filename)
        print "Deleting the archive"
        shutil.rmtree(os.path.join(self.backup_folder, filename))
        print "Deleting backup folder"
        d_name = filename.split(".")[0]
        shutil.rmtree(os.path.join(self.backup_folder, d_name))
        # innobackupex --apply-log --redo-only /home/leonidshusharin/bu/2016-11-23_15-42-28/ --user=root --password=vfr800
        # innobackupex --user=DBUSER --password=DBUSERPASS /path/to/BACKUP-DIR/
