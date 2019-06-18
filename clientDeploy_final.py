#!/usr/bin/python
# -*- encoding: utf-8 -*-
import subprocess
import os
import sys
import re
try:
  import boto3
except ImportError:
  subprocess.check_output(["curl","-O","https://bootstrap.pypa.io/get-pip.py"])
  subprocess.check_output(["sudo","python","get-pip.py"])
  subprocess.check_output(["sudo","python","/usr/bin/pip", "install", "boto3"])

class app:
    def __init__(self):
      m=0
      for fathenDir,tempDir,files in os.walk("."):
        #print(files)
        for f in files:
          #print(f)
          if re.search("\.pem$",f) != None:
            self.__pemFile_=f
            print(self.__pemFile_)
            m+=1
            break

      if m==0:
        print("Please copy your pem file into this directory")
        exit(-1)

      hdir=os.environ['HOME'] 
      if not os.access(hdir+"/.aws/credentials",os.F_OK):
        awsId=raw_input("Please fulfill your AWS Access ID:")
        awsKey=raw_input("Please fulfill your AWS Access Key:")
        cfg="[default]\n"+"aws_access_key_id = "+awsId+"\n"+"aws_secret_access_key = "+awsKey
        os.mkdir(hdir+"/.aws",0755)
        f=open(hdir+"/.aws/credentials","w+")
        f.write(cfg)
        f.close()

      if not os.access(hdir+"/.aws/config",os.F_OK):
        rgn=raw_input("Please fulfill your region:")
        f=open(hdir+"/.aws/config","w+")
        f.write("[default]\n"+"region = "+rgn)
        f.close()

    def _help(self):
      print("-help, display the help information. \n"
        "-hive, install the hive client into directory.\n"
        "-sqoop, install the sqoop client into directory."
      )
        
    def _install(self,instClient):
      if not os.access("/var/aws/emr",os.F_OK):
        subprocess.check_output(["sudo","/bin/mkdir","-p","/var/aws/emr/"])
      
      # detect whether there is hadoop account
      try:
        subprocess.check_output(["sudo","useradd","-d","/home/hadoop","-s","/bin/bash","-k","/etc/skel","-m","hadoop"])
      except subprocess.CalledProcessError:
        pass

      if not os.access("/home/hadoop/.aws",os.F_OK):
        subprocess.check_output(["sudo","rsync","-av","/home/ec2-user/.aws","/home/hadoop/"])
        subprocess.check_output(["sudo","chown","hadoop:hadoop","/home/hadoop/.aws"])

      if not os.access("/mnt/tmp",os.F_OK):
        subprocess.check_output(["sudo","/bin/mkdir","-p","/mnt/tmp"])
        subprocess.check_output(["sudo","chown","hadoop:hadoop","/mnt/tmp"])

      emrClient = boto3.client("emr")
      emRsp=emrClient.list_clusters(ClusterStates=["RUNNING","WAITING"])
      #print(emRsp)

      emrnum=len(emRsp["Clusters"])
      #print(emrnum)

      for i in range(0,emrnum):
        print("["+str(i)+"]:",emRsp["Clusters"][i]["Id"],emRsp["Clusters"][i]["Name"])

      if emrnum==1:
        lstId=0
      else: 
        lstId=int(raw_input("Please select cluster from the above list:"))
      
      instRsp=emrClient.list_instances(ClusterId=emRsp["Clusters"][lstId]["Id"],InstanceGroupTypes=["MASTER"])
      master=instRsp["Instances"][0]["PublicDnsName"]
      print(master)
      
      # download emr-apps.repo
      subprocess.check_output(["sudo","scp","-i",self.__pemFile_, "hadoop@"+master+":/etc/yum.repos.d/emr-apps.repo","/etc/yum.repos.d/"])
      subprocess.check_output(["ssh","-i",self.__pemFile_,"hadoop@"+master,"sudo cp /var/aws/emr/repoPublicKey.txt /home/hadoop/"])
      subprocess.check_output(["ssh","-i",self.__pemFile_,"hadoop@"+master,"sudo chown hadoop:hadoop /home/hadoop/repoPublicKey.txt"])
      subprocess.check_output(["scp","-i",self.__pemFile_,"hadoop@"+master+":/home/hadoop/repoPublicKey.txt","./"])
      subprocess.check_output(["sudo","cp","repoPublicKey.txt","/var/aws/emr/"])

      subprocess.check_output(["sudo","yum","-y","install","emr-goodies.noarch"])
      subprocess.check_output(["sudo","yum","-y","install","hadoop.x86_64"])
      subprocess.check_output(["sudo","yum","-y","install","hadoop-client.x86_64"])
      subprocess.check_output(["sudo","yum","-y","install","hadoop-yarn.x86_64"])
      subprocess.check_output(["sudo","yum","-y","install","hadoop-hdfs.x86_64"])
      subprocess.check_output(["sudo","yum","-y","install","hadoop-hdfs-fuse.x86_64"])
      subprocess.check_output(["sudo","yum","-y","install","hadoop-lzo.x86_64"])
      subprocess.check_output(["sudo","yum","-y","install","emrfs.noarch"])
      
      subprocess.check_output(["sudo","scp","-i",self.__pemFile_, "hadoop@"+master+":/etc/hadoop/conf/*","/etc/hadoop/conf/"])

      if instClient=="hive":
        subprocess.check_output(["sudo","yum","-y","install","emr-goodies-hive.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive-hbase.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive-jdbc.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive-metastore.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","tez.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive-hcatalog.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive-webhcat.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","aws-hm-client.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","hive-server2.noarch"])
        subprocess.check_output(["sudo","scp","-i",self.__pemFile_, "hadoop@"+master+":/etc/hive/conf.dist/*","/etc/hive/conf.dist/"])
        subprocess.check_output(["sudo","scp","-i",self.__pemFile_, "hadoop@"+master+":/etc/tez/conf/*","/etc/tez/conf/"])

        subprocess.check_output(["sudo","rsync","-av","/usr/share/aws/hmclient/lib/","/usr/lib/hive/lib/"])

        subprocess.check_output(["sudo","mkdir","-p","/var/log/hive/user/hadoop"])
        subprocess.check_output(["sudo","chown","-R","hadoop:hadoop","/var/log/hive/"])
        subprocess.check_output(["sudo","chmod","777","/var/log/hive/user/hadoop"])
        print("Please launch hive client by hadoop user!")
        print("Please configure your access ID and Key, and default region!")
        print("Please grant your access ID with AWSGlueServiceRole!")
       
      if instClient=="sqoop":
        subprocess.check_output(["sudo","yum","-y","install","sqoop.noarch"])
        subprocess.check_output(["sudo","yum","-y","install","sqoop-metastore.noarch"])   
        subprocess.check_output(["sudo","yum","-y","install","pig-0.17.0-1.amzn1.noarch"])
        subprocess.check_output(["sudo","cp","-r","/usr/share/doc/pig-0.17.0/api/org/apache/pig/backend/hadoop/accumulo","/usr/lib/"])
        print("Please launch sqoop client by hadoop user!")

if __name__ == '__main__':
  inst=app()
  
  if len(sys.argv) == 1:
    inst._help()
  else:
    if sys.argv[1] == "-help":
      inst._help()

    if sys.argv[1] == "-hive":
        inst._install("hive")
  
    if sys.argv[1] == "-sqoop":
        inst._install("sqoop")
