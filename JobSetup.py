'''
Created on 27-Nov-2015

@author: jatinrout
'''
import commands,sys
# This is the path need to add during running the script , this purely depends on your path where you have kept the script
sys.path.append('/home/biadmin/Job-Automation/')
from Automation.Config import config
import os,re

class JobSetup:
    
    def __init__(self):
        #Load the paths for delete before Job run
        self.Config=config()
        self.path_lsr=self.Config.get_path('path_lsr')
        self.joboutput=self.Config.get_path('job_output')
        self.regex_startime=re.compile('start=(\d+)')
        self.regex_endtime=re.compile('end=(\d+)')
        self.backupdir=self.Config.get_backupdir()
        
    def remove(self,job_level):
        
        if(job_level=='First_Level'):
            self.Config.logger.info('Removing the job output and lsr path from hdfs')
            commands.getoutput('hadoop dfs -rmr output/*')
            commands.getoutput('hadoop dfs -rmr data/*')
            
        else:
            self.Config.logger.info('Taking the backup of output directory and hadoop lsr path')
            commands.getoutput('hadoop dfs -rmr %s'%(self.backupdir.strip()))
            commands.getoutput('hadoop dfs -mkdir -p %s'%(self.backupdir.strip()))
            commands.getoutput('hadoop dfs -cp  output/* %s'%(self.backupdir.strip()))
            commands.getoutput('hadoop dfs -cp  data/DataModels %s'%(self.backupdir.strip()))
            
        
    def set_jobtime(self,job_name):
        temp_list=self.Config.get_jobconfig(job_name).split(',')
        self.job_name,self.job_path,self.job_starttime,self.job_endtime,self.job_properties=temp_list[0],temp_list[1],temp_list[2],temp_list[3].strip(),temp_list[4].strip()
        if(self.job_properties==''):
            self.path='%s/%s/job.properties'%(self.job_path,self.job_name)
        else:
            self.path='%s/%s/%s'%(self.job_path,self.job_name,self.job_properties)
        if(os.path.isfile(self.path)):
            lines=[line for line in open(self.path)]
            counter=0
            for line in lines:
                
                if self.regex_startime.match(line):
                    #newline=line.replace('start=(\d+)','start=self.job_starttime')
                    lines[counter]='start'+'='+('%s\n'%(self.job_starttime))

                elif(self.regex_endtime.match(line)):
                    #newline=line.replace('end=(\d+)','end=self.job_endtime')
                    lines[counter]='end'+'='+('%s\n'%(self.job_endtime))
                counter+=1
        else:
            self.Config.logger.error('%s job properties not present'%(self.job_name))
        FD=open(self.path,'w')
        FD.writelines(lines)
        return self.path
                
    def input_check(self):
        
        MSC_stat=commands.getoutput('hadoop dfs -lsr input/MSC_BKP')
        TAPIN_stat=commands.getoutput('hadoop dfs -lsr input/TAP_IN')
        TAPOUT_stat=commands.getoutput('hadoop dfs -lsr input/TAP_OUT')
        DBWRITER_stat=commands.getoutput('hadoop dfs -lsr input/DBWRITER_LIVE')
        TAS_stat=commands.getoutput('hadoop dfs -lsr input/TAS_BKP')
        
        if(MSC_stat.find('gz')):
            self.Config.logger.info('MSC files are avaialble')
        else:
            self.Config.logger.error('MSC file is not available')
        if(TAPIN_stat.find('TAP_IN')):
            self.Config.logger.info('TAP_IN available for processing')
        else:
            self.Config.logger.error('TAP_IN file not available')
        if(DBWRITER_stat.find('dbw')):
            self.Config.logger.info('DBwriter is avaialble for processing')  
        else:
            self.Config.logger.error('Dbwriter file is not available')
        if(TAS_stat.find('gz')):
            self.Config.logger.info('TAS is avaialble for processing')
        else:
            self.Config.logger.error('TAS file is not available') 
    
    def post_verification(self,job_name):
        
        roaming_model_stat=commands.getoutput('hadoop dfs -ls output/roaming_model')
        domestic_usage=commands.getoutput('hadoop dfs -ls output/domestic_usage')
        trip_stat=commands.getoutput('hadoop dfs -ls output/trips')
        if(job_name=='DataModels'):
            if(roaming_model_stat.find('roaming')):
                self.Config.logger.info('Roaming_model output generated')
           
            if(trip_stat.find('trip')):
                self.Config.logger.info('trip output generated')
                
        
            elif(domestic_usage.find('domestic')):
                self.Config.logger.info('Domestic usage output generated')
                return 1
            else:
                return 0
        else:
            return 1        
        

    
            
    
      
