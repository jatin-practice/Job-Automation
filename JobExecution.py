'''
Created on 27-Nov-2015

@author: jatinrout
'''
import sys
sys.path.append('/root/Job-Automation/')
from Automation.Config import config
import commands,time,re,threading
from JobSetup import JobSetup

class Jobexecution:
    
    def __init__(self):
        self.Config = config()
        self.jobsetup=JobSetup()
        self.oozie_url=self.Config.get_oozieurl('OOZIE_URL')
        self.job_dependency_dict=self.Config.get_jobdependency()
        self.job_levels=self.job_dependency_dict['Job_levels']
        self.semaphore=threading.Semaphore()
        
    def job_preverification(self):
        
        self.Config.logger.info('Verifying if job is running')
        job_list=commands.getoutput('oozie jobs -oozie %s -len 3 -filter status=RUNNING'%(self.oozie_url))
        line=job_list.split('\n')[0]
        if(re.search('criteria',line)):
            self.Config.logger.info('You Can proceed')
        else:
            self.Config.logger.info('Kill all Running Jobs')
            sys.exit()
            
    def jobstatus(self,jobname):
        
        job_details=self.Config.get_jobconfig(jobname)
        self.semaphore.acquire()
        self.path=self.jobsetup.set_jobtime(jobname)
        self.semaphore.release()
        details=job_details.split(',')
        print 'oozie URL is %s Job path is %s'%(self.oozie_url,self.path)
        coordinator_id=commands.getoutput('oozie job -oozie %s -config %s -submit'%(self.oozie_url,self.path)).split(':')[1]  
        
        self.Config.logger.info('Job %s coordinatorid is %s'%(jobname,coordinator_id.strip()))
        status=commands.getoutput('oozie job -oozie %s -len 3 -info %s'%(self.oozie_url,coordinator_id)).split('\n')
        status1=status[4].split(':')[1]
        status=status1.strip()
        
        while(re.match('RUNNING',status)):
            time.sleep(20.0)
            status=commands.getoutput('oozie job -oozie %s -len 3 -info %s'%(self.oozie_url,coordinator_id)).split('\n')
            status1=status[4].split(':')[1]
            status=status1.strip()

        if(re.match('SUCCEEDED',status)):
            
            self.Config.logger.info('Job Succeeded')
            if(self.jobsetup.post_verification('Datamodel')):
                self.Config.logger.info('Job successfully generated the output')
            else:
                self.Config.logger.error('Some issues with the job')
                raise Exception ('Data model Job could not generate proper output')
        else:
            self.Config.logger.info('Job %s failed '%(jobname))
                    
    def jobrun(self):
        self.Config.logger.info('Running the data model job')
        self.job_preverification()
        for key in self.job_levels:
            temp=self.job_dependency_dict[key]
            self.jobsetup.remove(key)
            
            for i in range(len(temp)):
                thread=threading.Thread(target=self.jobstatus,args=(temp[i],))
                thread.start()
                
            main_thread = threading.current_thread()
            
            for thread in threading.enumerate():
                if thread is main_thread:
                    continue
                self.Config.logger.debug('joining %s', thread.getName())
                thread.join()
            
                
            
                 
if __name__=='__main__':
    
    setup=JobSetup()
    setup.input_check()
    execution=Jobexecution()
    execution.jobrun()   


      
