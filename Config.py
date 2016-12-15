'''
Created on 27-Nov-2015

@author: jatinrout
'''
import ConfigParser,logging

class config:
    def __init__(self):
        
        self.config=ConfigParser.ConfigParser()
        self.config.read('global.cfg')
        logging.basicConfig()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger=logging.getLogger('Job-Logs')
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('spam.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        #self.configDict = {section:{option:self.config.get(section,option) for option in self.config.options(section)} for section in self.config.sections()}
    
    def get_path(self,x):
        return self.config.get('Paths',x)
    
    def get_IP(self,x):
        return self.config.get('IP',x)
    
    def get_oozieurl(self,OOZIE_URL):
        return self.config.get('OOZIE_SETTINGS',OOZIE_URL)
    
    def get_jobconfig(self,job_name):
        job_details=self.config.get('JOB_Config',job_name)
        return job_details
    
    def get_backupdir(self):
        backup_dir=self.config.get('Backup_Directory','Backup_DIR')
        return backup_dir
    
    def get_jobdependency(self):
        job_dependency_dict=dict()
        job_dependency_dict['First_Level']=self.config.get('Jobs_Dependency_Config','First_Level').split(',')
        job_dependency_dict['Second_Level']=self.config.get('Jobs_Dependency_Config','Second_Level').split(',')
        job_dependency_dict['Third_Level']=self.config.get('Jobs_Dependency_Config','Third_Level').split(',')
        job_dependency_dict['Fourth_Level']=self.config.get('Jobs_Dependency_Config','Fourth_Level').split(',')
        job_dependency_dict['Fifth_Level']=self.config.get('Jobs_Dependency_Config','Fifth_Level').split(',')
        job_dependency_dict['Sixth_Level']=self.config.get('Jobs_Dependency_Config','Sixth_Level').split(',')
        job_dependency_dict['Job_levels']=self.config.get('Jobs_Dependency_Config','Job_levels').split(',')
        return job_dependency_dict
        
        
        
    
    
        
        
    
