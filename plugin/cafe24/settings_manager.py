from yaml import *
import os




class SettingsManager():
       
    def __init__(self):
        self.settings = None

    def setting(self, file_name):
        if os.path.exists(file_name):
            self.settings = load(open(file_name), Loader=Loader)
        #smlee-error, RedisDatabaseError("Cannot connect Redis Database, e")
        #self.redis_conn = Redis(host=settings.get('Redis_host'))

    def get_settings(self):
        return self.settings

