import json

class Result(object):
    def __init__(self, status, title, content, status_code, scraped_date, scraped_time, content_type, url ):
        self.__status = status
        self.__title = title
        self.__content = content
        self.__status_code = status_code
        self.__scraped_date = scraped_date
        self.__scraped_time = scraped_time
        self.__content_type = content_type
        self.__url = url

    @property
    def status(self):
        return self.__status
        
    @property
    def title(self):
        return self.__title
    
    @property
    def content(self):
        return self.__content
    
    @property
    def status_code(self):
        return self.__status_code
    
    @property
    def scraped_date(self):
        return self.__scraped_date
    
    @property
    def scraped_time(self):
        return self.__scraped_time
    
    @property
    def content_type(self):
        return self.__content_type
    
    @property
    def url(self):
        return self.__url
    
    def __iter__(self):
        # return iter([self.scraped_date, self.scraped_time, self.status, self.status_code, self.url
        #              , self.content_type, self.title, self.content])
        return iter([self.url, self.content])
    
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

