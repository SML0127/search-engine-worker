# utils.py
# 20190820


UNKNOWN = 0
SUCCESS = 1
UNKNOWN_ERROR = -1
REDIS_ERROR = -2
GREENPLUM_ERROR = -3
PSQL_ERROR = -5
SELENIUM_ERROR = -6

ErrorDict={
    "RedisError" : REDIS_ERROR,
    "GreenplumError" : GREENPLUM_ERROR,
    "PSQLError" : PSQL_ERROR,
    "SeleniumError": SELENIUM_ERROR,
}

def getRandomUserAgent():

    # TODO generate randomly.
    return "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"


def checkRobotsTxt():
    pass
