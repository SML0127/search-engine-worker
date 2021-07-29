

# pse_errors.py
# made by jinho ko @ 20190819


# TODO specify errors.



class OperatorError(Exception):
    def __init__(self, error, op_id, xpath = ''):
        self.error = error
        self.op_id = op_id
        self.xpath = xpath
    def __str__(self):
        return str("OperatorError") + "\n" + str(self.error)

class RedisError(Exception):
    def __init__(self, error):
        self.error = error
        pass
    def __str__(self):
        return str("RedisError") +"\n" + str(self.error)

class GreenplumError(Exception):
    def __init__(self, error):
        self.error = error
        pass
    def __str__(self):
        return str("GreenplumError") +"\n" + str(self.error)

class PSQLError(Exception):
    def __init__(self, error):
        self.error = error
        pass
    def __str__(self):
        return str("PSQLError") +"\n" + str(self.error)

class SeleniumManagerError(Exception):
    def __init__(self, error):
        self.error = error
        pass
    def __str__(self):
        return str("SeleniumError") +"\n" + str(self.error)


class UserDefinedError(Exception):
    def __init__(self, msg = 'No user message'):
        self.msg = msg
        pass
    def __str__(self):
        return str("UserDefinedError") +"\nUser Message: " + str(self.msg) 




"""
Job-level Errors
"""
class TaskRunFailureError(Exception):

    # def : raise when the task has failed.
    # rollback procedure will be proceeded after this error.
    def __init__(self):
        pass
    def __str__(self):
        return str("TaskRunFailureError") +"\n"

class TaskRollbackFailureError(Exception):

    # def : raised when rollback has failed after task failure.
    # which can cause consistency problem within the system.

    def __init__(self):
        pass
    def __str__(self):
        return str("TaskRollbackFailureError") +"\n"

"""
Task-level Errors
"""
class DatabaseError(Exception):

    # def : all errors related to Selenium.
    # passes detailed error messages.
    def __init__(self):
        pass
    def __str__(self):
        return str("DatabaseError") +"\n"


class SeleniumError(Exception):

    # def : all errors related to Selenium.
    # passes detailed error messages.
    def __init__(self, error):
        self.error = error
        pass
    def __str__(self):
        return str("SeleniumError") +"\n"

class NoElementFoundError(Exception):

    # def : error occured when it cannot proceed due to lack of element.
    def __init__(self, xpath):
        self.xpath = xpath
        pass
    def __str__(self):
        return str("NoElementFoundError of xpath:") + str(self.xpath) + "\n"

class TooMuchElementFoundError(Exception):

    # def : multiple elements are found, which mismatches the logic.
    def __init__(self, xpath):
        self.xpath = xpath
        pass
    def __str__(self):
        return str("TooMuchElementFoundError of xpath ") + str(self.xpath) + "\n"

class ParsedDataWrongTypeError(Exception):

    # def : parsed data has wrong type compared to expected type.
    def __init__(self):
        pass
    def __str__(self):
        return str("ParsedDataWrongTypeError") +"\n"


class NoneDetailPageError(Exception):
    def __init__(self, xpath = ''):
        self.xpath = xpath
        pass
    def __str__(self):
        return str("NoneDetailPageError") + "\n" 


class BtnNumError(Exception):
    def __init__(self):
        pass
    def __str__(self):
        return str("SummarPageBtnNumError") + "\n" 


class CheckXpathError(Exception):
    def __init__(self):
        pass
    def __str__(self):
        return str("CheckXpathError") + "\n" 



        
