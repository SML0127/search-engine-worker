from functools import wraps
import errno
import os
import signal
import time

class PSETimeoutError(Exception):
    pass

def pse_timeout(seconds, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise PSETimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.setitimer(signal.ITIMER_REAL,seconds) #used timer instead of alarm
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wraps(func)(wrapper)
    return decorator


#@timeout(2)
#def testing():
#    count = 0
#    while count < 3:
#        print('helloworld', count)
#        count += 1
#        time.sleep(1)
#    return count
#
#count = 0
#while count < 5:
#    try:
#        testing()
#        break
#    except Exception as e:
#        print('timeout occur', str(e))
#        count += 1
