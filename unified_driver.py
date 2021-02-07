from pse_driver import PseDriver
from cafe24_driver import Cafe24Driver
import sys
import json
import time

if __name__ == "__main__":
    start_time = time.time()
    pse_driver = PseDriver()
    pse_driver.init()
    cafe24_driver = Cafe24Driver()
    try:
        eid = pse_driver.execute()
        cafe24_driver.execute({'execution_id': eid})
    except Exception as e:
        pass
    pse_driver.close()
    end_time = time.time()
    print("unified driver: {} seconds".format(end_time - start_time))

