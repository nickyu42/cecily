import os
import time
from multiprocessing import freeze_support
from pathlib import Path

from test_pickle_lib import Manager, start

import test_pickle_tasks




if __name__ == '__main__':


    # freeze_support()

    start()

    time.sleep(2)
    print('connecting to manage')

    man = Manager('manager.sock')
    man.register('fn')
    man.connect()
    fn = getattr(man, 'fn')

    print(fn())
    os.remove('./manager.sock')



