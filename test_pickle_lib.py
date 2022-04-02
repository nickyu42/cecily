import multiprocessing
import os
import pickle
import sys
from multiprocessing import freeze_support
from multiprocessing.managers import BaseManager
from types import ModuleType

foo = b''


class Manager(BaseManager):
    pass


def worker(b):
    print(f'starting manager server in {multiprocessing.current_process()} and {"sandbox" in sys.modules}')
    # setattr(sys.modules[__name__], ff.__name__, ff)

    ff = pickle.loads(b)

    print(f'loaded function {ff} {ff.__module__} {id(sys.modules)}')

    # g = globals()
    # g[ff.__name__] = ff

    m = Manager('manager.sock')
    m.register(ff.__name__, ff)

    print('serving')
    s = m.get_server()
    s.serve_forever()


p = None


def start():
    global p
    p.start()


def dec(f):
    # t = type('test', tuple(), dict())
    # globals()['test'] = t

    # globals()[f.__name__] = f
    # print(__name__)

    if 'sandbox' not in sys.modules:
        sys.modules['sandbox'] = ModuleType('sandbox')

    print(id(sys.modules))
    print(f'registering {f.__name__} from {multiprocessing.current_process()}, {os.getpid()} and {"sandbox" in sys.modules}')

    f.__module__ = 'sandbox'
    setattr(sys.modules['sandbox'], f.__name__, f)

    # setattr(sys.modules['__main__'], f.__name__, f)
    # setattr(sys.modules['__mp_main__'], f.__name__, f)

    # g = globals()
    # g[f.__name__] = f

    b = pickle.dumps(f)

    # Manager.register(f.__name__, callable=f)
    # Manager(address='manager.sock').start()

    global p
    p = multiprocessing.Process(target=worker, args=(b,), daemon=True)
    # p.start()

    return f
