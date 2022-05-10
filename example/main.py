from pathlib import Path
import time
import logging

from cecily import CecilyFuture

from example.tasks import mandelbrot, app


logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    Path('./manager.sock').unlink(missing_ok=True)

    app.start()

    time.sleep(0.1)

    futures = []
    for i in range(10):
        time.sleep(0.1)
        q: CecilyFuture[int] = mandelbrot.apply(0, 0.1 + 0.05j * i)
        futures.append(q)

    for f in futures:
        print(f.result())

    app.close()

    Path('./manager.sock').unlink(missing_ok=True)
