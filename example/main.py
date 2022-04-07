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
        q: CecilyFuture[int] = mandelbrot.apply(0, 1 + 1j * i)
        futures.append(q)

    for f in futures:
        print(f.result())
