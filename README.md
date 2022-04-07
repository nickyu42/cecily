Minimalistic Task Queue for Python

Features:
- No dependencies outside of standard library
- Simple architecture based around multiprocessing
- No broker necessary

### Get started

```python
import math

from cecily import Cecily, CecilyFuture
 
app = Cecily()


@app.task
def mandelbrot(z, c, n=40):
    if abs(z) > 1000:
        return float("nan")
    elif n > 0:
        return mandelbrot(z ** 2 + c, c, n - 1) 
    else:
        return z ** 2 + c


if __name__ == '__main__:
    app.start()

    futures = []
    for i in range(10):
        q: CecilyFuture[complex] = mandelbrot.apply(0, 1 + 1j * i)
        futures.append(q)

    for f in futures:
        print(f.result())
```