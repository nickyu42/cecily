### Cecily

Cecily is a minimalistic task queue for Python.

Features:
- No dependencies outside of standard library
- Simple architecture based around multiprocessing
- No broker necessary

### Get started

```python
import time
from cecily import Cecily, CecilyFuture

app = Cecily()


@app.task
def long_running_task(x: int, notifier):
    for _ in range(5):
        time.sleep(0.5)
        print(f'hello from {x}')

        notifier.put(x)

    return x


if __name__ == '__main__':
    futures = []
    for i in range(10):
        q: CecilyFuture[int] = long_running_task.apply(i)
        futures.append(q)

    for result in futures[0].collect():
        print(result)

    for f in futures:
        print(f.result())

    app.close()
```