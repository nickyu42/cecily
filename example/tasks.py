from cecily import Cecily

app = Cecily()


@app.task
def mandelbrot(z, c, n=40, notifier=None):
    if abs(z) > 1000:
        return float("nan")
    elif n > 0:
        return mandelbrot(z**2 + c, c, n - 1)
    else:
        return z**2 + c
