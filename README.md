Mongo Task Queue (mtq)
========================

[![Binstar Badge](https://binstar.org/binstar/mtq/badges/build.svg?branch=master)](https://binstar.org/binstar/mtq/builds)
[![Binstar Badge](https://binstar.org/binstar/mtq/badges/version.svg)](https://binstar.org/binstar/mtq)
[![Binstar Badge](https://binstar.org/binstar/mtq/badges/installer/conda.svg)](https://conda.binstar.org/binstar)

[![PyPI][pypi.png]][pypi]


mtq is a simple Python library for queueing jobs and processing them in the background with workers. 
It is backed by [Mongodb][m] and it is designed to have a low barrier to entry. 
It should be integrated in your web stack easily.

## Getting started

First, run a Mongod server:

```bash
$ mongod [options]
```

To put jobs on queues, define a regular python function:

```python
import requests

def count_words_at_url(url):
    """Just an example function that's called async."""
    resp = requests.get(url)
    return len(resp.text.split())
```

Then, create a MTQ queue:

```python
import mtq

conn = mtq.default_connection()
q = conn.queue()
```


And enqueue the function call:

```python
from my_module import count_words_at_url
result = q.enqueue(count_words_at_url, 'http://binstar.org')
```

For a more complete example, refer to the [docs][d].  But this is the essence.


### The worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

```bash
$ mtq-worker 
[info] Starting Main Loop worker=mr_chomps.local.67313 _id=51ffb3dd7d150a06f28b1e11
Got count_words_at_url('http://binstar.org') from default
Job result = 818
```

That's it.


## Installation

Simply use the following command to install the latest released version:

    pip install mtq


[mtq.png]: https://secure.travis-ci.org/srossross/mtq.png?branch=master
[travis]: https://secure.travis-ci.org/srossross/mtq

[coveralls.png]: https://coveralls.io/repos/srossross/mtq/badge.png?branch=master
[coveralls]: https://coveralls.io/r/srossross/mtq?branch=master

[pypi.png]: https://pypip.in/v/mtq/badge.png
[pypi]: https://crate.io/packages/mtq

[m]: http://www.mongodb.org/
[d]: http://example.com

