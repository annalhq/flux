import random

from flux_producers.ws import Backoff


def test_backoff_doubles_caps_and_jitters():
    b = Backoff(rng=random.Random(1))
    delays = [b.next() for _ in range(10)]
    bases = [1, 2, 4, 8, 16, 32, 60, 60, 60, 60]
    for delay, base in zip(delays, bases, strict=True):
        assert 0.8 * base <= delay <= 1.2 * base


def test_backoff_reset():
    b = Backoff(jitter=0)
    b.next(), b.next(), b.next()
    b.reset()
    assert b.next() == 1.0
