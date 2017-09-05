'''RNL utilities:
    1) Logged - decorator class derived from type to automatically add logging
                to every new class created. Logging also includes a simple
                try/except block to wrap the called function or method
                a) log_call to decorate function or method with logging
                b) profiler to add timing information for optimisation
    2) Logged usage - decorator for logging = @Logged.log_call. To profile code
                use @Logged.profiler
    3) Dependencies - standard libraries
                a) logging (DEBUG, INFO, WARN (default = 30), ERROR, CRITICAL)
                b) functools (wrap function or method with help documentation)
                c) cProfile to profile code performance, use pstats to read '''
import logging
import functools
import cProfile

LOG_FILE = "c:\\temp\\logged.log"
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = "%(asctime)s: %(message)s"

logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL, format=LOG_FORMAT)


class Logged(type):
    """A metaclass that causes classes that it creates to log
    their function calls.
    """
    def __new__(mcs, name, bases, attrs):
        for key, value in attrs.items():
            if callable(value):
                attrs[key] = mcs.log_call(value)
        return super(Logged, mcs).__new__(mcs, name, bases, attrs)

    @staticmethod
    def log_call(func):
        """Given a function, wrap it with some logging code and
        return the wrapped function.
        """
        @functools.wraps(func)
        def inner(*args, **kwargs):
            ''' Inner '''
            logging.info('Function %s was called with arguments %r and '
                         'keywords %r.' % (func.__name__, args, kwargs))
            try:
                response = func(*args, **kwargs)
                logging.info('Function %s was successful.' % func.__name__)
                return response
            except Exception as exc:
                logging.critical('Function call to %s raised exception: %r' %
                                 (func.__name__, exc))
                raise
        return inner

    def profiler(func):
        """Given a main function, wrap it with some profiling code and
        return the wrapped function."""

        @functools.wraps(func)
        def inner(*args, **kwargs):
            '''Read dump file with pstats - pstats.Stats(func.__name__.profile")
                                            prf.sort_stats("time")
                                            prf.print_stats()'''
            logging.info('The function %s was called with arguments %r and '
                         'keywords %r.' % (func.__name__, args, kwargs))
            try:
                profile = cProfile.Profile()
                result = profile.runcall(func, *args, **kwargs)
                profile.dump_stats(func.__name__ + ".profile")
                logging.info('Function %s was successful.' % func.__name__)
                return result
            except Exception as exc:
                logging.critical('Function call to %s raised exception: %r' %
                                 (func.__name__, exc))
                raise
        return inner
