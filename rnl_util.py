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

LOG_LOCATION = "c:\\temp\\"
LOG_FILE = "logged.txt"
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s: %(name)s: %(message)s"

logging.basicConfig(filename=LOG_LOCATION + LOG_FILE, level=LOG_LEVEL,
                    format=LOG_FORMAT)


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
            logging.debug('Function %s was called with arguments %r and '
                          'keywords %r.' % (func.__name__, args, kwargs))
            try:
                response = func(*args, **kwargs)
                logging.debug('Function %s was successful.' % func.__name__)
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
            logging.debug('The function %s was called with arguments %r and '
                          'keywords %r.' % (func.__name__, args, kwargs))
            try:
                profile = cProfile.Profile()
                result = profile.runcall(func, *args, **kwargs)
                profile.dump_stats(func.__name__ + ".profile")
                logging.debug('Function %s was successful.' % func.__name__)
                return result
            except Exception as exc:
                logging.critical('Function call to %s raised exception: %r' %
                                 (func.__name__, exc))
                raise
        return inner

    def logger(name, filename=LOG_LOCATION + LOG_FILE, format=LOG_FORMAT):
        '''Return a new logger to the caller and output to both console and
           file using the same format'''
        logger = logging.getLogger(name)
        logger.setLevel(LOG_LEVEL)
        file_handler = logging.FileHandler(filename)
        file_handler.setFormatter(logging.Formatter(format))
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(format))
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger
