import functools
import time


# https://stackoverflow.com/a/1094933
def sizeof_fmt(num, suffix="", with_unit=True):
    for unit in ["M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return (int(num), f"{unit}{suffix}") if with_unit else int(num)
        num /= 1024.0
    return (int(num), f"Y{suffix}") if with_unit else int(num)


def cache_for_n_seconds(seconds=1800):
    def decorator_cache_for_n_seconds(func):
        @functools.wraps(func)
        def wrapper_cache_for_n_seconds(*args, **kwargs):
            if not hasattr(wrapper_cache_for_n_seconds, "last_call_value") or len(wrapper_cache_for_n_seconds.last_call_value) == 0 or time.time() - wrapper_cache_for_n_seconds.last_call_time >= seconds:
                wrapper_cache_for_n_seconds.last_call_time = time.time()
                wrapper_cache_for_n_seconds.last_call_value = func(*args, **kwargs)
            return wrapper_cache_for_n_seconds.last_call_value
        return wrapper_cache_for_n_seconds
    return decorator_cache_for_n_seconds
