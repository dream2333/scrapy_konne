# 异步重试装饰器
import asyncio


def async_retry(retry_times: int = 3, delay: int = 1):
    def wrapper(func):
        async def wrapped(*args, **kwargs):
            for i in range(retry_times):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    await asyncio.sleep(delay)
            raise last_exc

        return wrapped

    return wrapper