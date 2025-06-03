from asyncio import Semaphore, to_thread
from collections.abc import Callable


async def async_to_thread[T, **P](
        semaphore: Semaphore,
        func: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs
) -> T:
    async with semaphore:
        return await to_thread(func, *args, **kwargs)
