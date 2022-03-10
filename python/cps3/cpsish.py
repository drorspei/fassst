import uuid
import traceback
from queue import Empty


class DontCatchExceptions(Exception):
    pass


def cpsish(func, *args, queue_gen, schedule, inactivity_timeout=300, event_gen=None, catch_exception=Exception, **kwargs):
    if catch_exception is None:
        catch_exception = DontCatchExceptions

    timeouts = [Empty]
    try:
        from distributed import TimeoutError
        timeouts.append(TimeoutError)
    except ImportError:
        pass

    queue = queue_gen()
    if event_gen is not None:
        event = event_gen()
    else:
        event = None
    
    root_token = str(uuid.uuid4())
    scheduled = {root_token}
    done = set()
    # all_rets = []
    
    def inner(args, kwargs, token):
        scheduled = set()

        def inner_schedule(*args, **kwargs):
            token = str(uuid.uuid4())
            if event is None or not event.is_set():
                schedule(inner, args, kwargs, token)
            scheduled.add(token)

        try:
            ret = func(*args, schedule=inner_schedule, **kwargs)
        except catch_exception as e:
            t = ''.join(traceback.format_exception(None, e, e.__traceback__))
            queue.put((token, scheduled, True, t))
        else:
            queue.put((token, scheduled, False, ret))

    try:
        schedule(inner, args, kwargs, root_token)

        rets = []
        while True:
            try:
                done_token, new_scheduled, *ret = yy = queue.get(
                    timeout=inactivity_timeout
                )
            except timeouts:
                break
                
            if done_token in scheduled:
                scheduled.remove(done_token)
            else:
                done.add(done_token)
                
            for new in new_scheduled:
                if new in done:
                    done.remove(new)
                else:
                    scheduled.add(new)
            
            rets.append(ret)
            
            if not scheduled:
                break
    finally:
        event.set()
        try:
            queue.close()
        except AttributeError:
            pass
            
    return rets


def callfunc(func, *args, **kwargs):
    return func(*args, **kwargs)
    
    
def singlethread_cpsish(func, *args, **kwargs):
    from multiprocessing.dummy import Queue, Event
        
        
def dask_schedule(func, *args, **kwargs):
    import distributed
    distributed.fire_and_forget(
        distributed.get_client().submit(func, *args, **kwargs)
    )
    
    
def dask_cpsish(func, *args, **kwargs):
    import distributed
    return cpsish(
        func,
        *args,
        queue_gen=distributed.Queue,
        event_gen=distributed.Event,
        schedule=dask_schedule,
        **kwargs
    )
    
    
def threadpool_cpsish(func, *args, nthreads=10, **kwargs):
    from multiprocessing.dummy import Pool, Queue, Event
    with Pool(nthreads) as pool:
        def schedule(func_, *args, **kwargs):
            pool.apply_async(func_, args, kwargs)

        return cpsish(
            func,
            *args,
            queue_gen=Queue,
            event_gen=Event,
            schedule=schedule,
            **kwargs
        )
