import distributed
import s3fs
import os
import time
import shutil
import uuid
import fsspec
import datetime
from queue import Empty


def cpsish(func, *args, queue_gen, schedule, inactivity_timeout=900, **kwargs):
    queue = queue_gen()
    
    root_token = str(uuid.uuid4())
    scheduled = {root_token}
    done = set()
    all_rets = []
    
    def inner(args, kwargs, token):
        scheduled = set()

        def inner_schedule(*args, **kwargs):
            token = str(uuid.uuid4())
            schedule(inner, args, kwargs, token)
            scheduled.add(token)

        try:
            ret = func(*args, schedule=inner_schedule, **kwargs)
        except Exception as e:
            queue.put((token, scheduled, True, str(e)))
        else:
            queue.put((token, scheduled, False, ret))

    try:
        schedule(inner, args, kwargs, root_token)

        rets = []
        while True:
            try:
                done_token, new_scheduled, *ret = yy = queue.get(timeout=inactivity_timeout)
                all_rets.append(yy)
            except (distributed.TimeoutError, Empty):
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
        try:
            queue.close()
        except AttributeError:
            pass
            
    return rets, all_rets


def dask_schedule(func, *args, **kwargs):
    distributed.fire_and_forget(
        distributed.get_client().submit(func, *args, **kwargs)
    )
    
    
def threadpool_cpsish(func, *args, nthreads=10, **kwargs):
    from multiprocessing.dummy import Pool, Queue
    with Pool(nthreads) as pool:
        def schedule(func_, *args, **kwargs):
            pool.apply_async(func_, args, kwargs)

        return cpsish(func, *args, queue_gen=Queue, schedule=schedule, **kwargs)
    
    
def dask_cpsish(func, *args, **kwargs):
    return cpsish(func, *args, queue_gen=distributed.Queue, schedule=dask_schedule, **kwargs)

