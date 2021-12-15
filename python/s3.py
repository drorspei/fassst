from dask.distributed import Client, LocalCluster
import distributed
import s3fs
import os
import time
import shutil
import uuid
import fsspec
import datetime
from queue import Empty


def dolist(
    path,
    *,
    schedule,
    protocol="s3",
    name_field="Key",
    batch_size=100,
    batch_size_mb=50,
    fsspec_kwargs=None
):
    fsspec_kwargs_ = dict(use_listings_cache=False)
    fsspec_kwargs_.update(fsspec_kwargs or {})
    fs = fsspec.get_filesystem_class(protocol)(**fsspec_kwargs_)
    keys = fs.listdir(path)
    for d in keys:
        if d['type'] == 'directory':
            schedule(
                d[name_field],
                protocol=protocol,
                name_field=name_field,
                batch_size=batch_size,
                batch_size_mb=batch_size_mb,
                fsspec_kwargs=fsspec_kwargs
            )
        
    return [
        {
            k: str(v) if isinstance(v, datetime.datetime) else v
            for k, v in d.items()
        }
        for d in keys
        if d['type'] != 'directory' and not d[name_field].endswith("/")
    ]
        

def to_batches(files, name_field, batch_size, batch_size_mb):
    s = 0
    batches = [[]]
    for d in files:
        if d['type'] == 'directory' or d[name_field].endswith("/"):
            continue
        batches[-1].append(d)
        s += d['size']
        if len(batches[-1]) >= batch_size or (s / 1024**2) >= batch_size_mb:
            batches.append([])
            s = 0
    return batches


def docopy(source, destination, *, schedule, protocol="s3", name_field="Key", length=64*1024**2, batch_size=100, batch_size_mb=50, fsspec_kwargs=None):
    if isinstance(source, list):
        fs = fsspec.get_filesystem_class(protocol)(**(fsspec_kwargs or {}))
        res = []
        for d in source:
            t1 = time.time()
            filepath = os.path.join(destination, d[name_field])
            err = None
            try:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)

                with open(filepath, "wb") as output:
                    with fs.open(d[name_field], "rb") as input:
                        shutil.copyfileobj(input, output, length)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                err = e
            finally:
                t2 = time.time()
            res.append((d, filepath, err, t1, t2))
        return res

    elif isinstance(source, str):
        def schedule_with_destination(*args, **kwargs):
            return schedule(*args, destination=destination, **kwargs)
        files = dolist(source, schedule=schedule_with_destination, protocol=protocol, name_field=name_field, batch_size=batch_size, batch_size_mb=batch_size_mb, fsspec_kwargs=fsspec_kwargs)
        batches = to_batches(files, name_field=name_field, batch_size=batch_size, batch_size_mb=batch_size_mb)
        for batch in batches:
            schedule(batch, destination, protocol=protocol, name_field=name_field, length=length, batch_size=batch_size, batch_size_mb=batch_size_mb, fsspec_kwargs=fsspec_kwargs)
    else:
        raise TypeError("arg should be a list of files to copy")

