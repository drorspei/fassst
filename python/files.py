import os
import time
import shutil
import uuid
import fsspec
import datetime
import zipfile


def dolist(
    path,
    *,
    schedule,
    protocol="s3",
    name_field="Key",
    fsspec_kwargs=None,
    **kwargs
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
                fsspec_kwargs=fsspec_kwargs,
                **kwargs
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


def fsspec_dict_lastmodified(d, lastmodified_field=None, default=(1980, 1, 1, 0, 0, 0)):
    lastmodified = default

    for field in [lastmodified_field, "LastModified", "mtime"]:
        if field in d:
            value = d[field]
            if isinstance(value, float):
                return datetime.datetime.fromtimestamp(value).timetuple()
            elif isinstance(value, str):
                return datetime.datetime.fromisoformat(value).timetuple()
            elif isinstance(value, datetime.datetime):
                return value.timetuple()
            elif isinstance(value, tuple):
                return value
    
    return lastmodified


def zip_and_copy(dicts, protocol, name_field, destination, lastmodified_field=None, format_="%Y%m%d_%H%M%S_{uuid}.zip", now=None, nthreads=None):
    from multiprocessing.dummy import Pool
    if not len(dicts):
        return
    
    fs = fsspec.get_filesystem_class(protocol)()
    
    if destination.startswith("/"):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
    
    if now is None:
        now = datetime.datetime.now()
        
    outpath = os.path.join(destination, now.strftime(format_.format(uuid=uuid.uuid4())))
    
    def readfile(d):
        lastmodified = fsspec_dict_lastmodified(d, lastmodified_field)
        info = zipfile.ZipInfo(os.path.basename(d[name_field]), lastmodified)

        with fs.open(d[name_field]) as openf:
            filebytes = openf.read()
        
        return info, filebytes
        
    with Pool(nthreads) as pool:
        with fsspec.open(outpath, "wb") as outzipfile:
            with zipfile.ZipFile(outzipfile, "w") as z:
                for info, filebytes in pool.imap_unordered(readfile, dicts):
                    with z.open(info, "w") as file_in_zip:
                        file_in_zip.write(filebytes)

    return dicts


def zipcopy(source, destination, *, schedule, protocol="s3", name_field="Key", lastmodified_field=None, fsspec_kwargs=None, batch_size=1000, batch_size_mb=200, copy_nthreads=None, filter_func=None):
    if isinstance(source, list):
        return zip_and_copy(source, protocol, name_field, destination, lastmodified_field)
    
    def schedule_with_destination(path, *args, **kwargs):
        path0 = path
        while path0.endswith("/"):
            path0 = path0[:-1]
        new_dest = os.path.join(destination, os.path.basename(path0))
        return schedule(path, *args, destination=new_dest, **kwargs)
    
    dicts = dolist(source, schedule=schedule_with_destination, protocol=protocol, name_field=name_field, lastmodified_field=lastmodified_field, fsspec_kwargs=fsspec_kwargs, batch_size=batch_size, batch_size_mb=batch_size_mb, copy_nthreads=copy_nthreads, filter_func=filter_func)
    if filter_func is not None:
        dicts = [d for d in dicts if filter_func(d)]
    if not len(dicts):
        return []
    
    existing_keys = {
        file.path
        for zip_ in fsspec.open_files(os.path.join(destination, "*.zip"), use_listings_cache=False)
        for protocols in [zip_.fs.protocol]
        for protocol in [protocols if isinstance(protocols, str) else protocols[0]]
        for file in fsspec.open_files(f"zip://*::{protocol}://{zip_.path}")
    }
    dicts = [d for d in dicts if os.path.basename(d[name_field]) not in existing_keys]
    batches = to_batches(dicts, name_field=name_field, batch_size=batch_size, batch_size_mb=batch_size_mb)
    
    for batch in batches:
        schedule(batch, destination, protocol=protocol, name_field=name_field, lastmodified_field=lastmodified_field, batch_size=batch_size, batch_size_mb=batch_size_mb, fsspec_kwargs=fsspec_kwargs, copy_nthreads=copy_nthreads, filter_func=filter_func)
