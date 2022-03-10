from cps3.cpsish import dask_cpsish
from cps3.main import dolist


def list(path, name_field="Key", protocol="s3", fsspec_kwargs=None, cps_backend="dask", inactivity_timeout=900):
    assert cps_backend == "dask", "only the dask backend is supported"
    l = dask_cpsish(dolist, path, name_field=name_field, protocol=protocol, fsspec_kwargs=fsspec_kwargs)
    res = []
    for err, ll in l:
        if err:
            print(l)
            raise Exception("see print (only print currently supported)")
        res.extend(ll)
    return res
