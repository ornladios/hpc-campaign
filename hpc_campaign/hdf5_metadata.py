try:
    import h5py

    __HAVE_H5PY__ = True
except Exception:
    __HAVE_H5PY__ = False

import numpy as np
import sys
from os import stat
from os.path import exists


def _report(operation, key, obj, size):
    type_str = type(obj).__name__.split(".")[-1].lower()
    print(f"{operation} {type_str} {key}, size = {size}")


def h5py_compatible_attributes(in_object):
    """Are all attributes of an object readable in h5py?"""
    try:
        # Force obtaining the attributes so that error may appear
        [0 for at in in_object.attrs.items()]
        return True
    except Exception:
        return False


def copy_attributes(in_object, out_object):
    """Copy attributes between 2 HDF5 objects."""
    for key, value in in_object.attrs.items():
        out_object.attrs[key] = value


def walk(in_object, out_object, log=False):
    """Recursively copy the tree but don't write actual array content

    If attributes cannot be transferred, a copy is created.
    Otherwise, dataset are compressed.
    """
    keys = list(in_object.keys())
    for key in keys:
        in_obj = in_object[key]
        if not isinstance(in_obj, h5py.Datatype) and h5py_compatible_attributes(in_obj):
            if isinstance(in_obj, h5py.Group):
                out_obj = out_object.create_group(key)
                walk(in_obj, out_obj, log)
                if log:
                    _report("Copied", key, in_obj, 0)
            elif isinstance(in_obj, h5py.Dataset):
                if in_obj.size <= 128:
                    out_obj = out_object.create_dataset(key, data=in_obj)
                    if log:
                        _report("Copied small object", key, in_obj, in_obj.size)
                else:
                    out_obj = out_object.create_dataset_like(key, other=in_obj)
                    if log:
                        _report("Recreated without copy", key, in_obj, in_obj.size)
            else:
                raise Exception("Invalid object type %s" % type(in_obj))
            copy_attributes(in_obj, out_obj)
        else:
            # We copy datatypes and objects with non-understandable attributes
            # identically.
            if log:
                _report("Copied object with non-understandable attributes", key, in_obj, 0)
            in_object.copy(key, out_object)


def copy_hdf5_file_without_data(infilename: str, outfilename: str, log: bool = False):
    """Copy an HDF5 file metadata without writing the array data

    :param infilename: Input HDF5 path
    :param outfilename: Output HDF5 path
    :param log: Whether to print results of operations'
    :returns: A tuple(original_size, new_size)
    """
    if __HAVE_H5PY__:
        with h5py.File(infilename, "r") as in_file, h5py.File(outfilename, "w") as out_file:
            walk(in_file, out_file, log=log)
        return stat(infilename).st_size, stat(outfilename).st_size
    else:
        return 0, 0


def IsHDF5Dataset(dataset):
    it_is = False
    if __HAVE_H5PY__:
        try:
            with h5py.File(dataset, "r") as _:
                it_is = True
        except Exception:
            it_is = False
    return it_is


def main(args=sys.argv[1:3], prog=None):
    infilename, outfilename = args
    insize, outsize = copy_hdf5_file_without_data(infilename, outfilename, log=True)
    print(f"{infilename} size = {insize}")
    print(f"{outfilename} size = {outsize}")


if __name__ == "__main__":
    main()

