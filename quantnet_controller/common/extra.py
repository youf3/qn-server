# -*- coding: utf-8 -*-

import importlib
import warnings


def import_extras(module_list):
    out = dict()
    for mod in module_list:
        out[mod] = None
        try:
            with warnings.catch_warnings():
                # TODO: remove when https://github.com/paramiko/paramiko/issues/2038 is fixed
                warnings.filterwarnings("ignore", "Blowfish has been deprecated", module="paramiko")
                # TODO: deprecated python 2 and 3.6 too ...
                warnings.filterwarnings("ignore", "Python .* is no longer supported", module="paramiko")
                # TODO: fix cryptography module import
                warnings.filterwarnings(
                    "ignore", "Cryptography module throws DeprecationWarning for 3.6", module="cryptography"
                )
                # import
                out[mod] = importlib.import_module(mod)
        except ImportError:
            pass
    return out
