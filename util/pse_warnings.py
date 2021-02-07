

# pse_warnings.py
# made by jinho ko @ 20190819

import warnings

# TODO specifiy warnings

class NoChildWarning():
    def __init__(self, *args, **kwargs):
        """
        this case just runs no BFSIterator of following level,  \
                but not intended. Therefore warn the users.
        """
        warnings.warn("NoChildWarning : ")

class NoElementFoundWarning():
    def __init__(self, *args, **kwargs):
        # in this case, we will just pass the case, does not extract the data
        warnings.warn("NoElementFoundWarning : ")

class TooMuchElementFoundWarning():
    def __init__(self, *args, **kwargs):
        # in this case, will use first element.
        warnings.warn("TooMuchElementsFoundWarning : ")