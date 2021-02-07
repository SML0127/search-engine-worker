
# pse_argument.py
import json

class Argument():

    def __init__(self, *args):
        #smlee-error, InitArgumentError("Fail to initialize argument")
        self.args = None
        self.kwargs = None

        if len(args) == 1:
            if type(args[0]) == type({}):  # if only dict, which is kwargs
                self.args = list()
                self.kwargs = args[0]
            else:  # if only list, which is args
                self.args = args[0]
                self.kwargs = dict()

        elif len(args) == 2:
            self.args = args[0]
            self.kwargs = args[1]

    def dump_args(self):
        return json.dumps(self.args)

    def dump_kwargs(self):
        #print(self.kwargs)
        return json.dumps(self.kwargs)

    def get_args(self):
        return self.args
    
    def get_kwars(self):
        return self.kwargs
