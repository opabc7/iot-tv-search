#!/usr/bin/env python3

import faulthandler
import os
from lib.vod  import VirtualProgram

if __name__ == '__main__':
    faulthandler.enable()

    VirtualProgram(os.path.dirname(__file__)).start()
