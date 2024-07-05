#!/usr/bin/env python3

import faulthandler
import os
from lib.vod import Album
 
if __name__ == '__main__':
    faulthandler.enable()

    task = Album(os.path.dirname(__file__))
    task.start()

