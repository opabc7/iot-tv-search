#!/usr/bin/env python3

import faulthandler
import os
from lib.vod import AlbumHeat
 
if __name__ == '__main__':
    faulthandler.enable()

    task = AlbumHeat(os.path.dirname(__file__))
    task.start()
