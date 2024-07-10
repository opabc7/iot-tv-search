#!/usr/bin/env python3

import faulthandler
import os
from lib.vod import Person
 
if __name__ == '__main__':
    faulthandler.enable()

    task = Person(os.path.dirname(__file__))
    task.start()
