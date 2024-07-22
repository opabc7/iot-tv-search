#!/usr/bin/env python3

import unicodedata

chn_num_dict = {
    u'零' : 0,
    u'一' : 1,
    u'二' : 2,
    u'三' : 3,
    u'四' : 4,
    u'五' : 5,
    u'六' : 6,
    u'七' : 7,
    u'八' : 8,
    u'九' : 9,
}

chn_digit_dict = {
    u'十' : 10,
    u'百' : 100,
    u'千' : 1000,
    u'万' : 10000,
    u'亿' : 100000000,
    u'兆' : 1000000000000,
}

def chn_to_int(num_str):
    """ chinese number to int """

    if not num_str.isnumeric():
        return 0

    if num_str.isdigit():
        return int(num_str)

    ret = 0
    temp = 0
    num = 0
    for item in num_str:
        if item in chn_num_dict:
            num = chn_num_dict[item]

        if item in chn_digit_dict:
            if num:
                temp = num * chn_digit_dict[item]
            else:
                temp = 1 * chn_digit_dict[item]

            ret += temp
            temp = 0
            num = 0

    if num:
        ret += num

    return ret

def unicode_to_int(num_str):
    """ unicode number to int """

    if not num_str.isnumeric():
        return 0

    if num_str.isdigit():
        try:
            return int(num_str)
        except Exception as e:
            pass

        for i in num_str:
            num = ''
            num += str(int(unicodedata.numeric(i)))

        return int(num)
    else:
        return chn_to_int(num_str)
