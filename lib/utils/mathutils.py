#!/usr/bin/env python3

import unicodedata

chn_num_dict = {
    '零' : 0,
    '一' : 1,
    '二' : 2,
    '三' : 3,
    '四' : 4,
    '五' : 5,
    '六' : 6,
    '七' : 7,
    '八' : 8,
    '九' : 9,
}

chn_digit_dict = {
    '十' : 10,
    '百' : 100,
    '千' : 1000,
    '万' : 10000,
    '亿' : 100000000,
    '兆' : 1000000000000,
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
