from datetime import timedelta,datetime

import numpy as np
import pandas as pd



class TimeFrameStr(dict[int, str]):
    """
    Timeframe类用于处理和转换时间频率。

    该类初始化时接受一个时间频率参数，并提供方法来设置不同时间单位的表示方式。
    它内部使用一个字典来映射不同时间单位到它们的字符串表示。
    """

    def __init__(
        self,
        freq: timedelta,
        unit: dict[int, str] = {
            365 * 24 * 60 * 60: "Y",
            7 * 24 * 60 * 60: "W",
            24 * 60 * 60: "d",
            60 * 60: "h",
            60: "m",
            1: "s",
        },
    ) -> None:
        self.update(unit)
        self.freq = freq

    @property
    def year(self):
        return self[365 * 24 * 60 * 60]

    @year.setter
    def year(self, year: str):
        self[365 * 24 * 60 * 60] = year

    @property
    def week(self):
        return self[7 * 24 * 60 * 60]

    @week.setter
    def week(self, week: str):
        self[7 * 24 * 60 * 60] = week

    @property
    def day(self):
        return self[24 * 60 * 60]

    @day.setter
    def day(self, day: str):
        self[24 * 60 * 60] = day

    @property
    def hour(self):
        return self[60 * 60]

    @hour.setter
    def hour(self, hour: str):
        self[60 * 60] = hour

    @property
    def min(self):
        return self[60]

    @min.setter
    def min(self, min: str):
        self[60] = min

    @property
    def sec(self):
        return self[1]

    @sec.setter
    def sec(self, sec: str):
        self[1] = sec

    def timedelta2s(self, seconds: timedelta):
        if isinstance(seconds, timedelta):
            return seconds.total_seconds()
        else:
            raise TypeError(seconds)

    def __str__(self):
        result = ""
        seconds = self.timedelta2s(self.freq)
        for sec, unit in self.items():
            value = int(seconds // sec)
            if value > 0:
                result += f"{int(value)}{unit}"
                seconds %= sec
        return result
