from datetime import datetime
from calendar import monthrange
from collections import namedtuple

class TimeSplitter:
    def __init__(self) -> None:
        self.actual_month_year = datetime.now()
        self.actual_month_year_str = datetime.now().strftime("%Y%m")

    def split_daily_prev_month(self) -> list:
        actual_year: int = self.actual_month_year.year
        previous_month: int = self.actual_month_year.month - 1
        range_prev_month: int = monthrange(actual_year,previous_month)[1]
        previousMonth: str = str(int(datetime.now().strftime("%Y%m")) - 1)
        split_time_list: list = []
        splitTimeDailyPrevMonth = namedtuple('SplitTimeDailyPrevMonth',
                                             ['start_date', 'end_date'])
        for item in range(1,10):
           start_date = previousMonth + '0' + str(item) + "000000"
           end_date =  previousMonth + '0' + str(item) + "235959"
           dates_tuple = splitTimeDailyPrevMonth(start_date, end_date)
           split_time_list.append(dates_tuple)

        for item in range(10,range_prev_month + 1):
           start_date = previousMonth  + str(item) + "000000"
           end_date =  previousMonth + str(item) + "235959"
           dates_tuple = splitTimeDailyPrevMonth(start_date, end_date)
           split_time_list.append(dates_tuple)
        return split_time_list






