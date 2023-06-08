
from typing import Dict, List, Any, Tuple

import json
import os
import requests
import dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple

dotenv.load_dotenv()

class DataProcessor:
    
    def __init__(self, process_id: int, start_global_date: str, end_global_date: str, interval: str = "month"):
        """
        Initialize a DataProcessor object
        Args:
            process_id (int): Process ID to run
            start_global_date (str): Start date in format YYYY-MM-DD
            end_global_date (str): End date in format YYYY-MM-DD
            interval (str, optional): Interval. Defaults to "month", can be "day", "month", "quarter", "semester" or "year".
        """
        self.process_id = process_id
        self.start_global_date = start_global_date
        self.end_global_date = end_global_date
        self.interval = interval

    def _run_process_dates(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Run a process with a start and end date and return the result
        Args:
            process_id (int): Process ID to run
            start_date (str): Start date in format YYYY-MM-DD
            end_date (str): End date in format YYYY-MM-DD
        Returns:
            Dict[str, Any]: Result of the process run
        """
        url_api=os.getenv("URL_API")
        request = requests.get(
            url_api,
            params={'process_id':self.process_id, 'start_date':start_date, 'end_date':end_date},
            auth=("admin", "admin")
            )
        return request.json()


    def _get_list_of_strdates(self, start_date: str, end_date: str) -> List[Tuple[str, str]]:
        """
        Create a list of tuples with start and end date for each interval between start_date and end_date
        Args:
            start_date (str, optional): Start date. Defaults to "2000-01-01".
            end_date (str, optional): End date. Defaults to "2000-01-01".
            interval (str, optional): Interval. Defaults to "month", can be "day", "month", "quarter", "semester" or "year".
        Returns:
            List[Tuple[str, str]]: List of tuples with start and end date for each interval between start_date and end_date
        """
        if self.interval == "month":
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            list_of_strdates = []
            while start_date < end_date:
                next_date = start_date + relativedelta(months=1)
                list_of_strdates.append((start_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
                start_date = next_date
            return list_of_strdates
        elif self.interval == "day":
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            list_of_strdates = []
            while start_date < end_date:
                next_date = start_date + timedelta(days=1)
                list_of_strdates.append((start_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
                start_date = next_date
            return list_of_strdates
        elif self.interval == "week":
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            list_of_strdates = []
            while start_date < end_date:
                next_date = start_date + timedelta(days=7)
                list_of_strdates.append((start_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
                start_date = next_date
            return list_of_strdates
        elif self.interval == "quarter":
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            list_of_strdates = []
            while start_date < end_date:
                next_date = start_date + relativedelta(months=3)
                list_of_strdates.append((start_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
                start_date = next_date
            return list_of_strdates
        elif self.interval == "semester":
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            list_of_strdates = []
            while start_date < end_date:
                next_date = start_date + relativedelta(months=6)
                list_of_strdates.append((start_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
                start_date = next_date
            return list_of_strdates
        elif self.interval == "year":
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            list_of_strdates = []
            while start_date < end_date:
                next_date = start_date + relativedelta(years=1)
                list_of_strdates.append((start_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
                start_date = next_date
            return list_of_strdates
        else:
            raise ValueError("Interval not supported")

    def run_process(self):
        """
        Run a process with a start and end date and return the result for each interval between start_date and end_date
        Args:
            process_id (int): Process ID to run
            start_global_date (str): Start date in format YYYY-MM-DD
            end_global_date (str): End date in format YYYY-MM-DD
            interval (str, optional): Interval. Defaults to "month", can be "day", "month", "quarter", "semester" or "year".
        """

        dates_list = self._get_list_of_strdates(self.start_global_date, self.end_global_date)

        start_time_process = datetime.now()
        result_list = []
        for start_date, end_date in dates_list:
            start_time_task = datetime.now()
            print("-"*50)
            print(f"{datetime.now()} Start date: {start_date} - End date: {end_date}")
            result = self._run_process_dates(start_date, end_date)
            result_list.append(result)
            print(f"{datetime.now()} Result: {result}")
            end_time_task = datetime.now()
            time_elapsed_task = end_time_task - start_time_task
            print(f"{datetime.now()} Task Time elapsed: {time_elapsed_task}")

        end_time_process = datetime.now()
        time_elapsed_process = end_time_process - start_time_process
        print(f"{datetime.now()} Process Time elapsed: {time_elapsed_process}")
        return result_list

if __name__=='__main__':
    processor = DataProcessor(
        process_id=501,
        start_global_date="2020-01-01",
        end_global_date="2020-12-31",
        interval="month"
        )
    
    process_results = processor.run_process()
