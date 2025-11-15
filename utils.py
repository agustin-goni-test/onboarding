from datetime import datetime
from typing import Dict

class TimeMeasure:
    _instance = None
    start_times: Dict[int, datetime] = None
    finish_times: Dict[int, datetime] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TimeMeasure, cls).__new__(cls)
            cls._instance.start_times = {}
            cls._instance.finish_times = {}
        return cls._instance

    def start_measurement(self) -> int:
        '''Method to start a measurement'''

        # Use auto generated id and return it
        next_id = len(self.start_times) + 1
        self.start_times[next_id] = datetime.now()
        return next_id
    

    def _finish_measurement(self, id: int):
        '''Method to finish a measurement'''
        self.finish_times[id] = datetime.now()


    def report_time_elapsed(self, id: int, event: str) -> str:
        '''Method to report the time elapsed'''
        time_elapsed = self.calculate_time_elapsed(id)
        return (f"Tiempo transcurrido en {event}: {time_elapsed['minutes']} minutos, " 
            f"{time_elapsed['seconds']} segundos y {time_elapsed['milliseconds']} milisegundos.")


    def calculate_time_elapsed(self, id: int):
        '''Method to calculate the time elapsed'''
        
        # Finish the measurement according to id and calculate time
        self._finish_measurement(id)
        start_time = self.start_times[id]
        finish_time = self.finish_times[id]
        elapsed_time = finish_time - start_time
        
        # Separate time in components
        total_seconds = elapsed_time.total_seconds()
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)
        milliseconds = int((total_seconds - int(total_seconds)) * 1000)

        # Return results
        return {
            "minutes": minutes,
            "seconds": seconds,
            "milliseconds": milliseconds
        }




