import logging


class AccumulatingLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_records = []

    def emit(self, record):
        log_entry = self.format(record)
        self.log_records.append(log_entry)

    def get_accumulated_logs(self):
        #return self.log_records
        return '\n'.join(self.log_records)

