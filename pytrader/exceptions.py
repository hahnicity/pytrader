class RecordsNotFoundError(Exception):
    def __init__(self, ticker, start_date, end_date):
        super(RecordsNotFoundError, self).__init__(
            "There were no records found for {} in between {} and {}".
            format(ticker, start_date, end_date)
        )
