import datetime

class DateTime:
    @staticmethod
    def range(first, second):
        return [first + datetime.timedelta(days=i) for i in range((second - first).days)]
