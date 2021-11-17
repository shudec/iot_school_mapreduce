from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader(object):

    filename=''

    def __init__ (self, filename):
        self.filename = filename

    def load (self, header=False):
        data = []
        try:
            dataFile = open(self.filename, mode='r', encoding='utf-8')
            for line in dataFile:
                linedata = line.split(';')
                linedata[1] = int(linedata[1])
                linedata[2] = float(linedata[2])
                data.append(linedata)
            if header:
                data.pop(0)    
        finally:
            dataFile.close
        return data

class MinMaxByDevice:

    def __init__(self, filename):
        self.data = DataLoader(filename=filename).load()

    def map(self, data) -> list:
        l = []
        for d in data:
            l.append((d[0], d[2]))
        return l

    def reduce(self, _list) -> map:
        _map = {}
        for l in _list:
            if _map.get(l[0]):
                _min = min(l[1], _map.get(l[0])[0])
                _max = max(l[1], _map.get(l[0])[1])
                _map[l[0]] = (_min, _max)
            else:
                _map[l[0]] = (l[1], l[1])
        return _map

    def map_reduce(self):
        return self.reduce(self.map(self.data))

class MinMaxByMonth:

    def __init__(self, filename):
        self.data = DataLoader(filename=filename).load()

    def map(self, data) -> list:
        l = []
        for d in data:
            l.append((datetime.fromtimestamp(int(d[1])).month, d[2]))
        return l

    def reduce(self, _list) -> map:
        _map = {}
        for l in _list:
            if _map.get(l[0]):
                _min = min(l[1], _map.get(l[0])[0])
                _max = max(l[1], _map.get(l[0])[1])
                _map[l[0]] = (_min, _max)
            else:
                _map[l[0]] = (l[1], l[1])
        return _map

    def map_reduce(self):
        return self.reduce(self.map(self.data))

class MeanByDevice:

    def __init__(self, filename):
        self.data = DataLoader(filename=filename).load()

    def map(self, data) -> list:
        l = []
        for d in data:
            l.append((d[0], d[2]))
        return l

    def reduce(self, _list) -> map:
        _map = {}
        for l in _list:
            if _map.get(l[0]):
                _sum = l[1] + _map.get(l[0])[0]
                _nb = _map.get(l[0])[1] + 1
                _map[l[0]] = (_sum, _nb)
            else:
                _map[l[0]] = (l[1], 1)
        for k in _map.keys():
            _map[k] = _map.get(k)[0] / _map.get(k)[1]
        return _map

    def map_reduce(self):
        return self.reduce(self.map(self.data))

if __name__ == "__main__":
    filename = 'Exterieur_12_10_2020.csv'
    minmaxbydevice = MinMaxByDevice('Exterieur_12_10_2020.csv')
    logger.info(f"minmax by device: {minmaxbydevice.map_reduce()}")
    minmaxbymonth = MinMaxByMonth('Exterieur_12_10_2020.csv')
    logger.info(f"minmax by month: {minmaxbymonth.map_reduce()}")
    meanbydevice = MeanByDevice('Exterieur_12_10_2020.csv')
    logger.info(f"mean by device: {meanbydevice.map_reduce()}")
