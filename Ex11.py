import io
import timeit
import time
import logging
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WordCount:

    txt = ""
    words = []

    def loadWords(self, max=None):
        with io.open("20 000 lieues sous les mers-cleaned.txt", encoding="utf-8") as f:
            self.txt = f.read()
            self.words = self.txt.split(" ")
        if max is not None:
            self.words = self.words[0:max]
        logger.info("words loaded")

    def _map(self, words) -> list:
        words_map = [
            (
                w,
                1
            )
            for w in words
        ]
        return words_map

    def _reduce(self, words_map) -> map:
        if words_map is None:
            return None
        reduced_map = {}
        for w in words_map:
            if reduced_map.get(w[0]):
                reduced_map[w[0]] += w[1]
            else:
                reduced_map[w[0]] = w[1]
        return reduced_map

    def map_reduce(self, words=None) -> map:
        return self._reduce(self._map(words))


def word_count_map_reduce(wc):
    return wc.map_reduce(wc.words)


if __name__ == "__main__":
    wc = WordCount()
    wc.loadWords(10000)
    logger.info(word_count_map_reduce(wc))
