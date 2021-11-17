import logging
from multiprocessing import Process, Queue
from Ex11 import WordCount

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MapReduceProcess:
    def __init__(self, wc):
        self.wc = wc

    def _map(self, wc, queue, words):
        logger.debug(f"words length : {len(words)}")
        map = wc._map(words)
        logger.debug(f"map length: {len(map)}")
        queue.put(map)

    def map_reduce(self, words) -> map:
        nb_core = 4
        queue = Queue()
        partial_words = []
        for i in range(nb_core):
            partial_words.append(
                words[
                    i
                    * len(words)
                    // (nb_core) : min((i + 1) * len(words) // (nb_core), len(words))
                ]
            )

        processes = [
            Process(target=self._map, args=(wc, queue, partial_words[x]))
            for x in range(nb_core)
        ]

        for p in processes:
            p.start()
            logger.info(f"start - {p.name}: alive: {p.is_alive()}")

        map_results = []
        for p in processes:
            map_results.extend(queue.get())
        logger.debug(len(map_results))

        for p in processes:
            p.join()
            logger.info(f"join - {p.name}: alive: {p.is_alive()}")

        # logger.info(
        #     f"reduce - {timeit.Timer(lambda: wc._reduce(map_results)).timeit(1)}"
        # )
        return wc._reduce(map_results)


def word_count_map_reduce_process(wc):
    mpp = MapReduceProcess(wc)
    logger.debug(mpp.map_reduce(wc.words))


if __name__ == "__main__":
    wc = WordCount()
    wc.loadWords(10000)
    logger.info(word_count_map_reduce_process(wc))
