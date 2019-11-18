class Algorithm:
    def __init__(self):
        print('Algorithm init')

    def run(self):
        raise NotImplementedError


class RandomNodeAlgorithm(Algorithm):
    def run(self):
        print('run')
