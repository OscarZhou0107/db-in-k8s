import argparse

import analyzer

parser = argparse.ArgumentParser()
analyzer.init(parser)
analyzer.main(parser.parse_args())
