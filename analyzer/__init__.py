import argparse

from analyzer import multi_run, single_run


def init(parser):
    subparsers = parser.add_subparsers(dest='target')

    parser_single = subparsers.add_parser('single', help=single_run.__help__)
    single_run.init(parser_single)

    parser_multi = subparsers.add_parser('multi', help=multi_run.__help__)
    multi_run.init(parser_multi)


def main(args):
    if args.target == 'single':
        single_run.main(args)
    elif args.target == 'multi':
        multi_run.main(args)
