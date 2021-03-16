import click
import functools
from Naked.toolshed.shell import execute_js, muterun_js


@click.group()
def cli():
    pass

def common_options(f):
    options = [
        #click.option("-a", is_flag=True),
        #click.option("-b", is_flag=True),
        click.option('--database','-d',type=click.Choice(['arangodb', 'arangodb-mmfiles','mongodb','neo4j','orientdb','postgresql','postgresql_tabular'], case_sensitive=True), help="Database names froom one of the following: 'arangodb', 'arangodb-mmfiles','mongodb','neo4j','orientdb','postgresql','postgresql_tabular'"),
        click.option('--tests','-t',type=str,default='all',help='tests to run separated by comma: shortest, neighbors, neighbors2, neighbors2data, singleRead, singleWrite, aggregation, hardPath, singleWriteSync'),
        click.option('--restrict','-s',type=int,default=0,help='restrict to that many elements (0=no restriction)'),
        click.option('--neighbors','-l',type=int,default=1000,help='look at that many neighbors'),
        click.option('--neighbors2data','-ld',type=int,default=100,help='look at that many neighbors2 with profiles'),
        click.option('--address','-a',type=str,default='127.0.0.1',help='server host')

    ]
    return functools.reduce(lambda x, opt: opt(x), options, f)

@cli.command()
@common_options
def performance(**kwargs):
    databases = kwargs["database"]
    tests = kwargs["tests"]
    debug = False
    restriction = kwargs["restrict"]
    neighbors = kwargs["neighbors"]
    neighbors2data =kwargs["neighbors2data"]
    host = kwargs["address"]

    total = 0

    if len(tests) == 0 or tests == 'all':
        tests = ['warmup', 'shortest', 'neighbors', 'neighbors2', 'singleRead', 'singleWrite',
                'singleWriteSync', 'aggregation', 'hardPath', 'neighbors2data']
    else:
        tests = tests.split(',')










if __name__ == "__main__":
    cli()