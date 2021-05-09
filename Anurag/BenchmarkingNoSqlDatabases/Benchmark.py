import click
import functools
import js2py
import neo4jBenchmarkTest,postgresBenchmarkTest,mongodbBenchmarkTest

@click.group()
def cli():
    pass

def common_options(f):
    options = [
        click.option('--database', '-d', type=click.Choice(
            ['mongodb', 'neo4j', 'postgres'],
            case_sensitive=True),
                     help="chose Database names from one of the following: 'mongodb', 'neo4j', 'postgres'"),

        click.option('--query', '-q', type=click.Choice(
            ['singleRead', 'singleWrite', 'aggregate','neighbors', 'neighbors2', 'neighbors2data','shortestPath'],
            case_sensitive=True),
                     help="chose one query from one of the following: 'singleRead', 'singleWrite', 'aggregate','neighbors', 'neighbors2', 'neighbors2data','shortestPath'")
    ]
    return functools.reduce(lambda x, opt: opt(x), options, f)

@cli.command()
@common_options
def performance(**kwargs):

    database = kwargs["database"]
    query = kwargs["query"]

    if database == "neo4j":
        if query == "singleRead":
            neo4jBenchmarkTest.singleRead()
        elif query == "singleWrite":
            neo4jBenchmarkTest.singleWrite()
        elif query == "aggregate":
            neo4jBenchmarkTest.aggregate()
        elif query == "neighbors":
            neo4jBenchmarkTest.neighbors()
        elif query == "neighbors2":
            neo4jBenchmarkTest.neighbors2()
        elif query == "neighbors2data":
            neo4jBenchmarkTest.neighbors2data()
        elif query == "shortestPath":
            neo4jBenchmarkTest.shortestPath()
    elif database == "postgres":
        if query == "singleRead":
            postgresBenchmarkTest.singleRead()
        elif query == "singleWrite":
            postgresBenchmarkTest.singleWrite()
        elif query == "aggregate":
            postgresBenchmarkTest.aggregate()
        elif query == "neighbors":
            postgresBenchmarkTest.neighbors()
        elif query == "neighbors2":
            postgresBenchmarkTest.neighbors2()
        elif query == "neighbors2data":
            postgresBenchmarkTest.neighbors2data()
    elif database == "mongodb":
        if query == "singleRead":
            mongodbBenchmarkTest.singleRead()
        elif query == "singleWrite":
            mongodbBenchmarkTest.singleWrite()
        elif query == "aggregate":
            mongodbBenchmarkTest.aggregate()
        elif query == "neighbors":
            mongodbBenchmarkTest.neighbors()
        elif query == "neighbors2":
            mongodbBenchmarkTest.neighbors2()
        elif query == "neighbors2data":
            mongodbBenchmarkTest.neighbors2data()



if __name__ == "__main__":
    cli()