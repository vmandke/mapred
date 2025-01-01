import click

from mapred.driver.server import start_driver
from mapred.worker.server import start_worker


@click.group()
def cli():
    """Main CLI group."""
    pass


@click.command()
@click.option("--port", required=True, help="Port to start the driver server.")
def driver(port):
    """Start as a driver"""
    click.echo(f"Driver Port: {port}")
    start_driver(port=port)


@click.command()
@click.option("--driver-uri", required=True, help="Driver URI to connect.")
@click.option("--port", required=True, help="Port to start the worker server.")
def worker(driver_uri, port):
    """Start as a worker"""
    click.echo(f"Driver URI: {driver_uri}")
    start_worker(driver_uri=driver_uri, port=port)


# Add commands to the CLI group
cli.add_command(driver)
cli.add_command(worker)

if __name__ == "__main__":
    cli()
