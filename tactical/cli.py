import click
from colorama import Fore, Style
import asyncio
import datetime
import random
from grpc import aio

from aapis.tactical.v1 import tactical_pb2_grpc, tactical_pb2
from wiki_tools.wiki import WikiTools
from wiki_tools.defaults import WikiToolsDefaults as WTD
from task_tools.manage import TaskManager
from task_tools.defaults import TaskToolsDefaults as TTD


DEFAULT_INSECURE_PORT = 60060


@click.group()
@click.pass_context
@click.option(
    "-p",
    "--port",
    type=int,
    default=DEFAULT_INSECURE_PORT,
)
@click.option(
    "--wiki-url",
    "wiki_url",
    type=str,
    default=WTD.WIKI_URL,
    show_default=True,
    help="URL of the DokuWiki instance (https).",
)
@click.option(
    "--wiki-secrets-file",
    "wiki_secrets_file",
    type=click.Path(),
    default=WTD.WIKI_SECRETS_FILE,
    show_default=True,
    help="Path to the DokuWiki login secrets JSON file.",
)
@click.option(
    "--task-secrets-file",
    "task_secrets_file",
    type=click.Path(),
    default=TTD.TASK_SECRETS_FILE,
    show_default=True,
    help="Google Tasks client secrets file.",
)
@click.option(
    "--task-refresh-token",
    "task_refresh_token",
    type=click.Path(),
    default=TTD.TASK_REFRESH_TOKEN,
    show_default=True,
    help="Google Tasks refresh file (if it exists).",
)
def cli(
    ctx: click.Context,
    port,
    wiki_url,
    wiki_secrets_file,
    task_secrets_file,
    task_refresh_token,
):
    """Configure tactical server."""
    ctx.obj = {
        "insecure_port": port,
        "wiki": WikiTools(
            wiki_url=wiki_url, wiki_secrets_file=wiki_secrets_file, enable_logging=False
        ),
        "tasks": TaskManager(
            task_secrets_file=task_secrets_file,
            task_refresh_token=task_refresh_token,
            enable_logging=False,
        ),
    }


@cli.command()
@click.pass_context
def vocab(ctx: click.Context):
    """Update the vocab word"""

    async def cmd_impl(word, definition):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = tactical_pb2_grpc.TacticalServiceStub(channel)
            try:
                response = await stub.UpdateComponent(
                    tactical_pb2.UpdateComponentRequest(
                        component_update=tactical_pb2.ComponentUpdate(
                            vocab=tactical_pb2.Vocabulary(
                                word=word,
                                definition=definition,
                            )
                        )
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"tacticald either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(Fore.GREEN + "Added" + Style.RESET_ALL)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL)

    page_id = "eloquence"
    entries = [
        entry.strip()
        for entry in ctx.obj["wiki"].getPage(id=page_id, as_list=False).split("\n")
    ]
    current_def = None
    current_word = ""
    words = []
    for line in entries:
        if "===" in line or not line:
            continue
        if line[:2] == "> ":
            if current_def is not None and current_word:
                words.append((current_word, current_def))
                current_def = None
                current_word = ""
            current_word += line[2:] + "\n\n"
        else:
            current_def = line
    words.append((current_word, current_def))
    word = random.choice(words)
    asyncio.run(cmd_impl(word[0], word[1]))


@cli.command()
@click.pass_context
def journal(ctx: click.Context):
    """Update the journal entry"""

    async def cmd_impl(date, entry):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = tactical_pb2_grpc.TacticalServiceStub(channel)
            try:
                response = await stub.UpdateComponent(
                    tactical_pb2.UpdateComponentRequest(
                        component_update=tactical_pb2.ComponentUpdate(
                            journal=tactical_pb2.JournalEntry(
                                date=date,
                                entry=entry,
                            )
                        )
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"tacticald either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(Fore.GREEN + "Added" + Style.RESET_ALL)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL)

    page_id = f"journals:{random.choice([y for y in range(2013, datetime.datetime.now().year+1)])}"
    entries = ctx.obj["wiki"].getPage(id=page_id, as_list=True)
    random_entry = random.choice(entries)
    date = random_entry[0]
    entry = random_entry[1]
    asyncio.run(cmd_impl(date, entry))


@cli.command()
@click.pass_context
def tasks(ctx: click.Context):
    """Update the task entries"""

    async def cmd_impl(today_tasks, tomorrow_tasks):
        success = True
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = tactical_pb2_grpc.TacticalServiceStub(channel)
            try:
                response = await stub.UpdateComponent(
                    tactical_pb2.UpdateComponentRequest(
                        component_update=tactical_pb2.ComponentUpdate(
                            today_tasks=tactical_pb2.TaskList(
                                tasks=today_tasks,
                            )
                        )
                    )
                )
                success = success and response.success
                response = await stub.UpdateComponent(
                    tactical_pb2.UpdateComponentRequest(
                        component_update=tactical_pb2.ComponentUpdate(
                            tomorrow_tasks=tactical_pb2.TaskList(
                                tasks=tomorrow_tasks,
                            )
                        )
                    )
                )
                success = success and response.success
            except:
                print(
                    Fore.RED
                    + f"tacticald either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if success:
            print(Fore.GREEN + "Added" + Style.RESET_ALL)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL)

    today = datetime.date.today()
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    today_tasks = [
        task.toString(show_id=False, show_due=False, show_bar=False).replace("P0: ", "")
        for task in ctx.obj["tasks"].getTasks(date=today, start_date=today)
        if task.timing == 0 and task.days_late == 0
    ]
    tomrw_tasks = [
        task.toString(show_id=False, show_due=False, show_bar=False).replace("P0: ", "")
        for task in ctx.obj["tasks"].getTasks(date=tomorrow, start_date=tomorrow)
        if task.timing == 0 and task.days_late == 0
    ]

    asyncio.run(cmd_impl(today_tasks, tomrw_tasks))


@cli.command()
@click.pass_context
def quote(ctx: click.Context):
    """Update the quote entry"""

    async def cmd_impl(quote, author):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = tactical_pb2_grpc.TacticalServiceStub(channel)
            try:
                response = await stub.UpdateComponent(
                    tactical_pb2.UpdateComponentRequest(
                        component_update=tactical_pb2.ComponentUpdate(
                            quote=tactical_pb2.Quote(
                                author=author,
                                quote=quote,
                            )
                        )
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"tacticald either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(Fore.GREEN + "Added" + Style.RESET_ALL)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL)

    page_id = "quotes"
    entries = [
        entry.strip()
        for entry in ctx.obj["wiki"].getPage(id=page_id, as_list=False).split("\n")
    ]
    current_author = None
    current_quote = ""
    quotes = []
    for line in entries:
        if "===" in line or not line:
            continue
        if line[:2] == "> ":
            if current_author is not None and current_quote:
                quotes.append((current_quote, current_author))
                current_author = None
                current_quote = ""
            current_quote += line[2:] + "\n\n"
        else:
            current_author = line
    quotes.append((current_quote, current_author))
    quote = random.choice(quotes)
    asyncio.run(cmd_impl(quote[0], quote[1]))


@cli.command()
@click.pass_context
def wiki_url(ctx: click.Context):
    """Update the wiki URL entry"""

    async def cmd_impl(url):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = tactical_pb2_grpc.TacticalServiceStub(channel)
            try:
                response = await stub.UpdateComponent(
                    tactical_pb2.UpdateComponentRequest(
                        component_update=tactical_pb2.ComponentUpdate(
                            wiki_url=tactical_pb2.WikiUrl(
                                url=url,
                            )
                        )
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"tacticald either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(Fore.GREEN + "Added" + Style.RESET_ALL)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL)

    # TODO randomized page selection
    url = "http://ats.local/wiki/"
    asyncio.run(cmd_impl(url))


def main():
    cli()


if __name__ == "__main__":
    main()
