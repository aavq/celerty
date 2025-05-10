import asyncio, os, sqlite3, logging, datetime
from itertools import product
from tqdm import tqdm
import aiohttp, tenacity

ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
TOTAL = 26 ** 4                        # 456_976
DB = "names.db"
PAT = os.getenv("GITHUB_PAT")

# ---------- logging ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("one.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ---------- DB --------------------------------------------------------------
con = sqlite3.connect(
    DB,
    isolation_level=None,
    detect_types=sqlite3.PARSE_DECLTYPES
)
con.execute("PRAGMA journal_mode=WAL")
con.execute("""CREATE TABLE IF NOT EXISTS names(
    idx INTEGER PRIMARY KEY,
    name CHAR(4) UNIQUE,
    status TEXT DEFAULT 'UNKNOWN',
    checked_at TEXT                 -- ISO-строка
)""")

def seed_after(idx: int) -> None:
    for i, tup in enumerate(product(ALPHA, repeat=4), 1):
        if i > idx:
            con.execute(
                "INSERT OR IGNORE INTO names(idx,name) VALUES(?,?)",
                (i, ''.join(tup))
            )

@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, max=30)
)
async def probe(session: aiohttp.ClientSession, name: str) -> str:
    async with session.get(
        f"https://api.github.com/users/{name}",
        headers={"Authorization": f"token {PAT}", "User-Agent": "login-checker"}
    ) as r:
        return "FREE" if r.status == 404 else "TAKEN"

async def worker(queue: asyncio.Queue, session: aiohttp.ClientSession, pbar: tqdm):
    while True:
        idx, name = await queue.get()
        status = await probe(session, name)
        con.execute(
            "UPDATE names SET status=?,checked_at=? WHERE idx=?",
            (
                status,
                datetime.datetime.now(datetime.UTC).isoformat(),   # <-- aware
                idx,
            ),
        )
        pbar.set_postfix_str(name)          # показать текущее имя
        pbar.update()                       # +1 к индикатору
        queue.task_done()

async def main(parallel: int = 20):
    last = con.execute(
        "SELECT COALESCE(MAX(idx),0) FROM names WHERE status!='UNKNOWN'"
    ).fetchone()[0]
    seed_after(last)

    queue = asyncio.Queue()
    todo = con.execute(
        "SELECT idx,name FROM names WHERE status='UNKNOWN'"
    ).fetchall()

    for row in todo:
        await queue.put(row)

    log.info("Start checking %d names (resume from idx %d)", len(todo), last)

    pbar = tqdm(total=len(todo), desc="GitHub names", unit="name")

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=parallel)
    ) as sess:
        workers = [asyncio.create_task(worker(queue, sess, pbar))
                   for _ in range(parallel)]
        await queue.join()
        for w in workers: w.cancel()
    pbar.close()
    log.info("Done.")

if __name__ == "__main__":
    asyncio.run(main())
