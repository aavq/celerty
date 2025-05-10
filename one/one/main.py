import asyncio, sqlite3, os, atexit, datetime
from itertools import product
import aiohttp, tenacity

ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
DB     = "names.db"
PAT    = os.getenv("GITHUB_PAT")

# ---------- DB init ----------------------------------------------------------
con = sqlite3.connect(DB, isolation_level=None)
con.execute("PRAGMA journal_mode=WAL")                       # concurrency
con.execute("""CREATE TABLE IF NOT EXISTS names(
                idx INTEGER PRIMARY KEY,
                name CHAR(4) UNIQUE,
                status TEXT DEFAULT 'UNKNOWN',
                checked_at TIMESTAMP)""")

# ---------- utilities --------------------------------------------------------
def seed_after(idx):
    for i, t in enumerate(product(ALPHA, repeat=4), 1):
        if i > idx:
            con.execute("INSERT OR IGNORE INTO names(idx,name) VALUES(?,?)",
                        (i, ''.join(t)))

@tenacity.retry(stop=tenacity.stop_after_attempt(5),
               wait=tenacity.wait_exponential(multiplier=1, max=30))
async def probe(session, name):
    async with session.get(f"https://api.github.com/users/{name}",
                           headers={"Authorization": f"token {PAT}",
                                    "User-Agent": "login-checker"}) as r:
        return "FREE" if r.status == 404 else "TAKEN"

async def worker(q, session):
    while True:
        idx, name = await q.get()
        status = await probe(session, name)
        con.execute("UPDATE names SET status=?,checked_at=? WHERE idx=?",
                    (status, datetime.datetime.utcnow(), idx))
        q.task_done()

async def main(parallel=20):
    last = con.execute(
        "SELECT COALESCE(MAX(idx),0) FROM names WHERE status!='UNKNOWN'"
    ).fetchone()[0]
    seed_after(last)
    q = asyncio.Queue()
    for row in con.execute("SELECT idx,name FROM names WHERE status='UNKNOWN'"):
        await q.put(row)

    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=parallel)) as sess:
        workers = [asyncio.create_task(worker(q, sess)) for _ in range(parallel)]
        await q.join()
        for w in workers: w.cancel()

if __name__ == "__main__":
    asyncio.run(main())
    atexit.register(con.close)
