import { Pool, PoolClient, types } from "pg";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

types.setTypeParser(20, function (val) {
  return parseInt(val);
});

let connectionString = "postgres://localhost:5432/replicate-issue";
if (process.env.DATABASE_URL) {
  connectionString = process.env.DATABASE_URL;
}
// DATABASE_URL="postgres://postgres:xyz1234@localhost:5432/replicate-issue"
export const pool = new Pool({
  connectionString,
});

export async function withTransaction<T>(
  f: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
   // await client.query('LOCK TABLE c IN ACCESS EXCLUSIVE MODE');
   // await client.query('LOCK TABLE x IN ACCESS EXCLUSIVE MODE');

    const r = await f(client);
    await client.query("COMMIT");
    return r;
  } catch (e) {
    console.error("withTransaction caught: ", e, " trying to rollback...");
    // TODO: uncomment this after 1855 is merged
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

// demo of problem, rerun it and it should return nothing

async function run() {
  // transaction one
  withTransaction(async (c) => {
    const x = await c.query(
      `SELECT claimable FROM c WHERE claimable->>'id' NOT IN (SELECT status->>'id' FROM x) FOR UPDATE`
    );
    console.log(x.rows, "#1"); // should return one
    // so we make sure tx #2 is called before this one is finished
    await delay(2000);
    await c.query(`INSERT INTO x(status) VALUES($1)`, [
      { id: x.rows[0].claimable.id, kind: "SENT" },
    ]);
  });

  // AAb(); will be called concurrent with transaction one
  withTransaction(async (c) => {
    const x = await c.query(
      `SELECT claimable FROM c WHERE claimable->>'id' NOT IN (SELECT status->>'id' FROM x) FOR UPDATE`
    );
    console.log(x.rows, "#2"); // should return 0, but returns one?

    await c.query(`INSERT INTO x(status) VALUES($1)`, [x.rows[0].claimable.id]);
  });

  // AAc(); // will return 0 as it's triggered after other two are committed
  await delay(5000);
  withTransaction(async (c) => {
    const x = await c.query(
      `SELECT claimable FROM c WHERE claimable->>'id' NOT IN (SELECT status->>'id' FROM x) FOR UPDATE`
    );
    console.log(x.rows, "#3"); // should return 0, returns 0

    await c.query(`INSERT INTO x(status) VALUES($1)`, [x.rows[0].claimable.id]);
  });
}

run();

// returns for me:

// [ { claimable: { id: 1 } } ] #1
// [ { claimable: { id: 1 } } ] #2 // this one should be empty?!
// [] #3


// const x = await c.query(`SELECT claimable FROM c WHERE claimable->>'id' NOT IN (SELECT status->>'id' FROM x) FOR UPDATE`)

// more alike to sendHookout query, gives same results
// const x = await c.query(`SELECT claimable FROM c WHERE claimable->>'id' NOT IN (SELECT status->>'id' FROM x WHERE (status->>'kind' = 'FAILED' OR status->>'kind' = 'SENT')) FOR UPDATE`)


// delete inserts into x (status) after running dev: DELETE FROM x;