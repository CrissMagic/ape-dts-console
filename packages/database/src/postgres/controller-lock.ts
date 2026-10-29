import type { Pool, PoolClient, QueryResultRow } from 'pg';

const CONTROLLER_LOCK_NAMESPACE = 1_096_048_964;
const CONTROLLER_LOCK_KEY = 1_297_305_922;

export async function inspectControllerLockHolders(pool: Pick<Pool, 'query'>) {
  return (
    await pool.query<{
      pid: number;
      username: string | null;
      application_name: string | null;
      client_addr: string | null;
      client_port: number | null;
      backend_started_at: string | null;
      state: string | null;
    }>(
      `SELECT activity.pid, activity.usename AS username, activity.application_name,
             host(activity.client_addr) AS client_addr, activity.client_port,
             to_char(activity.backend_start AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI:SS.US') AS backend_started_at,
             activity.state
        FROM pg_catalog.pg_locks AS lock
        JOIN pg_catalog.pg_stat_activity AS activity ON activity.pid=lock.pid
       WHERE lock.locktype='advisory' AND lock.classid=$1::oid AND lock.objid=$2::oid
         AND lock.objsubid=2 AND lock.granted
         AND lock.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
       ORDER BY activity.pid LIMIT 10`,
      [CONTROLLER_LOCK_NAMESPACE, CONTROLLER_LOCK_KEY],
    )
  ).rows;
}

interface LockRow extends QueryResultRow {
  acquired: boolean;
}

interface UnlockRow extends QueryResultRow {
  released: boolean;
}

export class ControllerLockLease {
  private released = false;
  private probing: Promise<void> | undefined;
  private readonly heartbeat: NodeJS.Timeout;
  private readonly connectionError = (error: Error): void => {
    if (this.released) return;
    this.abandon();
    this.onLost?.(error);
  };
  private readonly connectionEnded = (): void => {
    this.connectionError(new Error('Controller connection terminated'));
  };

  constructor(
    private readonly client: PoolClient,
    private readonly onLost?: (error: Error) => void,
  ) {
    client.on('error', this.connectionError);
    client.on('end', this.connectionEnded);
    this.heartbeat = setInterval(() => {
      void this.probe().catch(() => undefined);
    }, 10_000);
    this.heartbeat.unref();
  }

  isReleased(): boolean {
    return this.released;
  }

  probe(): Promise<void> {
    if (this.released) return Promise.reject(new Error('Controller connection closed'));
    this.probing ??= this.probeConnection().finally(() => {
      this.probing = undefined;
    });
    return this.probing;
  }

  private async probeConnection(): Promise<void> {
    try {
      await this.client.query('SELECT 1');
    } catch (error) {
      this.connectionError(error instanceof Error ? error : new Error('Controller probe failed'));
      throw error;
    }
  }

  private stopMonitoring(): void {
    clearInterval(this.heartbeat);
    this.client.off('error', this.connectionError);
    this.client.off('end', this.connectionEnded);
  }

  async release(): Promise<void> {
    if (this.released) {
      return;
    }
    this.released = true;
    clearInterval(this.heartbeat);
    let reusable = false;
    try {
      const result = await this.client.query<UnlockRow>(
        'SELECT pg_advisory_unlock($1, $2) AS released',
        [CONTROLLER_LOCK_NAMESPACE, CONTROLLER_LOCK_KEY],
      );
      reusable = result.rows[0]?.released === true;
      if (!reusable) throw new Error('CONTROLLER_LOCK_NOT_HELD');
    } finally {
      this.stopMonitoring();
      // A timed-out unlock may still own the session lock; never return it to the idle pool.
      this.client.release(!reusable);
    }
  }

  abandon(): void {
    if (this.released) {
      return;
    }
    this.released = true;
    this.stopMonitoring();
    this.client.release(true);
  }
}

export async function tryAcquireControllerLock(
  pool: Pool,
  onLost?: (error: Error) => void,
): Promise<ControllerLockLease | null> {
  const client = await pool.connect();
  try {
    const result = await client.query<LockRow>('SELECT pg_try_advisory_lock($1, $2) AS acquired', [
      CONTROLLER_LOCK_NAMESPACE,
      CONTROLLER_LOCK_KEY,
    ]);
    if (result.rows[0]?.acquired !== true) {
      client.release();
      return null;
    }
    return new ControllerLockLease(client, onLost);
  } catch (error) {
    client.release(true);
    throw error;
  }
}
