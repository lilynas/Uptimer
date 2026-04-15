import { AppError } from '../middleware/errors';
import {
  publicHomepageRenderArtifactSchema,
  publicHomepageResponseSchema,
  type PublicHomepageResponse,
} from '../schemas/public-homepage';

const SNAPSHOT_KEY = 'homepage';
const SNAPSHOT_ARTIFACT_KEY = 'homepage:artifact';
const MAX_AGE_SECONDS = 60;
const MAX_STALE_SECONDS = 10 * 60;
const MAX_FUTURE_SNAPSHOT_SKEW_SECONDS = 60;
const SPLIT_SNAPSHOT_VERSION = 3;
const LEGACY_COMBINED_SNAPSHOT_VERSION = 2;
type SnapshotKey = typeof SNAPSHOT_KEY | typeof SNAPSHOT_ARTIFACT_KEY;

const READ_REFRESH_SNAPSHOT_ROWS_SQL = `
  SELECT key, generated_at, updated_at, body_json
  FROM public_snapshots
  WHERE key = ?1 OR key = ?2
`;
const READ_REFRESH_SNAPSHOT_ROW_BY_KEY_SQL = `
  SELECT generated_at, updated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;
const readRefreshSnapshotRowsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const readRefreshSnapshotRowByKeyStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const normalizedHomepagePayloadCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, NormalizedSnapshotRow>
>();
const normalizedHomepageArtifactCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, NormalizedSnapshotRow>
>();
const normalizedHomepagePayloadCacheGlobal = new Map<SnapshotKey, RawNormalizedSnapshotRow>();
const normalizedHomepageArtifactCacheGlobal = new Map<SnapshotKey, RawNormalizedSnapshotRow>();
const parsedHomepagePayloadCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, ParsedSnapshotRow>
>();
const parsedHomepagePayloadCacheGlobal = new Map<SnapshotKey, RawParsedSnapshotRow>();

type SnapshotRefreshRow = {
  key: SnapshotKey;
  generated_at: number;
  body_json: string;
  updated_at?: number | null;
};

type SnapshotCandidate = {
  key: SnapshotKey;
  generatedAt: number;
  updatedAt: number;
};

type NormalizedSnapshotRow = {
  generatedAt: number;
  updatedAt: number;
  rawBodyJson: string;
  bodyJson: string;
};

type RawNormalizedSnapshotRow = NormalizedSnapshotRow & {
  rawBodyJson: string;
};

type ParsedSnapshotRow = {
  generatedAt: number;
  updatedAt: number;
  rawBodyJson: string;
  snapshot: PublicHomepageResponse;
};

type RawParsedSnapshotRow = ParsedSnapshotRow & {
  rawBodyJson: string;
};

type ParsedJsonText = {
  trimmed: string;
  value: unknown;
};

type CandidateReadResult = {
  row: NormalizedSnapshotRow | null;
  invalid: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function parseJsonText(text: string): ParsedJsonText | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return {
      trimmed,
      value: JSON.parse(trimmed) as unknown,
    };
  } catch {
    return null;
  }
}

function normalizeDirectHomepagePayload(
  value: unknown,
): string | null {
  const parsedPayload = parseDirectHomepagePayload(value);
  return parsedPayload ? JSON.stringify(parsedPayload) : null;
}

function parseDirectHomepagePayload(value: unknown): PublicHomepageResponse | null {
  const directPayload = publicHomepageResponseSchema.safeParse(value);
  if (directPayload.success) {
    return directPayload.data;
  }
  if (!isRecord(value)) {
    return null;
  }

  const normalizedPayload = publicHomepageResponseSchema.safeParse({
    ...value,
    bootstrap_mode:
      value.bootstrap_mode === 'full' || value.bootstrap_mode === 'partial'
        ? value.bootstrap_mode
        : 'full',
    monitor_count_total: Array.isArray(value.monitors) ? value.monitors.length : 0,
  });
  return normalizedPayload.success ? normalizedPayload.data : null;
}

function normalizeHomepagePayloadBodyJsonForKey(
  key: SnapshotKey,
  bodyJson: string,
): string | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;

  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version === SPLIT_SNAPSHOT_VERSION || version === LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return normalizeDirectHomepagePayload(parsed.value.data);
  }

  if (key === SNAPSHOT_KEY) {
    const directPayload = normalizeDirectHomepagePayload(parsed.value);
    if (directPayload) {
      return directPayload;
    }
  }

  const artifactSnapshot = publicHomepageResponseSchema.safeParse(parsed.value.snapshot);
  if (artifactSnapshot.success) {
    return JSON.stringify(artifactSnapshot.data);
  }

  return key === SNAPSHOT_KEY ? null : normalizeDirectHomepagePayload(parsed.value);
}

function normalizeHomepageArtifactBodyJson(bodyJson: string): string | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;

  const artifact = publicHomepageRenderArtifactSchema.safeParse(parsed.value);
  if (artifact.success) {
    return parsed.trimmed;
  }
  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version !== SPLIT_SNAPSHOT_VERSION && version !== LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return null;
  }

  const legacyArtifact = publicHomepageRenderArtifactSchema.safeParse(parsed.value.render);
  return legacyArtifact.success ? JSON.stringify(legacyArtifact.data) : null;
}

function parseHomepagePayloadSnapshotForKey(
  key: SnapshotKey,
  bodyJson: string,
): PublicHomepageResponse | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;
  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version === SPLIT_SNAPSHOT_VERSION || version === LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return parseDirectHomepagePayload(parsed.value.data);
  }

  if (key === SNAPSHOT_KEY) {
    const directPayload = parseDirectHomepagePayload(parsed.value);
    if (directPayload) {
      return directPayload;
    }
  }

  const artifactSnapshot = publicHomepageResponseSchema.safeParse(parsed.value.snapshot);
  if (artifactSnapshot.success) {
    return artifactSnapshot.data;
  }

  return key === SNAPSHOT_KEY ? null : parseDirectHomepagePayload(parsed.value);
}

function toSnapshotUpdatedAt(row: Pick<SnapshotRefreshRow, 'generated_at' | 'updated_at'>): number {
  return typeof row.updated_at === 'number' && Number.isFinite(row.updated_at)
    ? row.updated_at
    : row.generated_at;
}

function getNormalizedSnapshotCache(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
): Map<SnapshotKey, NormalizedSnapshotRow> {
  const cached = cacheByDb.get(db);
  if (cached) {
    return cached;
  }

  const next = new Map<SnapshotKey, NormalizedSnapshotRow>();
  cacheByDb.set(db, next);
  return next;
}

function getParsedSnapshotCache(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, ParsedSnapshotRow>>,
  db: D1Database,
): Map<SnapshotKey, ParsedSnapshotRow> {
  const cached = cacheByDb.get(db);
  if (cached) {
    return cached;
  }

  const next = new Map<SnapshotKey, ParsedSnapshotRow>();
  cacheByDb.set(db, next);
  return next;
}

function readCachedNormalizedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): NormalizedSnapshotRow | null {
  const cache = getNormalizedSnapshotCache(cacheByDb, db);
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedNormalizedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  bodyJson: string,
): NormalizedSnapshotRow {
  const row: NormalizedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    bodyJson,
  };
  getNormalizedSnapshotCache(cacheByDb, db).set(candidate.key, row);
  return row;
}

function readCachedParsedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, ParsedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): ParsedSnapshotRow | null {
  const cache = getParsedSnapshotCache(cacheByDb, db);
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedParsedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, ParsedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  snapshot: PublicHomepageResponse,
): ParsedSnapshotRow {
  const row: ParsedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    snapshot,
  };
  getParsedSnapshotCache(cacheByDb, db).set(candidate.key, row);
  return row;
}

function readCachedNormalizedSnapshotRowGlobal(
  cache: ReadonlyMap<SnapshotKey, RawNormalizedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): NormalizedSnapshotRow | null {
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedNormalizedSnapshotRowGlobal(
  cache: Map<SnapshotKey, RawNormalizedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  bodyJson: string,
): RawNormalizedSnapshotRow {
  const row: RawNormalizedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    bodyJson,
  };
  cache.set(candidate.key, row);
  return row;
}

function readCachedParsedSnapshotRowGlobal(
  cache: ReadonlyMap<SnapshotKey, RawParsedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): ParsedSnapshotRow | null {
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedParsedSnapshotRowGlobal(
  cache: Map<SnapshotKey, RawParsedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  snapshot: PublicHomepageResponse,
): RawParsedSnapshotRow {
  const row: RawParsedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    snapshot,
  };
  cache.set(candidate.key, row);
  return row;
}

function isSameUtcDay(a: number, b: number): boolean {
  return Math.floor(a / 86_400) === Math.floor(b / 86_400);
}

function isFutureSnapshotCandidate(candidate: SnapshotCandidate, now: number): boolean {
  return candidate.generatedAt > now + MAX_FUTURE_SNAPSHOT_SKEW_SECONDS;
}

function snapshotCandidateAgeSeconds(candidate: SnapshotCandidate, now: number): number {
  return Math.max(0, now - candidate.generatedAt);
}

async function readRefreshSnapshotRows(
  db: D1Database,
): Promise<SnapshotRefreshRow[]> {
  try {
    const cached = readRefreshSnapshotRowsStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_REFRESH_SNAPSHOT_ROWS_SQL);
    if (!cached) {
      readRefreshSnapshotRowsStatementByDb.set(db, statement);
    }

    const { results } = await statement
      .bind(SNAPSHOT_KEY, SNAPSHOT_ARTIFACT_KEY)
      .all<SnapshotRefreshRow>();
    return results ?? [];
  } catch (err) {
    console.warn('homepage snapshot: refresh rows read failed', err);
    return [];
  }
}

async function readRefreshSnapshotRowByKey(
  db: D1Database,
  key: SnapshotKey,
): Promise<SnapshotRefreshRow | null> {
  try {
    const cached = readRefreshSnapshotRowByKeyStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_REFRESH_SNAPSHOT_ROW_BY_KEY_SQL);
    if (!cached) {
      readRefreshSnapshotRowByKeyStatementByDb.set(db, statement);
    }

    const row = await statement.bind(key).first<Omit<SnapshotRefreshRow, 'key'>>();
    return row ? { key, ...row } : null;
  } catch (err) {
    console.warn('homepage snapshot: refresh row read failed', err);
    return null;
  }
}

function listSnapshotCandidatesFromRefreshRows(
  rows: readonly Pick<SnapshotRefreshRow, 'key' | 'generated_at' | 'updated_at'>[],
): SnapshotCandidate[] {
  return rows.map((row) => ({
    key: row.key,
    generatedAt: row.generated_at,
    updatedAt: toSnapshotUpdatedAt(row),
  }));
}

function comparePayloadCandidates(a: SnapshotCandidate, b: SnapshotCandidate): number {
  if (a.generatedAt !== b.generatedAt) {
    return b.generatedAt - a.generatedAt;
  }
  if (a.key === b.key) {
    return 0;
  }
  return a.key === SNAPSHOT_KEY ? -1 : 1;
}

function compareArtifactCandidates(a: SnapshotCandidate, b: SnapshotCandidate): number {
  if (a.generatedAt !== b.generatedAt) {
    return b.generatedAt - a.generatedAt;
  }
  if (a.key === b.key) {
    return 0;
  }
  return a.key === SNAPSHOT_ARTIFACT_KEY ? -1 : 1;
}

function readValidatedSnapshotCandidateFromRefreshRows(opts: {
  db: D1Database;
  candidate: SnapshotCandidate;
  rowByKey: ReadonlyMap<SnapshotKey, SnapshotRefreshRow>;
  normalize: (candidate: SnapshotCandidate, bodyJson: string) => string | null;
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>;
  globalCache: Map<SnapshotKey, RawNormalizedSnapshotRow>;
}): CandidateReadResult {
  const row = opts.rowByKey.get(opts.candidate.key);
  if (!row || row.generated_at !== opts.candidate.generatedAt) {
    return { row: null, invalid: false };
  }

  const dbCached = readCachedNormalizedSnapshotRow(
    opts.cacheByDb,
    opts.db,
    opts.candidate,
    row.body_json,
  );
  if (dbCached) {
    return { row: dbCached, invalid: false };
  }

  const globalCached = readCachedNormalizedSnapshotRowGlobal(
    opts.globalCache,
    opts.candidate,
    row.body_json,
  );
  if (globalCached) {
    return {
      row: writeCachedNormalizedSnapshotRow(
        opts.cacheByDb,
        opts.db,
        opts.candidate,
        row.body_json,
        globalCached.bodyJson,
      ),
      invalid: false,
    };
  }

  const bodyJson = opts.normalize(opts.candidate, row.body_json);
  if (!bodyJson) {
    return { row: null, invalid: true };
  }

  writeCachedNormalizedSnapshotRowGlobal(
    opts.globalCache,
    opts.candidate,
    row.body_json,
    bodyJson,
  );
  return {
    row: writeCachedNormalizedSnapshotRow(
      opts.cacheByDb,
      opts.db,
      opts.candidate,
      row.body_json,
      bodyJson,
    ),
    invalid: false,
  };
}

function toSnapshotCandidateFromRow(row: SnapshotRefreshRow): SnapshotCandidate {
  return {
    key: row.key,
    generatedAt: row.generated_at,
    updatedAt: toSnapshotUpdatedAt(row),
  };
}

function readNormalizedPayloadCandidateFromRow(opts: {
  db: D1Database;
  now: number;
  row: SnapshotRefreshRow | null;
}): { row: NormalizedSnapshotRow | null; invalid: boolean } {
  const { db, now, row } = opts;
  if (!row) {
    return { row: null, invalid: false };
  }

  const candidate = toSnapshotCandidateFromRow(row);
  if (isFutureSnapshotCandidate(candidate, now)) {
    return { row: null, invalid: true };
  }

  const dbCached = readCachedNormalizedSnapshotRow(
    normalizedHomepagePayloadCacheByDb,
    db,
    candidate,
    row.body_json,
  );
  if (dbCached) {
    return { row: dbCached, invalid: false };
  }

  const globalCached = readCachedNormalizedSnapshotRowGlobal(
    normalizedHomepagePayloadCacheGlobal,
    candidate,
    row.body_json,
  );
  if (globalCached) {
    return {
      row: writeCachedNormalizedSnapshotRow(
        normalizedHomepagePayloadCacheByDb,
        db,
        candidate,
        row.body_json,
        globalCached.bodyJson,
      ),
      invalid: false,
    };
  }

  const bodyJson = normalizeHomepagePayloadBodyJsonForKey(candidate.key, row.body_json);
  if (!bodyJson) {
    return { row: null, invalid: true };
  }

  return {
    row: writeCachedNormalizedSnapshotRow(
      normalizedHomepagePayloadCacheByDb,
      db,
      candidate,
      row.body_json,
      writeCachedNormalizedSnapshotRowGlobal(
        normalizedHomepagePayloadCacheGlobal,
        candidate,
        row.body_json,
        bodyJson,
      ).bodyJson,
    ),
    invalid: false,
  };
}

function readParsedPayloadCandidateFromRow(opts: {
  db: D1Database;
  now: number;
  row: SnapshotRefreshRow | null;
}): { row: ParsedSnapshotRow | null; invalid: boolean } {
  const { db, now, row } = opts;
  if (!row) {
    return { row: null, invalid: false };
  }

  const candidate = toSnapshotCandidateFromRow(row);
  if (isFutureSnapshotCandidate(candidate, now)) {
    return { row: null, invalid: true };
  }

  const dbCached = readCachedParsedSnapshotRow(
    parsedHomepagePayloadCacheByDb,
    db,
    candidate,
    row.body_json,
  );
  if (dbCached) {
    return { row: dbCached, invalid: false };
  }

  const globalCached = readCachedParsedSnapshotRowGlobal(
    parsedHomepagePayloadCacheGlobal,
    candidate,
    row.body_json,
  );
  if (globalCached) {
    return {
      row: writeCachedParsedSnapshotRow(
        parsedHomepagePayloadCacheByDb,
        db,
        candidate,
        row.body_json,
        globalCached.snapshot,
      ),
      invalid: false,
    };
  }

  const snapshot = parseHomepagePayloadSnapshotForKey(candidate.key, row.body_json);
  if (!snapshot) {
    return { row: null, invalid: true };
  }

  return {
    row: writeCachedParsedSnapshotRow(
      parsedHomepagePayloadCacheByDb,
      db,
      candidate,
      row.body_json,
      writeCachedParsedSnapshotRowGlobal(
        parsedHomepagePayloadCacheGlobal,
        candidate,
        row.body_json,
        snapshot,
      ).snapshot,
    ),
    invalid: false,
  };
}

function pickPreferredSnapshotRow<Row extends { generatedAt: number; updatedAt: number }>(
  payloadRow: Row | null,
  artifactRow: Row | null,
  predicate?: (row: Row) => boolean,
): { key: SnapshotKey; row: Row } | null {
  const preferredPayload =
    payloadRow && (!predicate || predicate(payloadRow)) ? payloadRow : null;
  const preferredArtifact =
    artifactRow && (!predicate || predicate(artifactRow)) ? artifactRow : null;

  if (preferredPayload && preferredArtifact) {
    return comparePayloadCandidates(
      {
        key: SNAPSHOT_KEY,
        generatedAt: preferredPayload.generatedAt,
        updatedAt: preferredPayload.updatedAt,
      },
      {
        key: SNAPSHOT_ARTIFACT_KEY,
        generatedAt: preferredArtifact.generatedAt,
        updatedAt: preferredArtifact.updatedAt,
      },
    ) <= 0
      ? { key: SNAPSHOT_KEY, row: preferredPayload }
      : { key: SNAPSHOT_ARTIFACT_KEY, row: preferredArtifact };
  }

  if (preferredPayload) {
    return { key: SNAPSHOT_KEY, row: preferredPayload };
  }
  if (preferredArtifact) {
    return { key: SNAPSHOT_ARTIFACT_KEY, row: preferredArtifact };
  }
  return null;
}

export async function readHomepageSnapshotGeneratedAt(
  db: D1Database,
  now = Math.floor(Date.now() / 1000),
): Promise<number | null> {
  const payloadResult = readNormalizedPayloadCandidateFromRow({
    db,
    now,
    row: await readRefreshSnapshotRowByKey(db, SNAPSHOT_KEY),
  });

  const artifactResult = readNormalizedPayloadCandidateFromRow({
    db,
    now,
    row: await readRefreshSnapshotRowByKey(db, SNAPSHOT_ARTIFACT_KEY),
  });

  return pickPreferredSnapshotRow(payloadResult.row, artifactResult.row)?.row.generatedAt ?? null;
}

export async function readHomepageArtifactSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter((candidate) => candidate.key === SNAPSHOT_ARTIFACT_KEY)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (_candidate, bodyJson) => normalizeHomepageArtifactBodyJson(bodyJson),
      cacheByDb: normalizedHomepageArtifactCacheByDb,
      globalCache: normalizedHomepageArtifactCacheGlobal,
    });
    if (result.row) {
      return result.row.generatedAt;
    }
  }

  return null;
}

export async function readHomepageRefreshBaseSnapshot(
  db: D1Database,
  now: number,
): Promise<{
  generatedAt: number | null;
  snapshot: PublicHomepageResponse | null;
  seedDataSnapshot: boolean;
}> {
  const payloadResult = readParsedPayloadCandidateFromRow({
    db,
    now,
    row: await readRefreshSnapshotRowByKey(db, SNAPSHOT_KEY),
  });

  const artifactResult = readParsedPayloadCandidateFromRow({
    db,
    now,
    row: await readRefreshSnapshotRowByKey(db, SNAPSHOT_ARTIFACT_KEY),
  });

  const freshestSameDayBase = pickPreferredSnapshotRow(
    payloadResult.row,
    artifactResult.row,
    (row) => isSameUtcDay(row.generatedAt, now),
  );
  if (freshestSameDayBase) {
    return {
      generatedAt: freshestSameDayBase.row.generatedAt,
      snapshot: freshestSameDayBase.row.snapshot,
      seedDataSnapshot: false,
    };
  }

  const freshestBase = pickPreferredSnapshotRow(payloadResult.row, artifactResult.row);
  if (freshestBase) {
    return {
      generatedAt: freshestBase.row.generatedAt,
      snapshot: freshestBase.row.snapshot,
      seedDataSnapshot: true,
    };
  }

  if (!payloadResult.invalid && !artifactResult.invalid) {
    return {
      generatedAt: null,
      snapshot: null,
      seedDataSnapshot: true,
    };
  }

  if (payloadResult.invalid || artifactResult.invalid) {
    console.warn('homepage snapshot: invalid refresh payload');
  }

  return {
    generatedAt: null,
    snapshot: null,
    seedDataSnapshot: true,
  };
}

export function primeHomepageRefreshBaseSnapshotCache(opts: {
  db: D1Database;
  generatedAt: number;
  updatedAt: number;
  snapshot: PublicHomepageResponse;
  renderBodyJson: string;
  payloadBodyJson?: string | null;
}): void {
  const artifactCandidate: SnapshotCandidate = {
    key: SNAPSHOT_ARTIFACT_KEY,
    generatedAt: opts.generatedAt,
    updatedAt: opts.updatedAt,
  };
  writeCachedParsedSnapshotRowGlobal(
    parsedHomepagePayloadCacheGlobal,
    artifactCandidate,
    opts.renderBodyJson,
    opts.snapshot,
  );
  writeCachedParsedSnapshotRow(
    parsedHomepagePayloadCacheByDb,
    opts.db,
    artifactCandidate,
    opts.renderBodyJson,
    opts.snapshot,
  );

  if (!opts.payloadBodyJson) {
    return;
  }

  const payloadCandidate: SnapshotCandidate = {
    key: SNAPSHOT_KEY,
    generatedAt: opts.generatedAt,
    updatedAt: opts.updatedAt,
  };
  writeCachedParsedSnapshotRowGlobal(
    parsedHomepagePayloadCacheGlobal,
    payloadCandidate,
    opts.payloadBodyJson,
    opts.snapshot,
  );
  writeCachedParsedSnapshotRow(
    parsedHomepagePayloadCacheByDb,
    opts.db,
    payloadCandidate,
    opts.payloadBodyJson,
    opts.snapshot,
  );
}

export function applyHomepageCacheHeaders(res: Response, ageSeconds: number): void {
  const remaining = Math.max(0, MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(30, remaining);
  const stale = Math.max(0, remaining - maxAge);

  res.headers.set(
    'Cache-Control',
    `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`,
  );
}

export async function readHomepageSnapshotJsonAnyAge(
  db: D1Database,
  now: number,
  maxStaleSeconds = MAX_STALE_SECONDS,
): Promise<{ bodyJson: string; age: number } | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter(
      (candidate) =>
        !isFutureSnapshotCandidate(candidate, now) &&
        snapshotCandidateAgeSeconds(candidate, now) <= maxStaleSeconds,
    )
    .sort(comparePayloadCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (currentCandidate, bodyJson) =>
        normalizeHomepagePayloadBodyJsonForKey(currentCandidate.key, bodyJson),
      cacheByDb: normalizedHomepagePayloadCacheByDb,
      globalCache: normalizedHomepagePayloadCacheGlobal,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: snapshotCandidateAgeSeconds(
        {
          key: candidate.key,
          generatedAt: result.row.generatedAt,
          updatedAt: result.row.updatedAt,
        },
        now,
      ),
    };
  }

  return null;
}

export async function readHomepageSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  return await readHomepageSnapshotJsonAnyAge(db, now, MAX_AGE_SECONDS);
}

export async function readHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter(
      (candidate) =>
        !isFutureSnapshotCandidate(candidate, now) &&
        snapshotCandidateAgeSeconds(candidate, now) <= MAX_AGE_SECONDS,
    )
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (_candidate, bodyJson) => normalizeHomepageArtifactBodyJson(bodyJson),
      cacheByDb: normalizedHomepageArtifactCacheByDb,
      globalCache: normalizedHomepageArtifactCacheGlobal,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid artifact payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: snapshotCandidateAgeSeconds(
        {
          key: candidate.key,
          generatedAt: result.row.generatedAt,
          updatedAt: result.row.updatedAt,
        },
        now,
      ),
    };
  }

  return null;
}

export async function readStaleHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter((candidate) => Math.max(0, now - candidate.generatedAt) <= MAX_STALE_SECONDS)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (_candidate, bodyJson) => normalizeHomepageArtifactBodyJson(bodyJson),
      cacheByDb: normalizedHomepageArtifactCacheByDb,
      globalCache: normalizedHomepageArtifactCacheGlobal,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid stale artifact payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: Math.max(0, now - result.row.generatedAt),
    };
  }

  return null;
}

export function assertHomepageArtifactAvailable(): never {
  throw new AppError(503, 'UNAVAILABLE', 'Homepage artifact unavailable');
}
