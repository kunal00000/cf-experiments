import { parseCronExpression } from "cron-schedule";

export type ScheduleContext = {
	scheduleId: string;
	scheduledFor: number;
	idempotencyKey: string;
	attempt: number;
};

export type ScheduledCallback<Payload = unknown> = (
	payload: Payload,
	context: ScheduleContext,
) => unknown;

export type SchedulerCallbacks = Record<string, ScheduledCallback<never>>;

export type CallbackPayload<Callback> = Callback extends (
	payload: infer Payload,
	...args: never[]
) => unknown
	? Payload
	: never;

export type ScheduleType = "once" | "cron";
export type ScheduleStatus = "pending" | "dead";

export type Schedule<Payload = unknown> = {
	id: string;
	callback: string;
	payload: Payload;
	type: ScheduleType;
	scheduledFor: number;
	runAt: number;
	cron?: string;
	attempts: number;
	maxAttempts: number;
	status: ScheduleStatus;
	lastError?: string;
};

export type ScheduleCriteria = {
	id?: string;
	callback?: string;
	type?: ScheduleType;
	status?: ScheduleStatus;
	runAt?: {
		from?: number;
		to?: number;
	};
};

type ScheduleRow = {
	id: string;
	callback: string;
	payload: string | null;
	kind: ScheduleType;
	scheduled_for: number;
	run_at: number;
	cron: string | null;
	attempts: number;
	max_attempts: number;
	status: ScheduleStatus;
	last_error: string | null;
};

const SCHEDULE_COLUMNS = `
	id, callback, payload, kind, scheduled_for, run_at,
	cron, attempts, max_attempts, status, last_error
`;

export function nextCronTimeMs(expression: string, nowMs: number): number {
	return parseCronExpression(expression).getNextDate(new Date(nowMs)).getTime();
}

export class DurableScheduler<Callbacks extends SchedulerCallbacks = SchedulerCallbacks> {
	readonly #ctx: DurableObjectState;
	readonly #callbacks: Readonly<Callbacks>;

	constructor(ctx: DurableObjectState, callbacks: Callbacks) {
		this.#ctx = ctx;
		this.#callbacks = callbacks;
		this.#ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS schedules (
				id TEXT PRIMARY KEY,
				callback TEXT NOT NULL,
				payload TEXT,
				kind TEXT NOT NULL CHECK(kind IN ('once', 'cron')),
				scheduled_for INTEGER NOT NULL,
				run_at INTEGER NOT NULL,
				cron TEXT,
				attempts INTEGER NOT NULL DEFAULT 0,
				max_attempts INTEGER NOT NULL DEFAULT 3,
				status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'dead')),
				last_error TEXT
			);

			CREATE INDEX IF NOT EXISTS schedules_due
			ON schedules(status, run_at);
		`);
	}

	scheduleAfter<Name extends keyof Callbacks & string>(
		delayMs: number,
		callback: Name,
		payload?: CallbackPayload<Callbacks[Name]>,
	): Promise<Schedule<CallbackPayload<Callbacks[Name]>>> {
		if (!Number.isFinite(delayMs) || delayMs < 0) {
			throw new RangeError("delayMs must be a non-negative finite number");
		}

		return this.#insert("once", Date.now() + delayMs, callback, payload);
	}

	scheduleAt<Name extends keyof Callbacks & string>(
		date: Date,
		callback: Name,
		payload?: CallbackPayload<Callbacks[Name]>,
	): Promise<Schedule<CallbackPayload<Callbacks[Name]>>> {
		const runAt = date.getTime();

		if (!Number.isFinite(runAt)) {
			throw new RangeError("date must be valid");
		}

		return this.#insert("once", runAt, callback, payload);
	}

	scheduleCron<Name extends keyof Callbacks & string>(
		expression: string,
		callback: Name,
		payload?: CallbackPayload<Callbacks[Name]>,
	): Promise<Schedule<CallbackPayload<Callbacks[Name]>>> {
		return this.#insert("cron", nextCronTimeMs(expression, Date.now()), callback, payload, expression);
	}

	get(id: string): Schedule | undefined {
		const row = this.#ctx.storage.sql
			.exec<ScheduleRow>(
				`SELECT ${SCHEDULE_COLUMNS}
				 FROM schedules
				 WHERE id = ?`,
				id,
			)
			.toArray()[0];

		return row ? this.#toSchedule(row) : undefined;
	}

	list(criteria: ScheduleCriteria = {}): Schedule[] {
		let query = `SELECT ${SCHEDULE_COLUMNS} FROM schedules WHERE 1 = 1`;
		const bindings: Array<string | number> = [];

		if (criteria.id) {
			query += " AND id = ?";
			bindings.push(criteria.id);
		}

		if (criteria.callback) {
			query += " AND callback = ?";
			bindings.push(criteria.callback);
		}

		if (criteria.type) {
			query += " AND kind = ?";
			bindings.push(criteria.type);
		}

		if (criteria.status) {
			query += " AND status = ?";
			bindings.push(criteria.status);
		}

		if (criteria.runAt?.from !== undefined) {
			query += " AND run_at >= ?";
			bindings.push(criteria.runAt.from);
		}

		if (criteria.runAt?.to !== undefined) {
			query += " AND run_at <= ?";
			bindings.push(criteria.runAt.to);
		}

		query += " ORDER BY run_at";

		return this.#ctx.storage.sql
			.exec<ScheduleRow>(query, ...bindings)
			.toArray()
			.map((row) => this.#toSchedule(row));
	}

	async cancel(id: string): Promise<boolean> {
		const result = this.#ctx.storage.sql.exec("DELETE FROM schedules WHERE id = ?", id);
		await this.#rearm();
		return result.rowsWritten > 0;
	}

	async handler(): Promise<void> {
		const due = this.#ctx.storage.sql
			.exec<ScheduleRow>(
				`SELECT ${SCHEDULE_COLUMNS}
				 FROM schedules
				 WHERE status = 'pending' AND run_at <= ?
				 ORDER BY run_at
				 LIMIT 50`,
				Date.now(),
			)
			.toArray();

		for (const job of due) {
			const callback = this.#callbacks[job.callback as keyof Callbacks] as unknown as
				| ScheduledCallback<unknown>
				| undefined;

			try {
				if (!callback) {
					throw new Error(`Unknown callback: ${job.callback}`);
				}

				await callback(job.payload === null ? undefined : JSON.parse(job.payload), {
					scheduleId: job.id,
					scheduledFor: job.scheduled_for,
					idempotencyKey: `${job.id}:${job.scheduled_for}`,
					attempt: job.attempts + 1,
				});

				this.#complete(job);
			} catch (error) {
				this.#retry(job, error);
			}
		}

		await this.#rearm();
	}

	async #insert<Payload>(
		kind: ScheduleType,
		runAt: number,
		callback: string,
		payload: Payload | undefined,
		cron?: string,
	): Promise<Schedule<Payload>> {
		if (typeof this.#callbacks[callback as keyof Callbacks] !== "function") {
			throw new Error(`Unknown scheduled callback: ${callback}`);
		}

		let serializedPayload: string | null = null;

		if (payload !== undefined) {
			const serialized = JSON.stringify(payload);

			if (serialized === undefined) {
				throw new TypeError("payload must be JSON-serializable");
			}

			serializedPayload = serialized;
		}

		if (kind === "cron") {
			const existing = this.#ctx.storage.sql
				.exec<ScheduleRow>(
					`SELECT ${SCHEDULE_COLUMNS}
					 FROM schedules
					 WHERE status = 'pending'
					   AND kind = 'cron'
					   AND callback = ?
					   AND payload IS ?
					   AND cron = ?
					 LIMIT 1`,
					callback,
					serializedPayload,
					cron ?? null,
				)
				.toArray()[0];

			if (existing) {
				await this.#rearm();
				return this.#toSchedule<Payload>(existing);
			}
		}

		const id = crypto.randomUUID();

		this.#ctx.storage.sql.exec(
			`INSERT INTO schedules (
				id, callback, payload, kind, scheduled_for, run_at,
				cron
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id,
			callback,
			serializedPayload,
			kind,
			runAt,
			runAt,
			cron ?? null,
		);

		await this.#rearm();
		return this.get(id) as Schedule<Payload>;
	}

	#complete(job: ScheduleRow): void {
		if (job.kind === "once") {
			this.#ctx.storage.sql.exec("DELETE FROM schedules WHERE id = ?", job.id);
			return;
		}

		const next = nextCronTimeMs(job.cron!, Date.now());

		this.#ctx.storage.sql.exec(
			`UPDATE schedules
			 SET scheduled_for = ?, run_at = ?, attempts = 0, last_error = NULL
			 WHERE id = ?`,
			next,
			next,
			job.id,
		);
	}

	#retry(job: Pick<ScheduleRow, "id" | "attempts" | "max_attempts">, error: unknown): void {
		const attempts = job.attempts + 1;
		const message = error instanceof Error ? error.message : String(error);

		if (attempts >= job.max_attempts) {
			this.#ctx.storage.sql.exec(
				`UPDATE schedules
				 SET status = 'dead', attempts = ?, last_error = ?
				 WHERE id = ?`,
				attempts,
				message,
				job.id,
			);
			return;
		}

		const retryAt = Date.now() + Math.min(1_000 * 2 ** attempts, 60_000);

		this.#ctx.storage.sql.exec(
			`UPDATE schedules
			 SET attempts = ?, run_at = ?, last_error = ?
			 WHERE id = ?`,
			attempts,
			retryAt,
			message,
			job.id,
		);
	}

	#toSchedule<Payload = unknown>(row: ScheduleRow): Schedule<Payload> {
		return {
			id: row.id,
			callback: row.callback,
			payload: (row.payload === null ? undefined : JSON.parse(row.payload)) as Payload,
			type: row.kind,
			scheduledFor: row.scheduled_for,
			runAt: row.run_at,
			...(row.cron === null ? {} : { cron: row.cron }),
			attempts: row.attempts,
			maxAttempts: row.max_attempts,
			status: row.status,
			...(row.last_error === null ? {} : { lastError: row.last_error }),
		};
	}

	async #rearm(): Promise<void> {
		const next = this.#ctx.storage.sql
			.exec<{ run_at: number | null }>(
				`SELECT MIN(run_at) AS run_at
				 FROM schedules
				 WHERE status = 'pending'`,
			)
			.one();

		if (next.run_at === null) {
			await this.#ctx.storage.deleteAlarm();
		} else {
			await this.#ctx.storage.setAlarm(next.run_at);
		}
	}
}
