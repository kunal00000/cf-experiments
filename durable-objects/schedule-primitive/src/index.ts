import { DurableObject } from "cloudflare:workers";
import { DurableScheduler, type ScheduleContext, type ScheduleCriteria } from "./durable-scheduler";

type LogPayload = {
	message: string;
};

type ScheduleRequest =
	| { action: "scheduleAfter"; delayMs: number; payload: LogPayload }
	| { action: "scheduleAt"; timestamp: number; payload: LogPayload }
	| { action: "scheduleCron"; expression: string; payload: LogPayload }
	| { action: "get"; id: string }
	| { action: "list"; criteria?: ScheduleCriteria }
	| { action: "cancel"; id: string };

const callbacks = {
	log: (payload: LogPayload, context: ScheduleContext) => {
		console.log({ payload, ...context });
	},
};

export class MyDurableObject extends DurableObject<Env> {
	readonly #alarm: DurableScheduler<typeof callbacks>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#alarm = new DurableScheduler(ctx, callbacks);
	}

	async fetch(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return new Response("POST only", { status: 405 });
		}

		const input = await request.json<ScheduleRequest>();

		switch (input.action) {
			case "scheduleAfter":
				return Response.json(
					await this.#alarm.scheduleAfter(input.delayMs, "log", input.payload),
				);
			case "scheduleAt":
				return Response.json(
					await this.#alarm.scheduleAt(new Date(input.timestamp), "log", input.payload),
				);
			case "scheduleCron":
				return Response.json(
					await this.#alarm.scheduleCron(input.expression, "log", input.payload),
				);
			case "get": {
				const schedule = this.#alarm.get(input.id);
				return schedule ? Response.json(schedule) : new Response(null, { status: 404 });
			}
			case "list":
				return Response.json(this.#alarm.list(input.criteria));
			case "cancel":
				return Response.json({ cancelled: await this.#alarm.cancel(input.id) });
		}
	}

	alarm(): Promise<void> {
		return this.#alarm.handler();
	}
}

export default {
	fetch(request, env): Promise<Response> {
		return env.MY_DURABLE_OBJECT.getByName("test").fetch(request);
	},
} satisfies ExportedHandler<Env>;
