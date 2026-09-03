import { DurableObject } from "cloudflare:workers";

type AlarmConfig = {
	durationMs: number;
	failure: "never" | "once" | "always";
};

type SetAlarmRequest = Partial<AlarmConfig> & {
	delayMs: number;
};

export class NativeAlarm extends DurableObject<Env> {
	async fetch(request: Request): Promise<Response> {
		if (request.method === "GET") {
			return Response.json({
				alarm: await this.ctx.storage.getAlarm(),
				config: await this.ctx.storage.get<AlarmConfig>("config"),
			});
		}

		if (request.method === "DELETE") {
			await this.ctx.storage.deleteAlarm();
			return new Response(null, { status: 204 });
		}

		if (request.method !== "POST") {
			return new Response("Use GET, POST, or DELETE", { status: 405 });
		}

		const input = await request.json<SetAlarmRequest>();
		const previousAlarm = await this.ctx.storage.getAlarm();
		const previousConfig = await this.ctx.storage.get<AlarmConfig>("config");
		const config: AlarmConfig = {
			durationMs: input.durationMs ?? previousConfig?.durationMs ?? 0,
			failure: input.failure ?? previousConfig?.failure ?? "never",
		};
		const scheduledTime = Date.now() + input.delayMs;

		await this.ctx.storage.put("config", config);
		await this.ctx.storage.setAlarm(scheduledTime);

		console.log({ event: "alarm.set", observedAt: Date.now(), previousAlarm, scheduledTime, config });

		return Response.json({ previousAlarm, scheduledTime, config });
	}

	async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
		const config = (await this.ctx.storage.get<AlarmConfig>("config")) ?? {
			durationMs: 0,
			failure: "never",
		};

		console.log({
			event: "alarm.started",
			observedAt: Date.now(),
			storedAlarm: await this.ctx.storage.getAlarm(),
			config,
			alarmInfo,
		});

		if (config.failure === "always" || (config.failure === "once" && !alarmInfo.isRetry)) {
			console.log({ event: "alarm.throwing", observedAt: Date.now(), alarmInfo });
			throw new Error("Intentional native alarm failure");
		}

		await new Promise<void>((resolve) => setTimeout(resolve, config.durationMs));

		console.log({
			event: "alarm.completed",
			observedAt: Date.now(),
			storedAlarm: await this.ctx.storage.getAlarm(),
			alarmInfo,
		});
	}
}

export default {
	fetch(request, env): Promise<Response> {
		return env.NATIVE_ALARM.getByName("repro").fetch(request);
	},
} satisfies ExportedHandler<Env>;
