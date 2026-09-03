import { DurableObject } from "cloudflare:workers";

export class MyDurableObject extends DurableObject<Env> {
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}
}

export default {} satisfies ExportedHandler<Env>;
