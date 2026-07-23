/** Kigumi's fixed Pi bridge. Extensions are trusted host code; tools are workspace scoped. */
import { StringEnum } from "@earendil-works/pi-ai";
import {
	createEditTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	createWriteTool,
	type ExtensionAPI,
} from "@earendil-works/pi-coding-agent";
import { isAbsolute, posix, win32 } from "node:path";
import { Type } from "typebox";

const ROOT_TOOLS = ["read", "write", "edit", "grep", "find", "ls"] as const;
const RESERVED = new Set(["bash", "shell", "terminal"]);
const TOOL_FACTORIES = {
	read: createReadTool,
	write: createWriteTool,
	edit: createEditTool,
	grep: createGrepTool,
	find: createFindTool,
	ls: createLsTool,
};

function checkedPath(raw: unknown): string {
	if (typeof raw !== "string" || raw.length === 0 || raw.includes("\\") || raw.includes("\0")) {
		throw new Error("Kigumi tool paths must be non-empty POSIX paths");
	}
	if (isAbsolute(raw) || posix.isAbsolute(raw) || win32.isAbsolute(raw)) {
		throw new Error("Kigumi tools reject absolute paths");
	}
	if (raw.split("/").includes("..")) {
		throw new Error("Kigumi tools reject parent traversal");
	}
	return raw;
}

function jsonSafe(value: unknown): boolean {
	try {
		JSON.stringify(value);
		return true;
	} catch {
		return false;
	}
}

export default function (pi: ExtensionAPI) {
	const root = process.env.KIGUMI_WORKSPACE;
	if (!root) throw new Error("KIGUMI_WORKSPACE is required");
	const allowed = new Set(
		(process.env.KIGUMI_ALLOWED_TOOLS ?? "")
			.split(",")
			.map((name) => name.trim())
			.filter(Boolean),
	);
	const evidence: unknown[] = [];
	let submitted = false;

	pi.events.on("kigumi:evidence", (item) => {
		if (item === null || typeof item !== "object" || Array.isArray(item) || !jsonSafe(item)) {
			throw new Error("kigumi:evidence must be a JSON-serializable object");
		}
		evidence.push(item);
	});

	for (const name of ROOT_TOOLS) {
		if (!allowed.has(name)) continue;
		const tool = TOOL_FACTORIES[name](root);
		pi.registerTool({
			...tool,
			async execute(id, params, signal, onUpdate, ctx) {
				const input = { ...params } as Record<string, unknown>;
				input.path = checkedPath(input.path ?? ".");
				return tool.execute(id, input, signal, onUpdate, ctx);
			},
		});
	}

	pi.on("tool_call", (event) => {
		if (RESERVED.has(event.toolName)) {
			return { block: true, reason: "Kigumi disables generic shell tools" };
		}
		if (event.toolName !== "submit_result" && !allowed.has(event.toolName)) {
			return { block: true, reason: `Undeclared Kigumi tool: ${event.toolName}` };
		}
		if ((ROOT_TOOLS as readonly string[]).includes(event.toolName)) {
			try {
				checkedPath((event.input as Record<string, unknown>).path ?? ".");
			} catch (error) {
				return { block: true, reason: String(error) };
			}
		}
		return undefined;
	});

	pi.registerTool({
		name: "submit_result",
		label: "Submit result",
		description:
			"Submit the final Kigumi result exactly once. Every output must be a collected relative path.",
		promptSnippet: "Submit a validated final result and terminate the agent run",
		promptGuidelines: [
			"Use submit_result exactly once as the final action after all declared outputs exist.",
		],
		parameters: Type.Object(
			{
				status: StringEnum(["completed"] as const),
				summary: Type.String({ minLength: 1 }),
				outputs: Type.Array(Type.String()),
				metrics: Type.Record(Type.String(), Type.Unknown()),
			},
			{ additionalProperties: false },
		),
		async execute(_id, params) {
			if (submitted) throw new Error("submit_result may only be called once");
			const outputs = params.outputs.map(checkedPath);
			if (new Set(outputs).size !== outputs.length) {
				throw new Error("submit_result outputs must be unique");
			}
			const completion = {
				status: "completed" as const,
				summary: params.summary,
				outputs,
				metrics: params.metrics,
			};
			const gate: { completion: typeof completion; evidence: unknown[]; rejected?: string } = {
				completion,
				evidence,
			};
			pi.events.emit("kigumi:submit", gate);
			if (gate.rejected) throw new Error(`Kigumi Hook rejected completion: ${gate.rejected}`);
			submitted = true;
			return {
				content: [{ type: "text", text: "Kigumi result submitted" }],
				details: { completion: gate.completion, evidence: gate.evidence },
				terminate: true,
			};
		},
	});
}
