import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import { StringEnum } from "@earendil-works/pi-ai";
import { type ExtensionAPI, type ExtensionContext, getAgentDir } from "@earendil-works/pi-coding-agent";
import { type Static, Type } from "typebox";
import {
	type AssignmentRecord,
	type AssignmentStatus,
	createPoolPaths,
	DEFAULT_HEARTBEAT_MS,
	DEFAULT_POLL_MS,
	deleteSession,
	ensurePoolPaths,
	isFresh,
	MAX_REPORT_BYTES,
	type MonitorRecord,
	makeId,
	normalizeCwd,
	POOL_PROTOCOL_VERSION,
	type PoolEvent,
	type PoolMeta,
	type PoolMode,
	type PoolPaths,
	readAssignment,
	readAssignmentRecords,
	readCommands,
	readEvents,
	readMonitor,
	readMonitorRecords,
	readSession,
	readSessionRecords,
	removeFile,
	type SessionRecord,
	type SessionStatus,
	truncateUtf8,
	type WakePolicy,
	writeAssignment,
	writeCommand,
	writeEvent,
	writeMonitor,
	writeSession,
} from "./store.ts";

const EXTENSION_PATH = fileURLToPath(import.meta.url);
const META_ENTRY = "session-pool-meta";
const MODE_ENTRY = "session-pool-mode";
const READY_ENTRY = "session-pool-ready";
const CHILD_ENV = "PI_SESSION_POOL_CHILD";
const COORDINATOR_ENV = "PI_SESSION_POOL_COORDINATOR_ID";
const SPAWN_TOKEN_ENV = "PI_SESSION_POOL_SPAWN_TOKEN";
const ROOT_ENV = "PI_SESSION_POOL_DIR";
const MAX_AUTO_CHILDREN = 8;
const SPAWN_HANDSHAKE_TIMEOUT_MS = 15_000;
const ASSIGNMENT_ACK_TIMEOUT_MS = 8_000;
const MAX_WIDGET_SESSIONS = 12;

const PoolModeSchema = StringEnum(["off", "visible", "headless"] as const);
const DeliverySchema = StringEnum(["steer", "followUp"] as const);
const WakePolicySchema = StringEnum(["queue", "silent", "wake"] as const);
const ReportKindSchema = StringEnum(["progress", "needs_attention", "needs_decision", "failed"] as const);
const ThinkingLevelSchema = StringEnum(["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const);
const PoolActionSchema = StringEnum([
	"list",
	"status",
	"spawn",
	"assign",
	"send",
	"abort",
	"monitor_start",
	"monitor_status",
	"monitor_cancel",
	"update_meta",
	"release",
	"stop",
	"report",
] as const);

const PoolParamsSchema = Type.Object({
	action: PoolActionSchema,
	sessionId: Type.Optional(Type.String()),
	assignmentId: Type.Optional(Type.String()),
	assignmentIds: Type.Optional(Type.Array(Type.String())),
	monitorId: Type.Optional(Type.String()),
	mode: Type.Optional(PoolModeSchema),
	delivery: Type.Optional(DeliverySchema),
	wakePolicy: Type.Optional(WakePolicySchema),
	reportKind: Type.Optional(ReportKindSchema),
	provider: Type.Optional(Type.String()),
	model: Type.Optional(Type.String()),
	thinkingLevel: Type.Optional(ThinkingLevelSchema),
	// "effort" is the model-facing name; Pi's CLI still calls this thinking.
	effort: Type.Optional(ThinkingLevelSchema),
	displayName: Type.Optional(Type.String()),
	role: Type.Optional(Type.String()),
	task: Type.Optional(Type.String()),
	worktree: Type.Optional(Type.String()),
	notes: Type.Optional(Type.String()),
	prompt: Type.Optional(Type.String()),
	message: Type.Optional(Type.String()),
	content: Type.Optional(Type.String()),
	timeoutMs: Type.Optional(Type.Number()),
	force: Type.Optional(Type.Boolean()),
});

type PoolParams = Static<typeof PoolParamsSchema>;
type MetaField = keyof PoolMeta;
type MetaSource = "user" | "coordinator";

interface SpawnedChild {
	process: ChildProcess;
	sessionId: string;
	mode: "visible" | "headless";
	stderr: string;
}

interface Runtime {
	ctx?: ExtensionContext;
	sessionId?: string;
	cwd?: string;
	mode: PoolMode;
	manualReady: boolean;
	managed: boolean;
	ownerId?: string;
	spawnToken?: string;
	meta: PoolMeta;
	userOverrides: Set<MetaField>;
	status: SessionStatus;
	activeAssignmentId?: string;
	cancelRequested?: { assignmentId: string; reason: "abort" | "stop" };
	sessionStartedAt?: number;
	lastAssistantText: string;
	lastAssistantStopReason?: string;
	pollTimer?: NodeJS.Timeout;
	heartbeatTimer?: NodeJS.Timeout;
	pollInFlight: boolean;
	lastWidget?: string;
	spawned: Map<string, SpawnedChild>;
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string | undefined {
	return typeof value === "string" ? value : undefined;
}

export function requestedEffort(value: { effort?: unknown; thinkingLevel?: unknown }): string | undefined {
	return stringValue(value.effort) ?? stringValue(value.thinkingLevel);
}

export function parseCommandWords(args: string): string[] {
	const words: string[] = [];
	let word = "";
	let quote: '"' | "'" | undefined;

	const push = () => {
		if (word) words.push(word);
		word = "";
	};

	for (let index = 0; index < args.length; index++) {
		const character = args[index];
		if (quote) {
			if (character === quote) {
				quote = undefined;
			} else if (character === "\\" && (args[index + 1] === quote || args[index + 1] === "\\")) {
				word += args[++index];
			} else {
				word += character;
			}
		} else if (character === '"' || character === "'") {
			quote = character;
		} else if (/\s/.test(character)) {
			push();
		} else if (
			character === "\\" &&
			(args[index + 1] === '"' || args[index + 1] === "'" || args[index + 1] === "\\")
		) {
			word += args[++index];
		} else {
			word += character;
		}
	}
	push();
	return words;
}

function readMeta(value: unknown): PoolMeta {
	if (!isObject(value)) return {};
	const fields: MetaField[] = ["displayName", "role", "task", "model", "worktree", "notes"];
	const meta: PoolMeta = {};
	for (const field of fields) {
		const item = stringValue(value[field]);
		if (item !== undefined) meta[field] = item;
	}
	return meta;
}

function patchFromParams(params: PoolParams): PoolMeta {
	const patch: PoolMeta = {};
	const fields: MetaField[] = ["displayName", "role", "task", "model", "worktree", "notes"];
	for (const field of fields) {
		const value = params[field];
		if (typeof value === "string") patch[field] = value;
	}
	return patch;
}

function patchFromPayload(payload: Record<string, unknown>): PoolMeta {
	return readMeta(payload);
}

function extractText(message: { content: readonly { type: string; text?: string }[] }): string {
	return message.content
		.filter((part): part is { type: "text"; text: string } => part.type === "text" && typeof part.text === "string")
		.map((part) => part.text)
		.join("\n");
}

function parseStoredState(ctx: ExtensionContext): {
	meta: PoolMeta;
	overrides: Set<MetaField>;
	mode: PoolMode;
	ready: boolean;
} {
	let meta: PoolMeta = {};
	let overrides = new Set<MetaField>();
	let mode: PoolMode = "off";
	let ready = false;
	for (const entry of ctx.sessionManager.getBranch()) {
		if (entry.type !== "custom") continue;
		if (entry.customType === META_ENTRY && isObject(entry.data)) {
			meta = readMeta(entry.data.meta);
			const storedOverrides = entry.data.userOverrides;
			overrides = new Set(
				Array.isArray(storedOverrides)
					? storedOverrides.filter(
							(item): item is MetaField =>
								typeof item === "string" &&
								["displayName", "role", "task", "model", "worktree", "notes"].includes(item),
						)
					: [],
			);
		}
		if (entry.customType === MODE_ENTRY && isObject(entry.data)) {
			const storedMode = entry.data.mode;
			if (storedMode === "off" || storedMode === "visible" || storedMode === "headless") mode = storedMode;
		}
		if (entry.customType === READY_ENTRY && isObject(entry.data) && typeof entry.data.ready === "boolean") {
			ready = entry.data.ready;
		}
	}
	return { meta, overrides, mode, ready };
}

function getPiInvocation(args: string[]): { command: string; args: string[] } {
	const currentScript = process.argv[1];
	const isBunVirtualScript = currentScript?.startsWith("/$bunfs/root/");
	if (currentScript && !isBunVirtualScript && fs.existsSync(currentScript)) {
		return { command: process.execPath, args: [currentScript, ...args] };
	}
	const executableName = path.basename(process.execPath).toLowerCase();
	if (!/^(node|bun)(\.exe)?$/.test(executableName)) return { command: process.execPath, args };
	return { command: "pi", args };
}

function sleep(delayMs: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function publicSession(record: SessionRecord): Record<string, unknown> {
	return {
		sessionId: record.sessionId,
		pid: record.pid,
		cwd: record.cwd,
		transport: record.transport,
		ready: record.ready,
		managed: record.managed,
		ownerId: record.ownerId,
		status: record.status,
		activeAssignmentId: record.activeAssignmentId,
		meta: record.meta,
		heartbeatAt: record.heartbeatAt,
		sessionName: record.sessionName,
	};
}

function publicAssignment(record: AssignmentRecord): Record<string, unknown> {
	return {
		assignmentId: record.assignmentId,
		sessionId: record.sessionId,
		role: record.role,
		task: record.task,
		status: record.status,
		monitorId: record.monitorId,
		result: record.result,
		error: record.error,
		updatedAt: record.updatedAt,
	};
}

function publicMonitor(record: MonitorRecord, assignments: AssignmentRecord[]): Record<string, unknown> {
	return {
		...record,
		assignments: assignments.map(publicAssignment),
	};
}

function notify(ctx: ExtensionContext, message: string, level: "info" | "warning" | "error" = "info"): void {
	if (ctx.hasUI) ctx.ui.notify(message, level);
	else console.error(`[session-pool] ${message}`);
}

function contextIsIdle(ctx: ExtensionContext): boolean {
	return ctx.isIdle() && !ctx.hasPendingMessages();
}

function isThinkingLevel(value: string): value is ThinkingLevel {
	return ["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(value);
}

export default function sessionPool(pi: ExtensionAPI): void {
	const configuredRoot = process.env[ROOT_ENV]?.trim();
	const paths: PoolPaths = createPoolPaths(
		configuredRoot ? path.resolve(configuredRoot) : path.join(getAgentDir(), "session-pool"),
	);
	ensurePoolPaths(paths);

	const runtime: Runtime = {
		mode: "off",
		manualReady: false,
		managed: process.env[CHILD_ENV] === "1",
		meta: {},
		userOverrides: new Set<MetaField>(),
		status: "idle",
		lastAssistantText: "",
		pollInFlight: false,
		spawned: new Map(),
	};

	function saveMeta(): void {
		if (!runtime.ctx) return;
		pi.appendEntry(META_ENTRY, { meta: runtime.meta, userOverrides: Array.from(runtime.userOverrides) });
	}

	function saveMode(): void {
		pi.appendEntry(MODE_ENTRY, { mode: runtime.mode });
	}

	function saveReady(): void {
		pi.appendEntry(READY_ENTRY, { ready: runtime.manualReady });
	}

	function applyMetaPatch(patch: PoolMeta, source: MetaSource, force = false): void {
		const fields: MetaField[] = ["displayName", "role", "task", "model", "worktree", "notes"];
		for (const field of fields) {
			const value = patch[field];
			if (value === undefined) continue;
			if (source === "coordinator" && !force && runtime.userOverrides.has(field)) continue;
			runtime.meta[field] = value;
			if (source === "user") runtime.userOverrides.add(field);
		}
		saveMeta();
	}

	function writeCurrentState(): void {
		const ctx = runtime.ctx;
		const sessionId = runtime.sessionId;
		const cwd = runtime.cwd;
		if (!ctx || !sessionId || !cwd) return;
		const now = Date.now();
		const record: SessionRecord = {
			version: POOL_PROTOCOL_VERSION,
			sessionId,
			pid: process.pid,
			cwd,
			cwdKey: normalizeCwd(cwd),
			transport: ctx.mode === "tui" ? "tui" : "rpc",
			ready: runtime.manualReady || runtime.managed,
			managed: runtime.managed,
			...(runtime.ownerId ? { ownerId: runtime.ownerId } : {}),
			...(runtime.spawnToken ? { spawnToken: runtime.spawnToken } : {}),
			status: runtime.status,
			...(runtime.activeAssignmentId ? { activeAssignmentId: runtime.activeAssignmentId } : {}),
			meta: { ...runtime.meta },
			userOverrides: Array.from(runtime.userOverrides),
			heartbeatAt: now,
			startedAt: runtime.sessionStartedAt ?? now,
			updatedAt: now,
			...(ctx.sessionManager.getSessionFile() ? { sessionFile: ctx.sessionManager.getSessionFile() } : {}),
			...(pi.getSessionName() ? { sessionName: pi.getSessionName() } : {}),
		};
		writeSession(paths, record);
	}

	function ownerIsFresh(ownerId: string): boolean {
		const owner = readSession(paths, ownerId);
		return owner !== undefined && isFresh(owner);
	}

	function ownerAllowsCoordinator(managed: boolean, ownerId: string | undefined, coordinatorId: string): boolean {
		if (managed) return ownerId === coordinatorId;
		return ownerId === undefined || ownerId === coordinatorId || !ownerIsFresh(ownerId);
	}

	function clearExpiredOwner(): void {
		if (runtime.ownerId && !ownerIsFresh(runtime.ownerId)) {
			runtime.ownerId = undefined;
			runtime.spawnToken = undefined;
		}
	}

	function setWidgetText(text: string): void {
		const ctx = runtime.ctx;
		if (!ctx || ctx.mode !== "tui" || runtime.lastWidget === text) return;
		runtime.lastWidget = text;
		if (!text) {
			ctx.ui.setWidget("session-pool", undefined);
			ctx.ui.setStatus("session-pool", undefined);
			return;
		}
		ctx.ui.setWidget("session-pool", text.split("\n"));
		ctx.ui.setStatus("session-pool", text.split("\n")[0]);
	}

	function updateWidget(): void {
		const ctx = runtime.ctx;
		const sessionId = runtime.sessionId;
		const cwd = runtime.cwd;
		if (!ctx || !sessionId || !cwd || ctx.mode !== "tui") return;
		const active =
			runtime.mode !== "off" ||
			runtime.manualReady ||
			runtime.managed ||
			readMonitorRecords(paths, sessionId).some((monitor) => monitor.state === "active");
		if (!active) {
			setWidgetText("");
			return;
		}
		const cwdKey = normalizeCwd(cwd);
		const sessions = readSessionRecords(paths)
			.filter((record) => record.cwdKey === cwdKey && isFresh(record))
			.sort((a, b) => b.updatedAt - a.updatedAt)
			.slice(0, MAX_WIDGET_SESSIONS);
		const modeLabel = runtime.mode === "off" ? "ready" : runtime.mode;
		const lines = [`session pool [${modeLabel}]`];
		for (const record of sessions) {
			const name = (record.meta.displayName || record.sessionName || record.sessionId.slice(0, 8)).replace(
				/\s+/g,
				" ",
			);
			const role = record.meta.role ? ` ${record.meta.role.replace(/\s+/g, " ")}` : "";
			const marker = record.status === "running" ? "*" : record.status === "stopping" ? "!" : "o";
			const owner = record.ownerId && record.ownerId !== sessionId ? " claimed" : "";
			lines.push(`${marker} ${name}${role} ${record.status}${owner}`);
			if (record.ownerId === sessionId || record.sessionId === sessionId) {
				if (record.meta.task) lines.push(`  task: ${record.meta.task.replace(/\s+/g, " ").slice(0, 100)}`);
				const details = [
					record.meta.model ? `model=${record.meta.model.replace(/\s+/g, " ")}` : undefined,
					record.meta.worktree ? `worktree=${record.meta.worktree.replace(/\s+/g, " ")}` : undefined,
					record.meta.notes ? `notes=${record.meta.notes.replace(/\s+/g, " ")}` : undefined,
				].filter((value): value is string => value !== undefined);
				if (details.length > 0) lines.push(`  ${details.join(" | ").slice(0, 140)}`);
			}
		}
		const monitors = readMonitorRecords(paths, sessionId).filter((monitor) => monitor.state === "active");
		if (monitors.length > 0) lines.push(`monitors: ${monitors.length} active`);
		setWidgetText(lines.join("\n"));
	}

	// Keep the registry live without holding the model turn open.
	function startTimers(): void {
		if (!runtime.pollTimer) {
			runtime.pollTimer = setInterval(() => {
				void poll();
			}, DEFAULT_POLL_MS);
			runtime.pollTimer.unref();
		}
		if (!runtime.heartbeatTimer) {
			runtime.heartbeatTimer = setInterval(() => {
				if (runtime.managed && runtime.ownerId && !ownerIsFresh(runtime.ownerId)) {
					const ctx = runtime.ctx;
					if (ctx && runtime.status !== "stopping") {
						runtime.status = "stopping";
						writeCurrentState();
						ctx.shutdown();
					}
					return;
				}
				writeCurrentState();
				updateWidget();
			}, DEFAULT_HEARTBEAT_MS);
			runtime.heartbeatTimer.unref();
		}
	}

	function stopTimers(): void {
		if (runtime.pollTimer) clearInterval(runtime.pollTimer);
		if (runtime.heartbeatTimer) clearInterval(runtime.heartbeatTimer);
		runtime.pollTimer = undefined;
		runtime.heartbeatTimer = undefined;
	}

	function startSession(ctx: ExtensionContext): void {
		if (runtime.sessionId === ctx.sessionManager.getSessionId() && runtime.ctx) {
			runtime.ctx = ctx;
			return;
		}
		stopTimers();
		runtime.ctx = ctx;
		runtime.sessionId = ctx.sessionManager.getSessionId();
		runtime.cwd = ctx.cwd;
		runtime.sessionStartedAt = Date.now();
		runtime.managed = process.env[CHILD_ENV] === "1";
		runtime.ownerId = process.env[COORDINATOR_ENV]?.trim() || undefined;
		runtime.spawnToken = process.env[SPAWN_TOKEN_ENV]?.trim() || undefined;
		runtime.status = contextIsIdle(ctx) ? "idle" : "running";
		runtime.activeAssignmentId = undefined;
		runtime.cancelRequested = undefined;
		runtime.lastAssistantText = "";
		runtime.lastAssistantStopReason = undefined;
		const stored = parseStoredState(ctx);
		runtime.meta = stored.meta;
		runtime.userOverrides = stored.overrides;
		runtime.mode = runtime.managed ? "off" : stored.mode;
		runtime.manualReady = runtime.managed || stored.ready;
		writeCurrentState();
		startTimers();
		updateWidget();
	}

	function ensureStarted(ctx: ExtensionContext): void {
		if (runtime.sessionId !== ctx.sessionManager.getSessionId() || !runtime.ctx) startSession(ctx);
		runtime.ctx = ctx;
	}

	function stopManagedChildren(coordinatorId: string): void {
		for (const record of readSessionRecords(paths)) {
			if (record.managed && record.ownerId === coordinatorId && isFresh(record)) {
				writeCommand(paths, record.sessionId, coordinatorId, "stop", {});
			}
		}
		for (const child of runtime.spawned.values()) {
			if (child.mode === "headless") child.process.kill();
		}
		runtime.spawned.clear();
	}

	function shutdownSession(ctx: ExtensionContext): void {
		if (runtime.sessionId && !runtime.managed) stopManagedChildren(runtime.sessionId);
		if (runtime.sessionId && runtime.managed && runtime.ownerId) {
			writeEvent(paths, {
				kind: "exited",
				sessionId: runtime.sessionId,
				coordinatorId: runtime.ownerId,
				...(runtime.activeAssignmentId ? { assignmentId: runtime.activeAssignmentId } : {}),
				...(runtime.cancelRequested ? { status: "cancelled", failed: false } : {}),
				error: runtime.cancelRequested?.reason === "stop" ? "child session stopped" : "child session shut down",
			});
		}
		if (runtime.sessionId) deleteSession(paths, runtime.sessionId);
		if (ctx.mode === "tui") {
			ctx.ui.setWidget("session-pool", undefined);
			ctx.ui.setStatus("session-pool", undefined);
		}
		stopTimers();
		runtime.ctx = undefined;
		runtime.sessionId = undefined;
		runtime.cwd = undefined;
		runtime.sessionStartedAt = undefined;
	}

	function commandValue(payload: Record<string, unknown>, key: string): string | undefined {
		return stringValue(payload[key]);
	}

	function commandBoolean(payload: Record<string, unknown>, key: string): boolean {
		return payload[key] === true;
	}

	function commandMeta(payload: Record<string, unknown>): PoolMeta {
		const nested = payload.meta;
		return isObject(nested) ? patchFromPayload(nested) : patchFromPayload(payload);
	}

	function isAuthorized(command: { coordinatorId: string }): boolean {
		if (!runtime.managed && !runtime.manualReady) return false;
		return ownerAllowsCoordinator(runtime.managed, runtime.ownerId, command.coordinatorId);
	}

	function emitCommandRejection(
		command: { coordinatorId: string },
		sessionId: string,
		error: string,
		assignmentId?: string,
	): void {
		writeEvent(paths, {
			kind: "rejected",
			sessionId,
			coordinatorId: command.coordinatorId,
			...(assignmentId ? { assignmentId } : {}),
			error,
		});
	}

	async function applyRequestedModel(
		payload: Record<string, unknown>,
		ctx: ExtensionContext,
	): Promise<string | undefined> {
		let provider = commandValue(payload, "provider");
		let modelId = commandValue(payload, "model");
		if (modelId && !provider) {
			const separator = modelId.indexOf("/");
			if (separator > 0) {
				provider = modelId.slice(0, separator);
				modelId = modelId.slice(separator + 1);
			}
		}
		let modelLabel: string | undefined;
		if (provider && modelId) {
			const model = ctx.modelRegistry.find(provider, modelId);
			if (!model) throw new Error(`Model not available: ${provider}/${modelId}`);
			if (!(await pi.setModel(model)))
				throw new Error(`Model authentication is unavailable: ${provider}/${modelId}`);
			modelLabel = `${provider}/${modelId}`;
		}
		const thinking = requestedEffort(payload);
		if (thinking) {
			if (!isThinkingLevel(thinking)) throw new Error(`Invalid thinking level: ${thinking}`);
			pi.setThinkingLevel(thinking);
		}
		return modelLabel;
	}

	async function handleAssign(command: {
		coordinatorId: string;
		sessionId: string;
		payload: Record<string, unknown>;
	}): Promise<void> {
		const assignmentId = commandValue(command.payload, "assignmentId");
		const prompt = commandValue(command.payload, "prompt");
		const sessionId = runtime.sessionId;
		const ctx = runtime.ctx;
		if (!sessionId || !ctx || !assignmentId || prompt === undefined) {
			emitCommandRejection(
				command,
				sessionId ?? command.sessionId,
				"assign requires assignmentId and prompt",
				assignmentId,
			);
			return;
		}
		if (!isAuthorized(command)) {
			emitCommandRejection(
				command,
				sessionId,
				"session is not ready or is owned by another coordinator",
				assignmentId,
			);
			return;
		}
		if (runtime.activeAssignmentId || !contextIsIdle(ctx)) {
			emitCommandRejection(command, sessionId, "session is busy", assignmentId);
			return;
		}
		try {
			const modelLabel = await applyRequestedModel(command.payload, ctx);
			if (!isAuthorized(command) || runtime.activeAssignmentId || !contextIsIdle(ctx))
				throw new Error("session became unavailable while applying assignment settings");
			const requestedMeta = commandMeta(command.payload);
			if (modelLabel && requestedMeta.model === undefined) requestedMeta.model = modelLabel;
			applyMetaPatch(requestedMeta, "coordinator", commandBoolean(command.payload, "force"));
			const previousOwner = runtime.ownerId;
			if (previousOwner !== undefined && previousOwner !== command.coordinatorId) clearExpiredOwner();
			runtime.ownerId = command.coordinatorId;
			if (previousOwner !== command.coordinatorId) runtime.spawnToken = undefined;
			runtime.activeAssignmentId = assignmentId;
			runtime.cancelRequested = undefined;
			runtime.lastAssistantText = "";
			runtime.lastAssistantStopReason = undefined;
			runtime.status = "running";
			writeCurrentState();
			await pi.sendUserMessage(prompt);
			writeEvent(paths, {
				kind: "accepted",
				sessionId,
				coordinatorId: command.coordinatorId,
				assignmentId,
				status: "running",
			});
		} catch (error) {
			runtime.activeAssignmentId = undefined;
			runtime.status = "idle";
			writeCurrentState();
			emitCommandRejection(command, sessionId, error instanceof Error ? error.message : String(error), assignmentId);
		}
	}

	async function handleSend(command: {
		coordinatorId: string;
		sessionId: string;
		payload: Record<string, unknown>;
	}): Promise<void> {
		const ctx = runtime.ctx;
		const sessionId = runtime.sessionId;
		const message = commandValue(command.payload, "message");
		if (!ctx || !sessionId || message === undefined) {
			emitCommandRejection(command, sessionId ?? command.sessionId, "send requires message");
			return;
		}
		if (!isAuthorized(command)) {
			emitCommandRejection(command, sessionId, "session is not owned by this coordinator");
			return;
		}
		if (!runtime.activeAssignmentId) {
			emitCommandRejection(command, sessionId, "session has no active assignment; use assign for new work");
			return;
		}
		try {
			const delivery = commandValue(command.payload, "delivery");
			if (contextIsIdle(ctx)) await pi.sendUserMessage(message);
			else await pi.sendUserMessage(message, { deliverAs: delivery === "followUp" ? "followUp" : "steer" });
		} catch (error) {
			emitCommandRejection(command, sessionId, error instanceof Error ? error.message : String(error));
		}
	}

	function handleAbort(command: { coordinatorId: string; sessionId: string }): void {
		if (!runtime.ctx || !runtime.sessionId) return;
		if (!isAuthorized(command)) {
			emitCommandRejection(command, runtime.sessionId, "session is not owned by this coordinator");
			return;
		}
		if (!runtime.activeAssignmentId) {
			emitCommandRejection(command, runtime.sessionId, "session has no active assignment");
			return;
		}
		runtime.cancelRequested = { assignmentId: runtime.activeAssignmentId, reason: "abort" };
		runtime.status = "stopping";
		writeCurrentState();
		runtime.ctx.abort();
	}

	function handleRelease(command: { coordinatorId: string; sessionId: string }): void {
		if (!runtime.sessionId) return;
		if (!isAuthorized(command)) {
			emitCommandRejection(command, runtime.sessionId, "session is not owned by this coordinator");
			return;
		}
		if (runtime.activeAssignmentId) {
			emitCommandRejection(command, runtime.sessionId, "cannot release a running assignment");
			return;
		}
		const coordinatorId = command.coordinatorId;
		runtime.ownerId = undefined;
		runtime.spawnToken = undefined;
		writeCurrentState();
		writeEvent(paths, { kind: "released", sessionId: runtime.sessionId, coordinatorId });
	}

	function handleStop(command: { coordinatorId: string; sessionId: string }): void {
		const ctx = runtime.ctx;
		if (!ctx || !runtime.sessionId) return;
		if (!isAuthorized(command)) {
			emitCommandRejection(command, runtime.sessionId, "session is not owned by this coordinator");
			return;
		}
		const assignmentId = runtime.activeAssignmentId;
		runtime.cancelRequested = assignmentId ? { assignmentId, reason: "stop" } : undefined;
		runtime.status = "stopping";
		writeCurrentState();
		writeEvent(paths, {
			kind: "stopped",
			sessionId: runtime.sessionId,
			coordinatorId: command.coordinatorId,
			...(assignmentId ? { assignmentId, status: "cancelled", failed: false } : {}),
		});
		setTimeout(() => ctx.shutdown(), 0);
	}

	function handleMeta(command: { coordinatorId: string; sessionId: string; payload: Record<string, unknown> }): void {
		if (!runtime.sessionId) return;
		if (!isAuthorized(command)) {
			emitCommandRejection(command, runtime.sessionId, "session is not owned by this coordinator");
			return;
		}
		applyMetaPatch(commandMeta(command.payload), "coordinator", commandBoolean(command.payload, "force"));
		writeCurrentState();
	}

	async function processCommands(): Promise<void> {
		const sessionId = runtime.sessionId;
		if (!sessionId) return;
		for (const item of readCommands(paths, sessionId)) {
			try {
				switch (item.command.type) {
					case "assign":
						await handleAssign(item.command);
						break;
					case "send":
						await handleSend(item.command);
						break;
					case "abort":
						handleAbort(item.command);
						break;
					case "release":
						handleRelease(item.command);
						break;
					case "stop":
						handleStop(item.command);
						break;
					case "meta":
						handleMeta(item.command);
						break;
				}
			} finally {
				removeFile(item.filePath);
			}
		}
	}

	function assignmentIsTerminal(status: AssignmentStatus): boolean {
		return status === "completed" || status === "failed" || status === "cancelled";
	}

	function assignmentRecordsForMonitor(monitor: MonitorRecord): AssignmentRecord[] {
		return monitor.assignmentIds
			.map((assignmentId) => readAssignment(paths, assignmentId))
			.filter((record): record is AssignmentRecord => record !== undefined);
	}

	function sessionDisplayName(sessionId: string): string {
		const record = readSession(paths, sessionId);
		return record?.meta.displayName || record?.sessionName || sessionId.slice(0, 8);
	}

	function queueMainMessage(content: string, wakePolicy: WakePolicy, details: Record<string, unknown>): void {
		const ctx = runtime.ctx;
		if (!ctx || wakePolicy === "silent") return;
		const shouldWake = wakePolicy === "wake";
		try {
			pi.sendMessage(
				{
					customType: "session-pool",
					content: truncateUtf8(content, MAX_REPORT_BYTES),
					display: true,
					details,
				},
				{
					deliverAs: shouldWake ? "steer" : "nextTurn",
					triggerTurn: shouldWake,
				},
			);
		} catch (error) {
			notify(
				ctx,
				`Could not deliver monitor message: ${error instanceof Error ? error.message : String(error)}`,
				"warning",
			);
		}
	}

	function monitorTransitionMessage(monitor: MonitorRecord, assignments: AssignmentRecord[]): string {
		const completed = assignments.filter((assignment) => assignment.status === "completed").length;
		const failed = assignments.filter((assignment) => assignment.status === "failed").length;
		const cancelled = assignments.filter((assignment) => assignment.status === "cancelled").length;
		const pending = assignments.length - completed - failed - cancelled;
		const lines = [
			`monitor ${monitor.monitorId}: ${monitor.state}`,
			`completed=${completed} failed=${failed} cancelled=${cancelled} pending=${Math.max(0, pending)}`,
		];
		for (const assignment of assignments) {
			const label = assignment.role || sessionDisplayName(assignment.sessionId);
			const result = assignment.error || assignment.result;
			lines.push(`- ${label}: ${assignment.status}${result ? ` - ${result.slice(0, 300)}` : ""}`);
		}
		return lines.join("\n");
	}

	function reconcileMonitor(monitor: MonitorRecord, now = Date.now()): MonitorRecord {
		if (monitor.state !== "active") return monitor;
		const assignments = assignmentRecordsForMonitor(monitor);
		let nextState: MonitorRecord["state"] = monitor.state;
		if (now >= monitor.deadlineAt) nextState = "timed_out";
		else if (
			assignments.length === monitor.assignmentIds.length &&
			assignments.every((assignment) => assignmentIsTerminal(assignment.status))
		)
			nextState = "completed";
		if (nextState === monitor.state) return monitor;
		const nextMonitor = { ...monitor, state: nextState };
		writeMonitor(paths, nextMonitor);
		queueMainMessage(monitorTransitionMessage(nextMonitor, assignments), monitor.wakePolicy, {
			monitorId: monitor.monitorId,
			state: nextState,
		});
		return nextMonitor;
	}

	function queueReportEvent(event: PoolEvent, monitor: MonitorRecord | undefined): void {
		if (!event.content || event.reportKind === "progress") return;
		const wakePolicy = monitor?.wakePolicy ?? "queue";
		const label =
			event.reportKind === "needs_decision"
				? "needs decision"
				: event.reportKind === "needs_attention"
					? "needs attention"
					: "failed";
		const content = `[session-pool] ${sessionDisplayName(event.sessionId)} ${label}:\n${event.content}`;
		queueMainMessage(content, wakePolicy, {
			monitorId: monitor?.monitorId,
			assignmentId: event.assignmentId,
			reportKind: event.reportKind,
		});
		if (event.reportKind === "needs_attention" || event.reportKind === "needs_decision") {
			const ctx = runtime.ctx;
			if (ctx?.hasUI) notify(ctx, `${sessionDisplayName(event.sessionId)} ${label}`, "warning");
		}
	}

	function handleCoordinatorEvent(event: PoolEvent): void {
		const assignment = event.assignmentId ? readAssignment(paths, event.assignmentId) : undefined;
		if (assignment && assignment.lastEventId === event.id) return;
		if (!assignment && event.kind === "rejected") {
			queueMainMessage(
				`[session-pool] ${sessionDisplayName(event.sessionId)} rejected a command:\n${event.error || event.content || "unknown error"}`,
				"queue",
				{ sessionId: event.sessionId },
			);
		}
		if (assignment) {
			const updated: AssignmentRecord = { ...assignment, updatedAt: Date.now(), lastEventId: event.id };
			switch (event.kind) {
				case "accepted":
					updated.status = "running";
					break;
				case "settled":
					updated.status = event.status === "cancelled" ? "cancelled" : event.failed ? "failed" : "completed";
					if (event.content !== undefined) updated.result = event.content;
					if (event.error) updated.error = event.error;
					break;
				case "rejected":
					updated.status = "failed";
					updated.error = event.error || event.content || "assignment rejected";
					break;
				case "exited":
					if (event.status !== "cancelled" && updated.status !== "cancelled") updated.status = "failed";
					updated.error = event.error || "child session exited";
					break;
				case "stopped":
					updated.status = "cancelled";
					updated.error = event.error || "child session stopped";
					break;
				case "report":
					if (event.content !== undefined) updated.result = event.content;
					break;
				default:
					break;
			}
			writeAssignment(paths, updated);
		}

		const monitors = event.assignmentId
			? readMonitorRecords(paths, runtime.sessionId ?? "").filter((monitor) =>
					monitor.assignmentIds.includes(event.assignmentId ?? ""),
				)
			: [];
		if (event.kind === "report") queueReportEvent(event, monitors[0]);
		if (event.kind === "settled" && monitors.length === 0) {
			const outcome = event.status === "cancelled" ? "cancelled" : event.failed ? "failed" : "completed";
			queueMainMessage(
				`[session-pool] ${sessionDisplayName(event.sessionId)} ${outcome}:\n${event.content || event.error || "(no output)"}`,
				"queue",
				{ assignmentId: event.assignmentId, sessionId: event.sessionId },
			);
		}
		if ((event.kind === "rejected" || event.kind === "exited" || event.kind === "stopped") && monitors.length > 0) {
			const outcome = event.kind === "stopped" || event.status === "cancelled" ? "cancelled" : "failed";
			queueMainMessage(
				`[session-pool] ${sessionDisplayName(event.sessionId)} ${outcome}:\n${event.error || event.content || "unknown error"}`,
				monitors[0].wakePolicy,
				{ monitorId: monitors[0].monitorId, assignmentId: event.assignmentId },
			);
		}
		for (const monitor of monitors) reconcileMonitor(monitor);
	}

	async function processCoordinatorEvents(): Promise<void> {
		const coordinatorId = runtime.sessionId;
		if (!coordinatorId || runtime.managed) return;
		for (const item of readEvents(paths, coordinatorId)) {
			handleCoordinatorEvent(item.event);
			removeFile(item.filePath);
		}
	}

	function processMonitorDeadlines(): void {
		const coordinatorId = runtime.sessionId;
		if (!coordinatorId || runtime.managed) return;
		for (const monitor of readMonitorRecords(paths, coordinatorId)) reconcileMonitor(monitor);
	}

	async function poll(): Promise<void> {
		if (runtime.pollInFlight || !runtime.ctx) return;
		runtime.pollInFlight = true;
		try {
			await processCommands();
			await processCoordinatorEvents();
			processMonitorDeadlines();
		} catch (error) {
			notify(
				runtime.ctx,
				`session-pool poll failed: ${error instanceof Error ? error.message : String(error)}`,
				"warning",
			);
		} finally {
			runtime.pollInFlight = false;
		}
	}

	function childArgs(childSessionId: string, mode: "visible" | "headless", params: PoolParams): string[] {
		const args = ["--session-id", childSessionId, "--extension", EXTENSION_PATH];
		if (mode === "headless") args.unshift("--mode", "rpc");
		if (params.provider) args.push("--provider", params.provider);
		if (params.model) args.push("--model", params.model);
		const effort = requestedEffort(params);
		if (effort) args.push("--thinking", effort);
		if (params.displayName) args.push("--name", params.displayName);
		return args;
	}

	function childEnv(parentId: string, token: string): NodeJS.ProcessEnv {
		return {
			...process.env,
			[CHILD_ENV]: "1",
			[COORDINATOR_ENV]: parentId,
			[SPAWN_TOKEN_ENV]: token,
			[ROOT_ENV]: paths.root,
		};
	}

	function attachChildLifecycle(child: SpawnedChild): void {
		child.process.on("error", () => {
			runtime.spawned.delete(child.sessionId);
		});
		child.process.on("close", () => {
			runtime.spawned.delete(child.sessionId);
		});
	}

	function startHeadlessChild(childSessionId: string, token: string, cwd: string, params: PoolParams): SpawnedChild {
		const parentId = runtime.sessionId;
		if (!parentId) throw new Error("Coordinator session is not initialized");
		const invocation = getPiInvocation(childArgs(childSessionId, "headless", params));
		const childProcess = spawn(invocation.command, invocation.args, {
			cwd,
			env: childEnv(parentId, token),
			stdio: ["pipe", "ignore", "pipe"],
			windowsHide: true,
		});
		const child: SpawnedChild = {
			process: childProcess,
			sessionId: childSessionId,
			mode: "headless",
			stderr: "",
		};
		childProcess.stderr?.on("data", (data: Buffer) => {
			child.stderr = truncateUtf8(`${child.stderr}${data.toString()}`, MAX_REPORT_BYTES);
		});
		attachChildLifecycle(child);
		return child;
	}

	async function startVisibleChild(
		childSessionId: string,
		token: string,
		cwd: string,
		params: PoolParams,
	): Promise<SpawnedChild> {
		if (process.platform !== "win32")
			throw new Error("visible child launch is currently supported through wt.exe on Windows");
		const parentId = runtime.sessionId;
		if (!parentId) throw new Error("Coordinator session is not initialized");
		const invocation = getPiInvocation(childArgs(childSessionId, "visible", params));
		const launcher = spawn("wt.exe", ["-w", "new", "--", invocation.command, ...invocation.args], {
			cwd,
			env: childEnv(parentId, token),
			stdio: "ignore",
			detached: true,
			windowsHide: false,
		});
		await new Promise<void>((resolve, reject) => {
			launcher.once("spawn", () => resolve());
			launcher.once("error", reject);
		});
		launcher.unref();
		const child: SpawnedChild = { process: launcher, sessionId: childSessionId, mode: "visible", stderr: "" };
		attachChildLifecycle(child);
		return child;
	}

	async function waitForChildSession(
		childSessionId: string,
		token: string,
		signal: AbortSignal | undefined,
	): Promise<SessionRecord | undefined> {
		const parentId = runtime.sessionId;
		const cwdKey = runtime.cwd ? normalizeCwd(runtime.cwd) : undefined;
		const deadline = Date.now() + SPAWN_HANDSHAKE_TIMEOUT_MS;
		while (Date.now() < deadline) {
			if (signal?.aborted) throw new Error("spawn aborted");
			const record = readSession(paths, childSessionId);
			if (
				record &&
				record.spawnToken === token &&
				record.ownerId === parentId &&
				record.cwdKey === cwdKey &&
				isFresh(record)
			)
				return record;
			await sleep(100);
		}
		return undefined;
	}

	async function spawnChild(
		params: PoolParams,
		signal: AbortSignal | undefined,
	): Promise<{ record: SessionRecord; mode: "visible" | "headless"; fallback?: string }> {
		const parentId = runtime.sessionId;
		const cwd = runtime.cwd;
		if (!parentId || !cwd) throw new Error("Coordinator session is not initialized");
		if (runtime.managed) throw new Error("child sessions cannot spawn more children");
		if (runtime.mode === "off") throw new Error("enable /pool-mode visible or /pool-mode headless first");
		const activeChildren = readSessionRecords(paths).filter(
			(record) => record.managed && record.ownerId === parentId && isFresh(record),
		);
		if (activeChildren.length >= MAX_AUTO_CHILDREN)
			throw new Error(`maximum managed child count reached (${MAX_AUTO_CHILDREN})`);
		const requestedMode = params.mode ?? runtime.mode;
		if (requestedMode === "off") throw new Error("spawn mode cannot be off");
		const childSessionId = makeId("session");
		const token = randomUUID();
		let actualMode: "visible" | "headless" = requestedMode;
		let fallback: string | undefined;
		let child: SpawnedChild;
		if (requestedMode === "visible") {
			try {
				child = await startVisibleChild(childSessionId, token, cwd, params);
			} catch (error) {
				fallback = error instanceof Error ? error.message : String(error);
				actualMode = "headless";
				child = startHeadlessChild(childSessionId, token, cwd, params);
			}
		} else {
			child = startHeadlessChild(childSessionId, token, cwd, params);
		}
		runtime.spawned.set(childSessionId, child);
		const record = await waitForChildSession(childSessionId, token, signal);
		if (!record) {
			if (child.mode === "headless") child.process.kill();
			runtime.spawned.delete(childSessionId);
			throw new Error(`child session handshake timed out${child.stderr ? `: ${child.stderr}` : ""}`);
		}
		return { record, mode: actualMode, ...(fallback ? { fallback } : {}) };
	}

	async function waitForAssignment(
		assignmentId: string,
		signal: AbortSignal | undefined,
	): Promise<AssignmentRecord | undefined> {
		const deadline = Date.now() + ASSIGNMENT_ACK_TIMEOUT_MS;
		while (Date.now() < deadline) {
			if (signal?.aborted) throw new Error("assignment aborted");
			await processCoordinatorEvents();
			const assignment = readAssignment(paths, assignmentId);
			if (assignment && assignment.status !== "pending") return assignment;
			await sleep(50);
		}
		return readAssignment(paths, assignmentId);
	}

	function currentCoordinatorId(): string {
		if (!runtime.sessionId || runtime.managed)
			throw new Error("this action is only available in a coordinator session");
		return runtime.sessionId;
	}

	function currentCwdKey(): string {
		if (!runtime.cwd) throw new Error("session cwd is not initialized");
		return normalizeCwd(runtime.cwd);
	}

	function availableSessions(): SessionRecord[] {
		const coordinatorId = currentCoordinatorId();
		return readSessionRecords(paths)
			.filter(
				(record) =>
					record.sessionId !== coordinatorId &&
					record.cwdKey === currentCwdKey() &&
					record.ready &&
					record.status === "idle" &&
					isFresh(record) &&
					ownerAllowsCoordinator(record.managed, record.ownerId, coordinatorId),
			)
			.sort((a, b) => a.updatedAt - b.updatedAt);
	}

	function allLocalSessions(): SessionRecord[] {
		const cwdKey = currentCwdKey();
		return readSessionRecords(paths)
			.filter((record) => record.cwdKey === cwdKey && isFresh(record))
			.sort((a, b) => b.updatedAt - a.updatedAt);
	}

	function toolText(
		text: string,
		details: Record<string, unknown> = {},
		isError = false,
	): {
		content: Array<{ type: "text"; text: string }>;
		details: Record<string, unknown>;
		isError?: boolean;
	} {
		return { content: [{ type: "text", text }], details, ...(isError ? { isError: true } : {}) };
	}

	async function assignSession(
		params: PoolParams,
		signal: AbortSignal | undefined,
	): Promise<ReturnType<typeof toolText>> {
		const coordinatorId = currentCoordinatorId();
		const sessionId = params.sessionId;
		const prompt = params.prompt ?? params.task;
		if (!sessionId || prompt === undefined)
			return toolText("assign requires sessionId and prompt (or task)", {}, true);
		const target = readSession(paths, sessionId);
		if (!target || target.cwdKey !== currentCwdKey() || !isFresh(target))
			return toolText(`session not available: ${sessionId}`, {}, true);
		if (
			!target.ready ||
			target.status !== "idle" ||
			!ownerAllowsCoordinator(target.managed, target.ownerId, coordinatorId)
		)
			return toolText(`session is not currently available: ${sessionId}`, {}, true);
		const assignmentId = makeId("assignment");
		const now = Date.now();
		const assignment: AssignmentRecord = {
			version: POOL_PROTOCOL_VERSION,
			assignmentId,
			sessionId,
			coordinatorId,
			prompt,
			...(params.role ? { role: params.role } : {}),
			...(params.task ? { task: params.task } : {}),
			status: "pending",
			createdAt: now,
			updatedAt: now,
		};
		writeAssignment(paths, assignment);
		writeCommand(paths, sessionId, coordinatorId, "assign", {
			assignmentId,
			prompt,
			meta: patchFromParams(params),
			...(params.provider ? { provider: params.provider } : {}),
			...(params.model ? { model: params.model } : {}),
			...(params.effort
				? { effort: params.effort }
				: params.thinkingLevel
					? { thinkingLevel: params.thinkingLevel }
					: {}),
			...(params.force ? { force: true } : {}),
		});
		const acknowledged = await waitForAssignment(assignmentId, signal);
		const result = acknowledged ?? readAssignment(paths, assignmentId) ?? assignment;
		return toolText(
			`assignment ${assignmentId}: ${result.status}${result.error ? ` - ${result.error}` : ""}`,
			{ assignment: publicAssignment(result) },
			result.status === "failed",
		);
	}

	function monitorAssignments(monitor: MonitorRecord): AssignmentRecord[] {
		return assignmentRecordsForMonitor(monitor);
	}

	function startMonitor(params: PoolParams): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		const assignmentIds = params.assignmentIds ?? (params.assignmentId ? [params.assignmentId] : []);
		if (assignmentIds.length === 0) return toolText("monitor_start requires assignmentIds", {}, true);
		const assignments = assignmentIds.map((assignmentId) => readAssignment(paths, assignmentId));
		if (assignments.some((assignment) => !assignment || assignment.coordinatorId !== coordinatorId))
			return toolText("all assignments must belong to the current coordinator", {}, true);
		const timeoutMs = params.timeoutMs;
		if (timeoutMs === undefined || !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0)
			return toolText("monitor_start requires a positive safe-integer timeoutMs", {}, true);
		const monitor: MonitorRecord = {
			version: POOL_PROTOCOL_VERSION,
			monitorId: makeId("monitor"),
			coordinatorId,
			assignmentIds,
			createdAt: Date.now(),
			deadlineAt: Date.now() + timeoutMs,
			state: "active",
			wakePolicy: params.wakePolicy ?? "queue",
		};
		writeMonitor(paths, monitor);
		for (const assignment of assignments) {
			if (!assignment) continue;
			writeAssignment(paths, { ...assignment, monitorId: monitor.monitorId, updatedAt: Date.now() });
		}
		const reconciled = reconcileMonitor(monitor);
		return toolText(`monitor ${reconciled.monitorId}: ${reconciled.state}`, {
			monitor: publicMonitor(reconciled, monitorAssignments(reconciled)),
		});
	}

	function monitorStatus(monitorId?: string): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		const monitors = monitorId
			? [readMonitor(paths, monitorId)].filter(
					(monitor): monitor is MonitorRecord => monitor?.coordinatorId === coordinatorId,
				)
			: readMonitorRecords(paths, coordinatorId);
		const reconciled = monitors.map((monitor) => reconcileMonitor(monitor));
		return toolText(
			JSON.stringify(
				reconciled.map((monitor) => publicMonitor(monitor, monitorAssignments(monitor))),
				null,
				2,
			),
			{
				monitors: reconciled.map((monitor) => publicMonitor(monitor, monitorAssignments(monitor))),
			},
		);
	}

	function cancelMonitor(monitorId?: string): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		if (!monitorId) return toolText("monitor_cancel requires monitorId", {}, true);
		const monitor = readMonitor(paths, monitorId);
		if (!monitor || monitor.coordinatorId !== coordinatorId)
			return toolText(`monitor not found: ${monitorId}`, {}, true);
		if (monitor.state === "active") writeMonitor(paths, { ...monitor, state: "cancelled" });
		return toolText(`monitor ${monitorId}: cancelled`, { monitorId, state: "cancelled" });
	}

	async function sendToSession(params: PoolParams): Promise<ReturnType<typeof toolText>> {
		const coordinatorId = currentCoordinatorId();
		if (!params.sessionId || params.message === undefined)
			return toolText("send requires sessionId and message", {}, true);
		const target = readSession(paths, params.sessionId);
		if (!target || !isFresh(target) || target.ownerId !== coordinatorId)
			return toolText("session is not owned by the current coordinator", {}, true);
		if (!target.activeAssignmentId)
			return toolText("session has no active assignment; use assign for new work", {}, true);
		writeCommand(paths, params.sessionId, coordinatorId, "send", {
			message: params.message,
			delivery: params.delivery ?? "steer",
		});
		return toolText(`message queued for ${params.sessionId}`, { sessionId: params.sessionId });
	}

	function updateRemoteMeta(params: PoolParams): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		if (!params.sessionId) return toolText("update_meta requires sessionId", {}, true);
		const patch = patchFromParams(params);
		if (params.sessionId === coordinatorId) {
			applyMetaPatch(patch, "user", true);
			writeCurrentState();
			return toolText("local meta updated", { meta: runtime.meta });
		}
		const target = readSession(paths, params.sessionId);
		if (!target || target.ownerId !== coordinatorId)
			return toolText("session is not owned by the current coordinator", {}, true);
		writeCommand(paths, params.sessionId, coordinatorId, "meta", {
			meta: patch,
			...(params.force ? { force: true } : {}),
		});
		return toolText(`meta update queued for ${params.sessionId}`, { sessionId: params.sessionId, meta: patch });
	}

	function releaseSession(params: PoolParams): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		if (!params.sessionId) return toolText("release requires sessionId", {}, true);
		const target = readSession(paths, params.sessionId);
		if (!target || target.ownerId !== coordinatorId)
			return toolText("session is not owned by the current coordinator", {}, true);
		writeCommand(paths, params.sessionId, coordinatorId, "release", {});
		return toolText(`release queued for ${params.sessionId}`, { sessionId: params.sessionId });
	}

	function stopSession(params: PoolParams): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		if (!params.sessionId) return toolText("stop requires sessionId", {}, true);
		const target = readSession(paths, params.sessionId);
		if (!target || target.ownerId !== coordinatorId)
			return toolText("session is not owned by the current coordinator", {}, true);
		writeCommand(paths, params.sessionId, coordinatorId, "stop", {});
		return toolText(`stop queued for ${params.sessionId}`, { sessionId: params.sessionId });
	}

	function abortSession(params: PoolParams): ReturnType<typeof toolText> {
		const coordinatorId = currentCoordinatorId();
		if (!params.sessionId) return toolText("abort requires sessionId", {}, true);
		const target = readSession(paths, params.sessionId);
		if (!target || target.ownerId !== coordinatorId)
			return toolText("session is not owned by the current coordinator", {}, true);
		writeCommand(paths, params.sessionId, coordinatorId, "abort", {});
		return toolText(`abort queued for ${params.sessionId}`, { sessionId: params.sessionId });
	}

	function reportToCoordinator(params: PoolParams): ReturnType<typeof toolText> {
		if (!runtime.ownerId || !runtime.sessionId || !runtime.activeAssignmentId)
			return toolText("report is only available in a claimed child assignment", {}, true);
		if (params.content === undefined) return toolText("report requires content", {}, true);
		const event = writeEvent(paths, {
			kind: "report",
			sessionId: runtime.sessionId,
			coordinatorId: runtime.ownerId,
			...(runtime.activeAssignmentId ? { assignmentId: runtime.activeAssignmentId } : {}),
			content: truncateUtf8(params.content),
			reportKind: params.reportKind ?? "progress",
		});
		return toolText(`reported ${event.reportKind ?? "progress"}`, { eventId: event.id });
	}

	function statusDetails(): {
		sessions: Record<string, unknown>[];
		assignments: Record<string, unknown>[];
		monitors: Record<string, unknown>[];
	} {
		const sessions = allLocalSessions().map(publicSession);
		if (!runtime.sessionId || runtime.managed) return { sessions, assignments: [], monitors: [] };
		const assignments = readAssignmentRecords(paths, runtime.sessionId).map(publicAssignment);
		const monitors = readMonitorRecords(paths, runtime.sessionId).map((monitor) =>
			publicMonitor(reconcileMonitor(monitor), monitorAssignments(monitor)),
		);
		return { sessions, assignments, monitors };
	}

	function localMetaUpdate(params: PoolParams): ReturnType<typeof toolText> {
		applyMetaPatch(patchFromParams(params), "user");
		writeCurrentState();
		updateWidget();
		return toolText("local meta updated", { meta: runtime.meta });
	}

	function statusText(): string {
		const details = statusDetails();
		const lines = [`session pool mode=${runtime.mode} ready=${runtime.manualReady || runtime.managed}`];
		for (const session of details.sessions) {
			const name =
				isObject(session.meta) && typeof session.meta.displayName === "string"
					? session.meta.displayName
					: String(session.sessionId).slice(0, 8);
			lines.push(
				`${session.status === "running" ? "*" : "o"} ${name} ${session.status} ${session.ownerId ? `owner=${String(session.ownerId).slice(0, 8)}` : ""}`.trim(),
			);
		}
		if (details.monitors.length > 0) lines.push(`monitors=${details.monitors.length}`);
		return lines.join("\n");
	}

	function parseMetaWords(args: string): PoolMeta {
		const patch: PoolMeta = {};
		const aliases: Record<string, MetaField> = { name: "displayName" };
		const allowed: MetaField[] = ["displayName", "role", "task", "model", "worktree", "notes"];
		for (const word of parseCommandWords(args)) {
			const separator = word.indexOf("=");
			if (separator <= 0) continue;
			const rawKey = word.slice(0, separator);
			const key = (aliases[rawKey] ?? rawKey) as MetaField;
			if (!allowed.includes(key)) continue;
			patch[key] = word.slice(separator + 1);
		}
		return patch;
	}

	function registerCommands(): void {
		pi.registerCommand("pool-mode", {
			description: "Enable or disable session-pool child spawning",
			handler: async (args, ctx) => {
				ensureStarted(ctx);
				if (runtime.managed) {
					notify(ctx, "managed child sessions cannot change pool mode", "warning");
					return;
				}
				const mode = parseCommandWords(args)[0];
				if (mode !== "off" && mode !== "visible" && mode !== "headless") {
					notify(ctx, "usage: /pool-mode visible|headless|off", "warning");
					return;
				}
				runtime.mode = mode;
				saveMode();
				updateWidget();
				notify(ctx, `session-pool mode: ${mode}`);
			},
		});

		pi.registerCommand("pool-ready", {
			description: "Mark this Pi session as available to a coordinator",
			handler: async (args, ctx) => {
				ensureStarted(ctx);
				runtime.manualReady = true;
				clearExpiredOwner();
				saveReady();
				const name = parseCommandWords(args).join(" ").trim();
				if (name) applyMetaPatch({ displayName: name }, "user");
				writeCurrentState();
				updateWidget();
				notify(ctx, `session-pool ready${name ? `: ${name}` : ""}`);
			},
		});

		pi.registerCommand("pool-unready", {
			description: "Remove this Pi session from the available pool",
			handler: async (_args, ctx) => {
				ensureStarted(ctx);
				runtime.manualReady = false;
				clearExpiredOwner();
				saveReady();
				writeCurrentState();
				updateWidget();
				notify(ctx, "session-pool unready");
			},
		});

		pi.registerCommand("pool-meta", {
			description: "Patch this session's visible session-pool metadata",
			handler: async (args, ctx) => {
				ensureStarted(ctx);
				const patch = parseMetaWords(args);
				if (Object.keys(patch).length === 0) {
					notify(ctx, 'usage: /pool-meta name=value role=value task="..."', "warning");
					return;
				}
				applyMetaPatch(patch, "user");
				writeCurrentState();
				updateWidget();
				notify(ctx, "session-pool meta updated");
			},
		});

		pi.registerCommand("pool-status", {
			description: "Show same-directory session-pool sessions and monitors",
			handler: async (_args, ctx) => {
				ensureStarted(ctx);
				notify(ctx, statusText());
			},
		});

		pi.registerCommand("pool-send", {
			description: "Send a steer or follow-up message to a claimed child",
			handler: async (args, ctx) => {
				ensureStarted(ctx);
				if (runtime.managed || !runtime.sessionId) {
					notify(ctx, "pool-send is only available in a coordinator session", "warning");
					return;
				}
				const words = parseCommandWords(args);
				const sessionId = words.shift();
				const message = words.join(" ");
				if (!sessionId || !message) {
					notify(ctx, "usage: /pool-send <sessionId> <message>", "warning");
					return;
				}
				const target = readSession(paths, sessionId);
				if (!target || target.ownerId !== runtime.sessionId) {
					notify(ctx, "session is not owned by this coordinator", "warning");
					return;
				}
				if (!target.activeAssignmentId) {
					notify(ctx, "session has no active assignment; use session_pool assign", "warning");
					return;
				}
				writeCommand(paths, sessionId, runtime.sessionId, "send", { message, delivery: "steer" });
				notify(ctx, `message queued for ${sessionId}`);
			},
		});

		pi.registerCommand("pool-abort", {
			description: "Abort the current turn in a claimed child",
			handler: async (args, ctx) => {
				ensureStarted(ctx);
				const sessionId = parseCommandWords(args)[0];
				if (!sessionId || !runtime.sessionId || runtime.managed) {
					notify(ctx, "usage: /pool-abort <sessionId>", "warning");
					return;
				}
				const target = readSession(paths, sessionId);
				if (!target || target.ownerId !== runtime.sessionId) {
					notify(ctx, "session is not owned by this coordinator", "warning");
					return;
				}
				writeCommand(paths, sessionId, runtime.sessionId, "abort", {});
				notify(ctx, `abort queued for ${sessionId}`);
			},
		});

		pi.registerCommand("pool-stop", {
			description: "Stop a claimed child session",
			handler: async (args, ctx) => {
				ensureStarted(ctx);
				const sessionId = parseCommandWords(args)[0];
				if (!sessionId || !runtime.sessionId || runtime.managed) {
					notify(ctx, "usage: /pool-stop <sessionId>", "warning");
					return;
				}
				const target = readSession(paths, sessionId);
				if (!target || target.ownerId !== runtime.sessionId) {
					notify(ctx, "session is not owned by this coordinator", "warning");
					return;
				}
				writeCommand(paths, sessionId, runtime.sessionId, "stop", {});
				notify(ctx, `stop queued for ${sessionId}`);
			},
		});
	}

	function registerLifecycleHandlers(): void {
		pi.on("session_start", async (_event, ctx) => {
			startSession(ctx);
		});
		pi.on("session_shutdown", async (_event, ctx) => {
			shutdownSession(ctx);
		});
		pi.on("agent_start", async (_event, ctx) => {
			ensureStarted(ctx);
			runtime.status = "running";
			writeCurrentState();
			updateWidget();
		});
		pi.on("message_end", async (event, ctx) => {
			ensureStarted(ctx);
			if (event.message.role === "assistant") {
				runtime.lastAssistantText = extractText(event.message);
				runtime.lastAssistantStopReason = event.message.stopReason;
			}
		});
		pi.on("agent_settled", async (_event, ctx) => {
			ensureStarted(ctx);
			const assignmentId = runtime.activeAssignmentId;
			if (!assignmentId || !runtime.sessionId || !runtime.ownerId) {
				runtime.status = contextIsIdle(ctx) ? "idle" : "running";
				writeCurrentState();
				return;
			}
			const cancelled = runtime.cancelRequested?.assignmentId === assignmentId;
			const failed =
				!cancelled &&
				(runtime.lastAssistantStopReason === "error" || runtime.lastAssistantStopReason === "aborted");
			writeEvent(paths, {
				kind: "settled",
				sessionId: runtime.sessionId,
				coordinatorId: runtime.ownerId,
				assignmentId,
				status: cancelled ? "cancelled" : failed ? "failed" : "completed",
				failed,
				content: truncateUtf8(runtime.lastAssistantText),
				...(cancelled
					? { error: runtime.cancelRequested?.reason === "stop" ? "child session stopped" : "child agent aborted" }
					: failed
						? { error: runtime.lastAssistantStopReason || "child agent failed" }
						: {}),
			});
			runtime.activeAssignmentId = undefined;
			runtime.cancelRequested = undefined;
			runtime.status = "idle";
			writeCurrentState();
			updateWidget();
		});
		pi.on("session_info_changed", async (_event, ctx) => {
			ensureStarted(ctx);
			writeCurrentState();
			updateWidget();
		});
	}

	function registerTool(): void {
		pi.registerTool({
			name: "session_pool",
			label: "Session pool",
			description: [
				"Coordinate independent Pi sessions in the current working directory.",
				"The coordinator model chooses roles, prompts, models, concurrency, and workflow; this tool only discovers, starts, claims, communicates with, and monitors sessions.",
				`Install/load this extension in every session. Shared state is stored under ${paths.root}.`,
			].join(" "),
			promptSnippet: "Discover and coordinate ready Pi sessions",
			promptGuidelines: [
				"Prefer delegating concrete implementation, research, and review work to available sessions; keep the coordinator focused on planning, supervision, and final decisions.",
				"Use list before assign, and start a non-blocking monitor after dispatching parallel assignments.",
				"Do not assume this tool creates or isolates worktrees; the user and workflow decide repository isolation.",
				"A queue wake policy returns child reports on the next user/model turn without triggering a model call; use wake only when an immediate coordinator turn is worth the token cost.",
			],
			parameters: PoolParamsSchema,
			executionMode: "sequential",
			async execute(_toolCallId, params, signal, _onUpdate, ctx) {
				ensureStarted(ctx);
				try {
					switch (params.action) {
						case "list": {
							const records = availableSessions();
							return toolText(JSON.stringify(records.map(publicSession), null, 2), {
								sessions: records.map(publicSession),
							});
						}
						case "status": {
							const details = statusDetails();
							return toolText(JSON.stringify(details, null, 2), details);
						}
						case "spawn": {
							const result = await spawnChild(params, signal);
							if (result.fallback)
								notify(
									ctx,
									`visible launcher unavailable; using headless child (${result.fallback})`,
									"warning",
								);
							return toolText(`spawned ${result.record.sessionId} (${result.mode})`, {
								session: publicSession(result.record),
								mode: result.mode,
								fallback: result.fallback,
							});
						}
						case "assign":
							return await assignSession(params, signal);
						case "send":
							return await sendToSession(params);
						case "abort":
							return abortSession(params);
						case "monitor_start":
							return startMonitor(params);
						case "monitor_status":
							return monitorStatus(params.monitorId);
						case "monitor_cancel":
							return cancelMonitor(params.monitorId);
						case "update_meta":
							if (!params.sessionId) return localMetaUpdate(params);
							return updateRemoteMeta(params);
						case "release":
							return releaseSession(params);
						case "stop":
							if (runtime.managed && !params.sessionId) {
								ctx.shutdown();
								return toolText("stopping child session");
							}
							return stopSession(params);
						case "report":
							return reportToCoordinator(params);
					}
				} catch (error) {
					return toolText(error instanceof Error ? error.message : String(error), {}, true);
				}
			},
		});
	}

	registerLifecycleHandlers();
	registerCommands();
	registerTool();
}
