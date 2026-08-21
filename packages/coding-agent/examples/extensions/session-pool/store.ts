import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export const POOL_PROTOCOL_VERSION = 1;
export const DEFAULT_STALE_AFTER_MS = 15_000;
export const DEFAULT_HEARTBEAT_MS = 2_000;
export const DEFAULT_POLL_MS = 250;
export const MAX_REPORT_BYTES = 32 * 1024;

export type PoolMode = "off" | "visible" | "headless";
export type SessionTransport = "tui" | "rpc";
export type SessionStatus = "idle" | "running" | "stopping" | "exited";
export type AssignmentStatus = "pending" | "running" | "completed" | "failed" | "cancelled";
export type MonitorState = "active" | "completed" | "timed_out" | "cancelled";
export type WakePolicy = "queue" | "silent" | "wake";
export type ReportKind = "progress" | "needs_attention" | "needs_decision" | "failed";

export interface PoolMeta {
	displayName?: string;
	role?: string;
	task?: string;
	model?: string;
	worktree?: string;
	notes?: string;
}

export interface SessionRecord {
	version: number;
	sessionId: string;
	pid: number;
	cwd: string;
	cwdKey: string;
	transport: SessionTransport;
	ready: boolean;
	managed: boolean;
	ownerId?: string;
	spawnToken?: string;
	status: SessionStatus;
	activeAssignmentId?: string;
	meta: PoolMeta;
	userOverrides: string[];
	heartbeatAt: number;
	startedAt: number;
	updatedAt: number;
	sessionFile?: string;
	sessionName?: string;
}

export interface AssignmentRecord {
	version: number;
	assignmentId: string;
	sessionId: string;
	coordinatorId: string;
	prompt: string;
	role?: string;
	task?: string;
	status: AssignmentStatus;
	createdAt: number;
	updatedAt: number;
	monitorId?: string;
	result?: string;
	error?: string;
	lastEventId?: string;
}

export interface MonitorRecord {
	version: number;
	monitorId: string;
	coordinatorId: string;
	assignmentIds: string[];
	createdAt: number;
	deadlineAt: number;
	state: MonitorState;
	wakePolicy: WakePolicy;
}

export type PoolCommandType = "assign" | "send" | "abort" | "release" | "stop" | "meta";

export interface PoolCommand {
	version: number;
	id: string;
	type: PoolCommandType;
	sessionId: string;
	coordinatorId: string;
	createdAt: number;
	payload: Record<string, unknown>;
}

export type PoolEventKind = "accepted" | "rejected" | "settled" | "report" | "released" | "stopped" | "exited";

export interface PoolEvent {
	version: number;
	id: string;
	kind: PoolEventKind;
	sessionId: string;
	coordinatorId: string;
	createdAt: number;
	assignmentId?: string;
	status?: AssignmentStatus;
	failed?: boolean;
	content?: string;
	error?: string;
	reportKind?: ReportKind;
}

export interface PoolPaths {
	root: string;
	sessions: string;
	commands: string;
	events: string;
	assignments: string;
	monitors: string;
}

export function createPoolPaths(root: string): PoolPaths {
	return {
		root,
		sessions: path.join(root, "sessions"),
		commands: path.join(root, "commands"),
		events: path.join(root, "events"),
		assignments: path.join(root, "assignments"),
		monitors: path.join(root, "monitors"),
	};
}

export function ensurePoolPaths(paths: PoolPaths): void {
	for (const directory of [
		paths.root,
		paths.sessions,
		paths.commands,
		paths.events,
		paths.assignments,
		paths.monitors,
	]) {
		fs.mkdirSync(directory, { recursive: true });
	}
}

export function normalizeCwd(cwd: string): string {
	const normalized = path.normalize(path.resolve(cwd));
	return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

export function isFresh(record: SessionRecord, now = Date.now(), staleAfterMs = DEFAULT_STALE_AFTER_MS): boolean {
	return now - record.heartbeatAt <= staleAfterMs;
}

export function truncateUtf8(value: string, maxBytes = MAX_REPORT_BYTES): string {
	if (maxBytes <= 0) return "";
	if (Buffer.byteLength(value, "utf8") <= maxBytes) return value;
	const suffix = "\n\n[session-pool output truncated]";
	if (Buffer.byteLength(suffix, "utf8") > maxBytes) {
		let suffixEnd = suffix.length;
		while (suffixEnd > 0 && Buffer.byteLength(suffix.slice(0, suffixEnd), "utf8") > maxBytes) suffixEnd--;
		return suffix.slice(0, suffixEnd);
	}
	let end = value.length;
	while (end > 0 && Buffer.byteLength(`${value.slice(0, end)}${suffix}`, "utf8") > maxBytes) end--;
	return `${value.slice(0, end)}${suffix}`;
}

export function writeAtomicJson(filePath: string, value: unknown): void {
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	const tempPath = path.join(path.dirname(filePath), `.${path.basename(filePath)}.${process.pid}.${randomUUID()}.tmp`);
	try {
		fs.writeFileSync(tempPath, JSON.stringify(value, null, 2), "utf8");
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				fs.renameSync(tempPath, filePath);
				return;
			} catch (error) {
				if (attempt === 2) throw error;
				if (process.platform === "win32") {
					try {
						fs.rmSync(filePath, { force: true });
					} catch {
						// The next rename attempt may still succeed if the file is briefly locked.
					}
				}
			}
		}
	} finally {
		try {
			fs.rmSync(tempPath, { force: true });
		} catch {
			// Best-effort cleanup; the uniquely named temp file is harmless.
		}
	}
}

export function readJson(filePath: string): unknown {
	try {
		return JSON.parse(fs.readFileSync(filePath, "utf8")) as unknown;
	} catch {
		return undefined;
	}
}

export function removeFile(filePath: string): void {
	try {
		fs.rmSync(filePath, { force: true });
	} catch {
		// A concurrent cleanup or antivirus scan should not stop the poller.
	}
}

function safeId(value: string): string {
	return value.replace(/[^A-Za-z0-9._-]/g, "_");
}

export function sessionPath(paths: PoolPaths, sessionId: string): string {
	return path.join(paths.sessions, `${safeId(sessionId)}.json`);
}

export function assignmentPath(paths: PoolPaths, assignmentId: string): string {
	return path.join(paths.assignments, `${safeId(assignmentId)}.json`);
}

export function monitorPath(paths: PoolPaths, monitorId: string): string {
	return path.join(paths.monitors, `${safeId(monitorId)}.json`);
}

export function commandDirectory(paths: PoolPaths, sessionId: string): string {
	return path.join(paths.commands, safeId(sessionId));
}

export function eventDirectory(paths: PoolPaths, coordinatorId: string): string {
	return path.join(paths.events, safeId(coordinatorId));
}

function nextMessagePath(directory: string): string {
	return path.join(directory, `${Date.now().toString(36)}-${randomUUID()}.json`);
}

export function writeCommand(
	paths: PoolPaths,
	sessionId: string,
	coordinatorId: string,
	type: PoolCommandType,
	payload: Record<string, unknown>,
): PoolCommand {
	const command: PoolCommand = {
		version: POOL_PROTOCOL_VERSION,
		id: randomUUID(),
		type,
		sessionId,
		coordinatorId,
		createdAt: Date.now(),
		payload,
	};
	const directory = commandDirectory(paths, sessionId);
	writeAtomicJson(nextMessagePath(directory), command);
	return command;
}

export function writeEvent(paths: PoolPaths, event: Omit<PoolEvent, "version" | "id" | "createdAt">): PoolEvent {
	const fullEvent: PoolEvent = {
		version: POOL_PROTOCOL_VERSION,
		id: randomUUID(),
		createdAt: Date.now(),
		...event,
	};
	writeAtomicJson(nextMessagePath(eventDirectory(paths, event.coordinatorId)), fullEvent);
	return fullEvent;
}

export function listFiles(directory: string): string[] {
	try {
		return fs
			.readdirSync(directory, { withFileTypes: true })
			.filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
			.map((entry) => path.join(directory, entry.name))
			.sort();
	} catch {
		return [];
	}
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function asString(value: unknown): string | undefined {
	return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asBoolean(value: unknown): boolean | undefined {
	return typeof value === "boolean" ? value : undefined;
}

function parseMeta(value: unknown): PoolMeta {
	const record = asRecord(value);
	if (!record) return {};
	const meta: PoolMeta = {};
	for (const field of ["displayName", "role", "task", "model", "worktree", "notes"] as const) {
		const item = asString(record[field]);
		if (item !== undefined) meta[field] = item;
	}
	return meta;
}

function parseSession(value: unknown): SessionRecord | undefined {
	const record = asRecord(value);
	if (!record) return undefined;
	const sessionId = asString(record.sessionId);
	const cwd = asString(record.cwd);
	const cwdKey = asString(record.cwdKey);
	const transport = record.transport === "tui" || record.transport === "rpc" ? record.transport : undefined;
	const status =
		record.status === "idle" ||
		record.status === "running" ||
		record.status === "stopping" ||
		record.status === "exited"
			? record.status
			: undefined;
	const pid = asNumber(record.pid);
	const heartbeatAt = asNumber(record.heartbeatAt);
	const startedAt = asNumber(record.startedAt);
	const updatedAt = asNumber(record.updatedAt);
	if (
		!sessionId ||
		!cwd ||
		!cwdKey ||
		!transport ||
		!status ||
		pid === undefined ||
		heartbeatAt === undefined ||
		startedAt === undefined ||
		updatedAt === undefined
	)
		return undefined;
	const overrides = Array.isArray(record.userOverrides)
		? record.userOverrides.filter((item): item is string => typeof item === "string")
		: [];
	const ownerId = asString(record.ownerId);
	const spawnToken = asString(record.spawnToken);
	const activeAssignmentId = asString(record.activeAssignmentId);
	const sessionFile = asString(record.sessionFile);
	const sessionName = asString(record.sessionName);
	return {
		version: asNumber(record.version) ?? POOL_PROTOCOL_VERSION,
		sessionId,
		pid,
		cwd,
		cwdKey,
		transport,
		ready: asBoolean(record.ready) ?? false,
		managed: asBoolean(record.managed) ?? false,
		...(ownerId ? { ownerId } : {}),
		...(spawnToken ? { spawnToken } : {}),
		status,
		...(activeAssignmentId ? { activeAssignmentId } : {}),
		meta: parseMeta(record.meta),
		userOverrides: overrides,
		heartbeatAt,
		startedAt,
		updatedAt,
		...(sessionFile ? { sessionFile } : {}),
		...(sessionName ? { sessionName } : {}),
	};
}

function parseAssignment(value: unknown): AssignmentRecord | undefined {
	const record = asRecord(value);
	if (!record) return undefined;
	const assignmentId = asString(record.assignmentId);
	const sessionId = asString(record.sessionId);
	const coordinatorId = asString(record.coordinatorId);
	const prompt = asString(record.prompt);
	const status =
		record.status === "pending" ||
		record.status === "running" ||
		record.status === "completed" ||
		record.status === "failed" ||
		record.status === "cancelled"
			? record.status
			: undefined;
	const createdAt = asNumber(record.createdAt);
	const updatedAt = asNumber(record.updatedAt);
	if (
		!assignmentId ||
		!sessionId ||
		!coordinatorId ||
		prompt === undefined ||
		!status ||
		createdAt === undefined ||
		updatedAt === undefined
	)
		return undefined;
	const role = asString(record.role);
	const task = asString(record.task);
	const monitorId = asString(record.monitorId);
	const result = asString(record.result);
	const error = asString(record.error);
	const lastEventId = asString(record.lastEventId);
	return {
		version: asNumber(record.version) ?? POOL_PROTOCOL_VERSION,
		assignmentId,
		sessionId,
		coordinatorId,
		prompt,
		...(role ? { role } : {}),
		...(task ? { task } : {}),
		status,
		createdAt,
		updatedAt,
		...(monitorId ? { monitorId } : {}),
		...(result ? { result } : {}),
		...(error ? { error } : {}),
		...(lastEventId ? { lastEventId } : {}),
	};
}

function parseMonitor(value: unknown): MonitorRecord | undefined {
	const record = asRecord(value);
	if (!record) return undefined;
	const monitorId = asString(record.monitorId);
	const coordinatorId = asString(record.coordinatorId);
	const assignmentIds = Array.isArray(record.assignmentIds)
		? record.assignmentIds.filter((item): item is string => typeof item === "string")
		: [];
	const createdAt = asNumber(record.createdAt);
	const deadlineAt = asNumber(record.deadlineAt);
	const state =
		record.state === "active" ||
		record.state === "completed" ||
		record.state === "timed_out" ||
		record.state === "cancelled"
			? record.state
			: undefined;
	const wakePolicy =
		record.wakePolicy === "queue" || record.wakePolicy === "silent" || record.wakePolicy === "wake"
			? record.wakePolicy
			: undefined;
	if (
		!monitorId ||
		!coordinatorId ||
		assignmentIds.length === 0 ||
		createdAt === undefined ||
		deadlineAt === undefined ||
		!state ||
		!wakePolicy
	)
		return undefined;
	return {
		version: asNumber(record.version) ?? POOL_PROTOCOL_VERSION,
		monitorId,
		coordinatorId,
		assignmentIds,
		createdAt,
		deadlineAt,
		state,
		wakePolicy,
	};
}

function parseCommand(value: unknown): PoolCommand | undefined {
	const record = asRecord(value);
	if (!record) return undefined;
	const id = asString(record.id);
	const sessionId = asString(record.sessionId);
	const coordinatorId = asString(record.coordinatorId);
	const createdAt = asNumber(record.createdAt);
	const type = record.type;
	const payload = asRecord(record.payload);
	if (
		!id ||
		!sessionId ||
		!coordinatorId ||
		createdAt === undefined ||
		!payload ||
		(type !== "assign" &&
			type !== "send" &&
			type !== "abort" &&
			type !== "release" &&
			type !== "stop" &&
			type !== "meta")
	)
		return undefined;
	return {
		version: asNumber(record.version) ?? POOL_PROTOCOL_VERSION,
		id,
		type,
		sessionId,
		coordinatorId,
		createdAt,
		payload,
	};
}

function parseEvent(value: unknown): PoolEvent | undefined {
	const record = asRecord(value);
	if (!record) return undefined;
	const id = asString(record.id);
	const sessionId = asString(record.sessionId);
	const coordinatorId = asString(record.coordinatorId);
	const createdAt = asNumber(record.createdAt);
	const kind = record.kind;
	if (
		!id ||
		!sessionId ||
		!coordinatorId ||
		createdAt === undefined ||
		(kind !== "accepted" &&
			kind !== "rejected" &&
			kind !== "settled" &&
			kind !== "report" &&
			kind !== "released" &&
			kind !== "stopped" &&
			kind !== "exited")
	)
		return undefined;
	const assignmentId = asString(record.assignmentId);
	const content = asString(record.content);
	const error = asString(record.error);
	const failed = asBoolean(record.failed);
	const reportKind =
		record.reportKind === "progress" ||
		record.reportKind === "needs_attention" ||
		record.reportKind === "needs_decision" ||
		record.reportKind === "failed"
			? record.reportKind
			: undefined;
	return {
		version: asNumber(record.version) ?? POOL_PROTOCOL_VERSION,
		id,
		kind,
		sessionId,
		coordinatorId,
		createdAt,
		...(assignmentId ? { assignmentId } : {}),
		...(record.status === "pending" ||
		record.status === "running" ||
		record.status === "completed" ||
		record.status === "failed" ||
		record.status === "cancelled"
			? { status: record.status }
			: {}),
		...(failed !== undefined ? { failed } : {}),
		...(content ? { content } : {}),
		...(error ? { error } : {}),
		...(reportKind ? { reportKind } : {}),
	};
}

export function readSessionRecords(paths: PoolPaths): SessionRecord[] {
	return listFiles(paths.sessions)
		.map((filePath) => parseSession(readJson(filePath)))
		.filter((record): record is SessionRecord => record !== undefined);
}

export function readAssignmentRecords(paths: PoolPaths, coordinatorId: string): AssignmentRecord[] {
	return listFiles(paths.assignments)
		.map((filePath) => parseAssignment(readJson(filePath)))
		.filter((record): record is AssignmentRecord => record?.coordinatorId === coordinatorId);
}

export function readMonitorRecords(paths: PoolPaths, coordinatorId: string): MonitorRecord[] {
	return listFiles(paths.monitors)
		.map((filePath) => parseMonitor(readJson(filePath)))
		.filter((record): record is MonitorRecord => record?.coordinatorId === coordinatorId);
}

export function readCommands(paths: PoolPaths, sessionId: string): Array<{ filePath: string; command: PoolCommand }> {
	return listFiles(commandDirectory(paths, sessionId))
		.map((filePath) => ({ filePath, command: parseCommand(readJson(filePath)) }))
		.filter((item): item is { filePath: string; command: PoolCommand } => item.command !== undefined);
}

export function readEvents(paths: PoolPaths, coordinatorId: string): Array<{ filePath: string; event: PoolEvent }> {
	return listFiles(eventDirectory(paths, coordinatorId))
		.map((filePath) => ({ filePath, event: parseEvent(readJson(filePath)) }))
		.filter((item): item is { filePath: string; event: PoolEvent } => item.event !== undefined);
}

export function readAssignment(paths: PoolPaths, assignmentId: string): AssignmentRecord | undefined {
	return parseAssignment(readJson(assignmentPath(paths, assignmentId)));
}

export function readMonitor(paths: PoolPaths, monitorId: string): MonitorRecord | undefined {
	return parseMonitor(readJson(monitorPath(paths, monitorId)));
}

export function readSession(paths: PoolPaths, sessionId: string): SessionRecord | undefined {
	return parseSession(readJson(sessionPath(paths, sessionId)));
}

export function writeSession(paths: PoolPaths, record: SessionRecord): void {
	writeAtomicJson(sessionPath(paths, record.sessionId), record);
}

export function writeAssignment(paths: PoolPaths, record: AssignmentRecord): void {
	writeAtomicJson(assignmentPath(paths, record.assignmentId), record);
}

export function writeMonitor(paths: PoolPaths, record: MonitorRecord): void {
	writeAtomicJson(monitorPath(paths, record.monitorId), record);
}

export function deleteSession(paths: PoolPaths, sessionId: string): void {
	removeFile(sessionPath(paths, sessionId));
}

export function makeId(prefix: string): string {
	return `${prefix}-${randomUUID()}`;
}
