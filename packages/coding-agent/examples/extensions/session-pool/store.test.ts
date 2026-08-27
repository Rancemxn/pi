import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { firstPoolResultLine, parseCommandWords, poolCallLabel, requestedEffort, shortPoolId } from "./index.ts";
import {
	type AssignmentRecord,
	createPoolPaths,
	ensurePoolPaths,
	isFresh,
	MAX_REPORT_BYTES,
	type MonitorRecord,
	normalizeCwd,
	readAssignment,
	readAssignmentRecords,
	readCommands,
	readEvents,
	readMonitor,
	readMonitorRecords,
	readSession,
	readSessionRecords,
	type SessionRecord,
	truncateUtf8,
	writeAssignment,
	writeCommand,
	writeEvent,
	writeMonitor,
	writeSession,
} from "./store.ts";

function makeTempPaths() {
	const root = mkdtempSync(path.join(os.tmpdir(), "pi-session-pool-"));
	const paths = createPoolPaths(root);
	ensurePoolPaths(paths);
	return { root, paths };
}

test("normalizes cwd and truncates UTF-8 without exceeding the byte limit", () => {
	const root = path.resolve(os.tmpdir(), "pool-cwd");
	assert.equal(normalizeCwd(path.join(root, "nested", "..")), normalizeCwd(root));
	const value = "汉字".repeat(100);
	const truncated = truncateUtf8(value, 40);
	assert.ok(Buffer.byteLength(truncated, "utf8") <= 40);
	assert.match(truncated, /output truncated/);
	assert.ok(Buffer.byteLength(truncateUtf8(value, 5), "utf8") <= 5);
	assert.ok(Buffer.byteLength(truncateUtf8(value), "utf8") <= MAX_REPORT_BYTES);
});

test("parses quoted values embedded in key=value arguments", () => {
	assert.deepEqual(parseCommandWords('role=worker task="review the parser" worktree=C:\\work\\parser-a'), [
		"role=worker",
		"task=review the parser",
		"worktree=C:\\work\\parser-a",
	]);
	assert.deepEqual(parseCommandWords("\"two words\" notes='keep it'"), ["two words", "notes=keep it"]);
});

test("prefers effort over the legacy thinkingLevel field", () => {
	assert.equal(requestedEffort({ effort: "high", thinkingLevel: "low" }), "high");
	assert.equal(requestedEffort({ thinkingLevel: "medium" }), "medium");
});

test("keeps session-pool tool rows compact while preserving expansion text", () => {
	assert.equal(shortPoolId("assignment-12345678-rest"), "assignment:12345678");
	assert.equal(poolCallLabel({ action: "monitor_start", assignmentIds: ["a", "b"] }), "monitor_start 2 assignments");
	assert.equal(
		firstPoolResultLine({ content: [{ type: "text", text: "assignment assignment-1: completed\nfull payload" }] }),
		"assignment assignment-1: completed",
	);
	assert.equal(firstPoolResultLine({ content: [] }), "done");
});

test("round-trips command and event mailboxes", () => {
	const { root, paths } = makeTempPaths();
	try {
		const command = writeCommand(paths, "child-1", "coord-1", "assign", { prompt: "inspect" });
		const commands = readCommands(paths, "child-1");
		assert.equal(commands.length, 1);
		assert.deepEqual(commands[0]?.command, command);

		const event = writeEvent(paths, {
			kind: "settled",
			sessionId: "child-1",
			coordinatorId: "coord-1",
			assignmentId: "assignment-1",
			status: "completed",
			failed: false,
			content: "done",
		});
		const events = readEvents(paths, "coord-1");
		assert.equal(events.length, 1);
		assert.deepEqual(events[0]?.event, event);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
});

test("round-trips sessions, assignments, and monitors", () => {
	const { root, paths } = makeTempPaths();
	try {
		const now = Date.now();
		const session: SessionRecord = {
			version: 1,
			sessionId: "child-1",
			pid: process.pid,
			cwd: root,
			cwdKey: normalizeCwd(root),
			transport: "rpc",
			ready: true,
			managed: true,
			ownerId: "coord-1",
			spawnToken: "token-1",
			status: "idle",
			meta: { displayName: "worker", role: "worker", model: "provider/model", notes: "" },
			userOverrides: ["displayName"],
			heartbeatAt: now,
			startedAt: now,
			updatedAt: now,
		};
		writeSession(paths, session);
		assert.deepEqual(readSession(paths, session.sessionId), session);
		assert.deepEqual(readSessionRecords(paths), [session]);
		assert.equal(isFresh(session, now), true);

		const assignment: AssignmentRecord = {
			version: 1,
			assignmentId: "assignment-1",
			sessionId: session.sessionId,
			coordinatorId: "coord-1",
			prompt: "inspect",
			role: "worker",
			status: "pending",
			createdAt: now,
			updatedAt: now,
		};
		writeAssignment(paths, assignment);
		assert.deepEqual(readAssignment(paths, assignment.assignmentId), assignment);
		assert.deepEqual(readAssignmentRecords(paths, "coord-1"), [assignment]);

		const monitor: MonitorRecord = {
			version: 1,
			monitorId: "monitor-1",
			coordinatorId: "coord-1",
			assignmentIds: [assignment.assignmentId],
			createdAt: now,
			deadlineAt: now + 60_000,
			state: "active",
			wakePolicy: "queue",
		};
		writeMonitor(paths, monitor);
		assert.deepEqual(readMonitor(paths, monitor.monitorId), monitor);
		assert.deepEqual(readMonitorRecords(paths, "coord-1"), [monitor]);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
});
