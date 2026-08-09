import type { AgentTool } from "@earendil-works/pi-agent-core";
import { contentText, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it } from "vitest";
import { createHarness, type Harness } from "../harness.ts";

describe("mid-run auto-compaction regression", () => {
	const harnesses: Harness[] = [];

	afterEach(() => {
		while (harnesses.length > 0) harnesses.pop()?.cleanup();
	});

	it("compacts a tool loop before its next provider request", async () => {
		const noopTool: AgentTool = {
			name: "noop",
			label: "No-op",
			description: "Do nothing",
			parameters: Type.Object({}),
			execute: async () => ({ content: [{ type: "text", text: "done" }], details: {} }),
		};
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 10_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 3, reserveTokens: 5_000 } },
			tools: [noopTool],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "mid-run summary",
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							details: {},
						},
					}));
				},
			],
		});
		harnesses.push(harness);
		const prepareNextTurn = harness.session.agent.prepareNextTurnWithContext!;
		harness.session.agent.prepareNextTurnWithContext = async (turn, signal) => {
			if (turn.toolResults.length > 0) {
				turn.message.usage = {
					input: 5_001,
					output: 0,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 5_001,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				};
			}
			return await prepareNextTurn(turn, signal);
		};

		let secondRequestText = "";
		harness.setResponses([
			fauxAssistantMessage(fauxToolCall("noop", {}), { stopReason: "toolUse" }),
			(context) => {
				secondRequestText = context.messages
					.filter((message) => message.role === "user")
					.map((message) => contentText(message.content))
					.join("\n");
				return fauxAssistantMessage("finished");
			},
		]);

		await harness.session.prompt("start");

		expect(harness.eventsOfType("turn_end").map((event) => event.toolResults.length)).toEqual([1, 0]);
		expect(harness.eventsOfType("compaction_start")).toHaveLength(1);
		expect(harness.eventsOfType("compaction_end")[0]).toMatchObject({
			reason: "threshold",
			aborted: false,
		});
		expect(secondRequestText).toContain("mid-run summary");
		expect(harness.faux.state.callCount).toBe(2);
		expect(harness.eventsOfType("agent_end")).toHaveLength(1);
		expect(harness.session.getLastAssistantText()).toBe("finished");
	});
});
