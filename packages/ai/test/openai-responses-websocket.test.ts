import { afterEach, describe, expect, it, vi } from "vitest";
import { stream as streamOpenAIResponses } from "../src/api/openai-responses.ts";
import { cleanupSessionResources } from "../src/session-resources.ts";
import type { Context, Model } from "../src/types.ts";

const websocketState = vi.hoisted(() => ({
	connections: 0,
	completed: 0,
	failedContinuation: false,
	headers: [] as Record<string, string>[],
	requests: [] as Array<Record<string, unknown>>,
}));

vi.mock("openai/resources/responses/ws", () => ({
	ResponsesWS: class {
		socket = { readyState: 1 };

		constructor(_client: unknown, options?: { headers?: Record<string, string> }) {
			websocketState.connections++;
			websocketState.headers.push(options?.headers ?? {});
		}

		send(event: Record<string, unknown>): void {
			websocketState.requests.push(event);
		}

		close(): void {
			this.socket.readyState = 3;
		}

		async *stream(): AsyncGenerator<Record<string, unknown>> {
			yield { type: "open" };
			const request = websocketState.requests.at(-1)!;
			if (request.previous_response_id && !websocketState.failedContinuation) {
				websocketState.failedContinuation = true;
				yield {
					type: "error",
					error: {
						error: {
							type: "error",
							error: { code: "previous_response_not_found", message: "expired" },
						},
					},
				};
				return;
			}

			const responseId = `resp_${++websocketState.completed}`;
			const text = websocketState.completed === 1 ? "first answer" : "second answer";
			yield { type: "message", message: { type: "response.created", response: { id: responseId } } };
			yield {
				type: "message",
				message: {
					type: "response.output_item.added",
					output_index: 0,
					item: { type: "message", id: `msg_${websocketState.completed}`, role: "assistant", content: [] },
				},
			};
			yield { type: "message", message: { type: "response.output_text.delta", output_index: 0, delta: text } };
			yield {
				type: "message",
				message: {
					type: "response.output_item.done",
					output_index: 0,
					item: {
						type: "message",
						id: `msg_${websocketState.completed}`,
						role: "assistant",
						content: [{ type: "output_text", text }],
					},
				},
			};
			yield {
				type: "message",
				message: {
					type: "response.completed",
					response: {
						id: responseId,
						status: "completed",
						usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
					},
				},
			};
		}
	},
}));

function createModel(): Model<"openai-responses"> {
	return {
		id: "gpt-5.1",
		name: "GPT-5.1",
		api: "openai-responses",
		provider: "sub2api",
		baseUrl: "http://gateway.test/v1",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128000,
		maxTokens: 16384,
		compat: { supportsResponsesWebSocket: true },
	};
}

afterEach(() => {
	cleanupSessionResources("ws-test-session");
	Object.assign(websocketState, {
		connections: 0,
		completed: 0,
		failedContinuation: false,
		headers: [],
		requests: [],
	});
});

describe("openai responses websocket", () => {
	it("reuses a cached socket, sends deltas, and recovers a missing continuation", async () => {
		const model = createModel();
		const firstContext: Context = {
			messages: [{ role: "user", content: "first", timestamp: 1 }],
		};
		const first = await streamOpenAIResponses(model, firstContext, {
			apiKey: "sk-test",
			sessionId: "ws-test-session",
		}).result();

		const secondContext: Context = {
			messages: [...firstContext.messages, first, { role: "user", content: "second", timestamp: 2 }],
		};
		const second = await streamOpenAIResponses(model, secondContext, {
			apiKey: "sk-test",
			sessionId: "ws-test-session",
		}).result();

		expect(second.content.find((block) => block.type === "text")).toMatchObject({ text: "second answer" });
		expect(websocketState.connections).toBe(2);
		expect(websocketState.headers).toHaveLength(2);
		expect(websocketState.headers[0]).toMatchObject({
			"openai-beta": "responses_websockets=2026-02-06",
			authorization: "Bearer sk-test",
			session_id: "ws-test-session",
			"x-client-request-id": "ws-test-session",
		});

		expect(websocketState.requests).toHaveLength(3);
		const [firstRequest, cachedRequest, recoveredRequest] = websocketState.requests;
		expect(firstRequest).toMatchObject({ type: "response.create", model: "gpt-5.1" });
		expect(cachedRequest.previous_response_id).toBe("resp_1");
		expect(cachedRequest.input).toEqual([{ role: "user", content: [{ type: "input_text", text: "second" }] }]);
		expect(recoveredRequest.previous_response_id).toBeUndefined();
		expect(JSON.stringify(recoveredRequest.input)).toContain("first answer");
		expect(JSON.stringify(recoveredRequest.input)).toContain("second");
	});
});
