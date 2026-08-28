import OpenAI from "openai";
import type {
	ResponseCreateParamsStreaming,
	ResponseInput,
	ResponseStreamEvent,
} from "openai/resources/responses/responses.js";
import { ResponsesWS, type ResponsesWSClientOptions } from "openai/resources/responses/ws";
import { clampThinkingLevel } from "../models.ts";
import { registerSessionResourceCleanup } from "../session-resources.ts";
import type {
	Api,
	AssistantMessage,
	CacheRetention,
	Context,
	Model,
	OpenAIResponsesCompat,
	ProviderEnv,
	ProviderHeaders,
	SimpleStreamOptions,
	StreamFunction,
	StreamOptions,
	Usage,
} from "../types.ts";
import { splitDeferredTools } from "../utils/deferred-tools.ts";
import { formatProviderError, normalizeProviderError } from "../utils/error-body.ts";
import { AssistantMessageEventStream } from "../utils/event-stream.ts";
import { shortHash } from "../utils/hash.ts";
import { headersToRecord } from "../utils/headers.ts";
import { getPiUserAgent } from "../utils/pi-user-agent.ts";
import { getProviderEnvValue } from "../utils/provider-env.ts";
import { retryProviderRequest } from "../utils/provider-retry.ts";
import { createGrammarToolInputProperties } from "./constrained-sampling.ts";
import { buildCopilotDynamicHeaders, hasCopilotVisionInput } from "./github-copilot-headers.ts";
import { clampOpenAIPromptCacheKey } from "./openai-prompt-cache.ts";
import { convertResponsesMessages, convertResponsesTools, processResponsesStream } from "./openai-responses-shared.ts";
import { buildBaseOptions } from "./simple-options.ts";

const OPENAI_TOOL_CALL_PROVIDERS = new Set(["openai", "openai-codex", "opencode"]);
// OpenAI Responses rejects max_output_tokens below 16: https://github.com/earendil-works/pi/issues/6265
const OPENAI_RESPONSES_MIN_OUTPUT_TOKENS = 16;

function hasHeader(headers: ProviderHeaders | undefined, name: string): boolean {
	if (!headers) return false;
	const expected = name.toLowerCase();
	for (const [key, value] of Object.entries(headers)) {
		if (key.toLowerCase() === expected && value !== null && value.trim().length > 0) return true;
	}
	return false;
}

function getClientApiKey(provider: string, apiKey: string | undefined, headers: ProviderHeaders | undefined): string {
	if (apiKey) return apiKey;
	if (hasHeader(headers, "authorization") || hasHeader(headers, "cf-aig-authorization")) return "unused";
	throw new Error(`No API key for provider: ${provider}`);
}

function detectSessionAffinityFormat(model: Pick<Model<"openai-responses">, "provider" | "baseUrl">) {
	return model.provider === "openrouter" || model.baseUrl.includes("openrouter.ai") ? "openrouter" : "openai";
}

/**
 * Resolve cache retention preference.
 * Defaults to "short" and uses PI_CACHE_RETENTION for backward compatibility.
 */
function resolveCacheRetention(cacheRetention?: CacheRetention, env?: ProviderEnv): CacheRetention {
	if (cacheRetention) {
		return cacheRetention;
	}
	if (getProviderEnvValue("PI_CACHE_RETENTION", env) === "long") {
		return "long";
	}
	return "short";
}

function getCompat(model: Model<"openai-responses">): Required<OpenAIResponsesCompat> {
	return {
		supportsDeveloperRole: model.compat?.supportsDeveloperRole ?? true,
		supportsResponsesWebSocket: model.compat?.supportsResponsesWebSocket ?? false,
		sessionAffinityFormat: model.compat?.sessionAffinityFormat ?? detectSessionAffinityFormat(model),
		supportsLongCacheRetention: model.compat?.supportsLongCacheRetention ?? true,
		supportsStrictMode: model.compat?.supportsStrictMode ?? false,
		supportsOpenAIGrammarTools: model.compat?.supportsOpenAIGrammarTools ?? false,
		supportsAdditionalTools: model.compat?.supportsAdditionalTools ?? false,
		supportsToolSearch: model.compat?.supportsToolSearch ?? false,
		supportsExplicitPromptCacheMode: model.compat?.supportsExplicitPromptCacheMode ?? false,
	};
}

function getPromptCacheRetention(
	compat: Required<OpenAIResponsesCompat>,
	cacheRetention: CacheRetention,
): "24h" | undefined {
	return cacheRetention === "long" && compat.supportsLongCacheRetention ? "24h" : undefined;
}

function formatOpenAIResponsesError(error: unknown): string {
	return formatProviderError(normalizeProviderError(error), "OpenAI API error");
}

// OpenAI Responses-specific options
export interface OpenAIResponsesOptions extends StreamOptions {
	reasoningEffort?: "minimal" | "low" | "medium" | "high" | "xhigh" | "max";
	reasoningSummary?: "auto" | "detailed" | "concise" | null;
	serviceTier?: ResponseCreateParamsStreaming["service_tier"];
	toolChoice?: ResponseCreateParamsStreaming["tool_choice"];
}

/**
 * Generate function for OpenAI Responses API
 */
export const stream: StreamFunction<"openai-responses", OpenAIResponsesOptions> = (
	model: Model<"openai-responses">,
	context: Context,
	options?: OpenAIResponsesOptions,
): AssistantMessageEventStream => {
	const stream = new AssistantMessageEventStream();

	// Start async processing
	(async () => {
		const output: AssistantMessage = {
			role: "assistant",
			content: [],
			api: model.api as Api,
			provider: model.provider,
			model: model.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "pending",
			timestamp: Date.now(),
		};

		try {
			const apiKey = getClientApiKey(model.provider, options?.apiKey, options?.headers);
			const cacheRetention = resolveCacheRetention(options?.cacheRetention, options?.env);
			const cacheSessionId = cacheRetention === "none" ? undefined : options?.sessionId;
			const compat = getCompat(model);
			const grammarToolInputProperties = createGrammarToolInputProperties(
				context.tools,
				compat.supportsOpenAIGrammarTools,
			);
			const headers = buildRequestHeaders(model, context, options?.headers, cacheSessionId, compat);
			const client = createClient(model, apiKey, headers, options?.fetch);
			let params = buildParams(model, context, options, compat, grammarToolInputProperties);
			const nextParams = await options?.onPayload?.(params, model);
			if (nextParams !== undefined) {
				params = nextParams as ResponseCreateParamsStreaming;
			}

			const transport = options?.transport ?? (compat.supportsResponsesWebSocket ? "auto" : "sse");
			const useWebSocket =
				transport === "websocket" ||
				transport === "websocket-cached" ||
				(transport === "auto" && compat.supportsResponsesWebSocket);
			const useCachedWebSocket =
				cacheSessionId !== undefined && (transport === "websocket-cached" || transport === "auto");
			const websocketHeaders = buildWebSocketHeaders(headers, apiKey);
			const websocketCacheKey = getWebSocketCacheKey(model.baseUrl, websocketHeaders);
			let startEmitted = false;

			if (
				useWebSocket &&
				(transport !== "auto" || !isWebSocketSseFallbackActive(cacheSessionId, websocketCacheKey))
			) {
				let retriedMissingContinuation = false;
				while (true) {
					let websocketStarted = false;
					try {
						await processWebSocketStream({
							client,
							body: params,
							headers: websocketHeaders,
							output,
							stream,
							model,
							grammarToolInputProperties,
							cacheSessionId,
							cacheKey: websocketCacheKey,
							useCachedContext: useCachedWebSocket,
							idleTimeoutMs: options?.timeoutMs,
							connectTimeoutMs: options?.websocketConnectTimeoutMs,
							signal: options?.signal,
							onStart: () => {
								websocketStarted = true;
								if (!startEmitted) {
									startEmitted = true;
									stream.push({ type: "start", partial: output });
								}
							},
						});
						assertSuccessfulOutput(output);
						stream.push({ type: "done", reason: output.stopReason, message: output });
						stream.end();
						return;
					} catch (error) {
						if (
							!websocketStarted &&
							!retriedMissingContinuation &&
							error instanceof OpenAIResponsesWebSocketApiError &&
							error.code === "previous_response_not_found"
						) {
							retriedMissingContinuation = true;
							continue;
						}
						if (
							transport !== "auto" ||
							options?.signal?.aborted ||
							websocketStarted ||
							error instanceof OpenAIResponsesWebSocketApiError
						) {
							throw error;
						}
						recordWebSocketSseFallback(cacheSessionId, websocketCacheKey);
						break;
					}
				}
			}

			const requestOptions = {
				...(options?.signal ? { signal: options.signal } : {}),
				...(options?.timeoutMs !== undefined ? { timeout: options.timeoutMs } : {}),
				maxRetries: 0,
			};
			const { data: openaiStream, response } = await retryProviderRequest(
				() => client.responses.create(params, requestOptions).withResponse(),
				{
					maxRetries: options?.maxRetries,
					maxRetryDelayMs: options?.maxRetryDelayMs,
					signal: options?.signal,
				},
			);
			await options?.onResponse?.({ status: response.status, headers: headersToRecord(response.headers) }, model);
			if (!startEmitted) {
				startEmitted = true;
				stream.push({ type: "start", partial: output });
			}

			await processResponsesStream(openaiStream, output, stream, model, {
				serviceTier: options?.serviceTier,
				grammarToolInputProperties,
				applyServiceTierPricing: (usage, serviceTier) => applyServiceTierPricing(usage, serviceTier, model),
			});
			assertSuccessfulOutput(output);
			stream.push({ type: "done", reason: output.stopReason, message: output });
			stream.end();
		} catch (error) {
			for (const block of output.content) {
				delete (block as { index?: number }).index;
				// Streaming scratch buffers are only used during parsing; never persist them.
				delete (block as { partialJson?: string }).partialJson;
				delete (block as { customInput?: unknown }).customInput;
			}
			output.stopReason = options?.signal?.aborted ? "aborted" : "error";
			output.errorMessage = formatOpenAIResponsesError(error);
			stream.push({ type: "error", reason: output.stopReason, error: output });
			stream.end();
		}
	})();

	return stream;
};

export const streamSimple: StreamFunction<"openai-responses", SimpleStreamOptions> = (
	model: Model<"openai-responses">,
	context: Context,
	options?: SimpleStreamOptions,
): AssistantMessageEventStream => {
	getClientApiKey(model.provider, options?.apiKey, options?.headers);

	const base = {
		...buildBaseOptions(model, context, options, options?.apiKey),
		toolChoice: options?.toolChoice,
	} satisfies OpenAIResponsesOptions;
	const clampedReasoning = options?.reasoning ? clampThinkingLevel(model, options.reasoning) : undefined;
	const reasoningEffort = clampedReasoning === "off" ? undefined : clampedReasoning;

	return stream(model, context, {
		...base,
		reasoningEffort,
	} satisfies OpenAIResponsesOptions);
};

function buildRequestHeaders(
	model: Model<"openai-responses">,
	context: Context,
	optionsHeaders: ProviderHeaders | undefined,
	sessionId: string | undefined,
	compat: Required<OpenAIResponsesCompat>,
): ProviderHeaders {
	const headers: ProviderHeaders = { "User-Agent": getPiUserAgent(), ...model.headers };
	if (model.provider === "github-copilot") {
		Object.assign(
			headers,
			buildCopilotDynamicHeaders({
				messages: context.messages,
				hasImages: hasCopilotVisionInput(context.messages),
			}),
		);
	}

	if (sessionId) {
		if (compat.sessionAffinityFormat === "openrouter") {
			headers["x-session-id"] = sessionId;
		} else {
			if (compat.sessionAffinityFormat === "openai") {
				headers.session_id = sessionId;
			}
			headers["x-client-request-id"] = sessionId;
		}
	}

	if (optionsHeaders) Object.assign(headers, optionsHeaders);
	return headers;
}

function createClient(
	model: Model<"openai-responses">,
	apiKey: string,
	headers: ProviderHeaders,
	fetch?: typeof globalThis.fetch,
) {
	return new OpenAI({
		apiKey,
		baseURL: model.baseUrl,
		dangerouslyAllowBrowser: true,
		fetch,
		defaultHeaders: headers,
	});
}

function buildWebSocketHeaders(headers: ProviderHeaders, apiKey: string): Headers {
	const result = new Headers();
	for (const [name, value] of Object.entries(headers)) {
		if (value === null) continue;
		result.set(name, value);
	}
	result.delete("accept");
	result.delete("content-type");
	result.delete("openai-beta");
	if (!result.has("authorization")) result.set("authorization", `Bearer ${apiKey}`);
	result.set("OpenAI-Beta", OPENAI_RESPONSES_WEBSOCKET_BETA);
	return result;
}

function buildParams(
	model: Model<"openai-responses">,
	context: Context,
	options: OpenAIResponsesOptions | undefined,
	compat: Required<OpenAIResponsesCompat> = getCompat(model),
	grammarToolInputProperties: ReadonlyMap<string, string> = createGrammarToolInputProperties(
		context.tools,
		compat.supportsOpenAIGrammarTools,
	),
) {
	const deferredToolsMode = compat.supportsAdditionalTools
		? "additional-tools"
		: compat.supportsToolSearch
			? "tool-search"
			: undefined;
	const toolPlacement = splitDeferredTools(context, deferredToolsMode !== undefined);
	const messages = convertResponsesMessages(model, context, OPENAI_TOOL_CALL_PROVIDERS, {
		grammarToolInputProperties,
		deferredTools: toolPlacement.deferred,
		deferredToolsMode,
		toolOptions: {
			supportsStrictMode: compat.supportsStrictMode,
			supportsOpenAIGrammarTools: compat.supportsOpenAIGrammarTools,
		},
	});

	const cacheRetention = resolveCacheRetention(options?.cacheRetention, options?.env);
	const disableImplicitPromptCache = cacheRetention === "none" && compat.supportsExplicitPromptCacheMode;
	const params: ResponseCreateParamsStreaming & { prompt_cache_options?: { mode: "explicit" } } = {
		model: model.id,
		input: messages,
		stream: true,
		prompt_cache_key: cacheRetention === "none" ? undefined : clampOpenAIPromptCacheKey(options?.sessionId),
		prompt_cache_retention: getPromptCacheRetention(compat, cacheRetention),
		prompt_cache_options: disableImplicitPromptCache ? { mode: "explicit" } : undefined,
		store: false,
	};

	if (options?.maxTokens) {
		params.max_output_tokens = Math.max(options.maxTokens, OPENAI_RESPONSES_MIN_OUTPUT_TOKENS);
	}

	if (options?.temperature !== undefined) {
		params.temperature = options?.temperature;
	}

	if (options?.serviceTier !== undefined) {
		params.service_tier = options.serviceTier;
	}

	if (toolPlacement.immediate.length > 0) {
		params.tools = convertResponsesTools(toolPlacement.immediate, {
			supportsStrictMode: compat.supportsStrictMode,
			supportsOpenAIGrammarTools: compat.supportsOpenAIGrammarTools,
		});
	}

	if (options?.toolChoice !== undefined) {
		params.tool_choice = options.toolChoice;
	}

	if (model.reasoning) {
		if (options?.reasoningEffort || options?.reasoningSummary) {
			const effort = options?.reasoningEffort
				? (model.thinkingLevelMap?.[options.reasoningEffort] ?? options.reasoningEffort)
				: "medium";
			params.reasoning = {
				effort: effort as NonNullable<typeof params.reasoning>["effort"],
				// Some Responses-compatible gateways expose reasoning token usage but
				// omit the summary when auto is requested. GPT models are expected to
				// show a useful thinking summary in Pi, so request it explicitly.
				summary: options?.reasoningSummary || (/^gpt-/i.test(model.id) ? "detailed" : "auto"),
			};
			params.include = ["reasoning.encrypted_content"];
		} else if (model.provider !== "github-copilot" && model.thinkingLevelMap?.off !== null) {
			params.reasoning = {
				effort: (model.thinkingLevelMap?.off ?? "none") as NonNullable<typeof params.reasoning>["effort"],
			};
		}
		if (model.provider === "xai") params.include = ["reasoning.encrypted_content"];
	}

	// Last so custom keys override the named request fields.
	if (options?.samplingParams) {
		Object.assign(params, options.samplingParams);
	}

	return params;
}

function getServiceTierCostMultiplier(
	model: Pick<Model<"openai-responses">, "id">,
	serviceTier: ResponseCreateParamsStreaming["service_tier"] | undefined,
): number {
	switch (serviceTier) {
		case "flex":
			return 0.5;
		case "priority":
			return model.id === "gpt-5.5" ? 2.5 : 2;
		default:
			return 1;
	}
}

function applyServiceTierPricing(
	usage: Usage,
	serviceTier: ResponseCreateParamsStreaming["service_tier"] | undefined,
	model: Pick<Model<"openai-responses">, "id">,
) {
	const multiplier = getServiceTierCostMultiplier(model, serviceTier);
	if (multiplier === 1) return;

	usage.cost.input *= multiplier;
	usage.cost.output *= multiplier;
	usage.cost.cacheRead *= multiplier;
	usage.cost.cacheWrite *= multiplier;
	usage.cost.total = usage.cost.input + usage.cost.output + usage.cost.cacheRead + usage.cost.cacheWrite;
}

const OPENAI_RESPONSES_WEBSOCKET_BETA = "responses_websockets=2026-02-06";
const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS = 15_000;
const SESSION_WEBSOCKET_CACHE_TTL_MS = 5 * 60 * 1000;
const SESSION_WEBSOCKET_MAX_AGE_MS = 55 * 60 * 1000;

interface CachedWebSocketContinuationState {
	lastRequestBody: ResponseCreateParamsStreaming;
	lastResponseId: string;
	lastResponseItems: ResponseInput;
}

interface CachedWebSocketConnection {
	socket: ResponsesWS;
	busy: boolean;
	createdAt: number;
	idleTimer?: ReturnType<typeof setTimeout>;
	continuation?: CachedWebSocketContinuationState;
}

interface ProcessWebSocketStreamOptions {
	client: OpenAI;
	body: ResponseCreateParamsStreaming;
	headers: Headers;
	output: AssistantMessage;
	stream: AssistantMessageEventStream;
	model: Model<"openai-responses">;
	grammarToolInputProperties: ReadonlyMap<string, string>;
	cacheSessionId?: string;
	cacheKey: string;
	useCachedContext: boolean;
	idleTimeoutMs?: number;
	connectTimeoutMs?: number;
	signal?: AbortSignal;
	onStart: () => void;
}

class OpenAIResponsesWebSocketApiError extends Error {
	readonly code?: string;

	constructor(message: string, code?: string) {
		super(message);
		this.name = "OpenAIResponsesWebSocketApiError";
		this.code = code;
	}
}

const websocketSessionCache = new Map<string, Map<string, CachedWebSocketConnection>>();
const websocketSseFallbackSessions = new Map<string, Set<string>>();

function assertSuccessfulOutput(
	output: AssistantMessage,
): asserts output is AssistantMessage & { stopReason: "deferred" | "length" | "stop" | "toolUse" } {
	if (output.stopReason === "pending") {
		throw new Error("OpenAI Responses stream ended without a stop reason");
	}
	if (output.stopReason === "aborted" || output.stopReason === "error") {
		throw new Error(output.errorMessage || "An unknown error occurred");
	}
}

function getWebSocketCacheKey(baseUrl: string, headers: Headers): string {
	const normalizedHeaders = [...headers.entries()].sort(([a], [b]) => a.localeCompare(b));
	return `${baseUrl.replace(/\/+$/, "")}:${shortHash(JSON.stringify(normalizedHeaders))}`;
}

function isWebSocketSseFallbackActive(sessionId: string | undefined, cacheKey: string): boolean {
	return sessionId !== undefined && websocketSseFallbackSessions.get(sessionId)?.has(cacheKey) === true;
}

function recordWebSocketSseFallback(sessionId: string | undefined, cacheKey: string): void {
	if (!sessionId) return;
	let failed = websocketSseFallbackSessions.get(sessionId);
	if (!failed) {
		failed = new Set();
		websocketSseFallbackSessions.set(sessionId, failed);
	}
	failed.add(cacheKey);
}

function closeWebSocketSilently(socket: ResponsesWS, reason = "done"): void {
	try {
		socket.close({ code: 1000, reason });
	} catch {}
}

function isWebSocketReusable(socket: ResponsesWS): boolean {
	return socket.socket.readyState === 1;
}

function closeOpenAIResponsesWebSocketSessions(sessionId?: string): void {
	const closeEntry = (entry: CachedWebSocketConnection) => {
		if (entry.idleTimer) clearTimeout(entry.idleTimer);
		closeWebSocketSilently(entry.socket, "session_cleanup");
	};
	if (sessionId) {
		for (const entry of websocketSessionCache.get(sessionId)?.values() ?? []) closeEntry(entry);
		websocketSessionCache.delete(sessionId);
		websocketSseFallbackSessions.delete(sessionId);
		return;
	}
	for (const entries of websocketSessionCache.values()) {
		for (const entry of entries.values()) closeEntry(entry);
	}
	websocketSessionCache.clear();
	websocketSseFallbackSessions.clear();
}

registerSessionResourceCleanup(closeOpenAIResponsesWebSocketSessions);

function scheduleWebSocketExpiry(sessionId: string, cacheKey: string, entry: CachedWebSocketConnection): void {
	if (entry.idleTimer) clearTimeout(entry.idleTimer);
	entry.idleTimer = setTimeout(() => {
		if (entry.busy) return;
		closeWebSocketSilently(entry.socket, "idle_timeout");
		const entries = websocketSessionCache.get(sessionId);
		if (entries?.get(cacheKey) === entry) entries.delete(cacheKey);
		if (entries?.size === 0) websocketSessionCache.delete(sessionId);
	}, SESSION_WEBSOCKET_CACHE_TTL_MS);
}

function createWebSocket(client: OpenAI, headers: Headers): ResponsesWS {
	const options: ResponsesWSClientOptions & { headers: Record<string, string> } = {
		headers: headersToRecord(headers),
		reconnect: null,
	};
	return new ResponsesWS(client, options);
}

function acquireWebSocket(
	client: OpenAI,
	headers: Headers,
	sessionId: string | undefined,
	cacheKey: string,
	useCache: boolean,
): { socket: ResponsesWS; entry?: CachedWebSocketConnection; release: (keep: boolean) => void } {
	if (!sessionId || !useCache) {
		const socket = createWebSocket(client, headers);
		return { socket, release: () => closeWebSocketSilently(socket) };
	}

	let entries = websocketSessionCache.get(sessionId);
	const cached = entries?.get(cacheKey);
	if (cached) {
		if (cached.idleTimer) {
			clearTimeout(cached.idleTimer);
			cached.idleTimer = undefined;
		}
		if (!cached.busy && Date.now() - cached.createdAt >= SESSION_WEBSOCKET_MAX_AGE_MS) {
			closeWebSocketSilently(cached.socket, "connection_age_limit");
			entries?.delete(cacheKey);
			if (entries?.size === 0) websocketSessionCache.delete(sessionId);
		} else if (!cached.busy && isWebSocketReusable(cached.socket)) {
			cached.busy = true;
			return {
				socket: cached.socket,
				entry: cached,
				release: (keep) => {
					if (!keep || !isWebSocketReusable(cached.socket)) {
						closeWebSocketSilently(cached.socket);
						const current = websocketSessionCache.get(sessionId);
						if (current?.get(cacheKey) === cached) current.delete(cacheKey);
						if (current?.size === 0) websocketSessionCache.delete(sessionId);
						return;
					}
					cached.busy = false;
					scheduleWebSocketExpiry(sessionId, cacheKey, cached);
				},
			};
		}
		if (cached.busy) {
			const socket = createWebSocket(client, headers);
			return { socket, release: () => closeWebSocketSilently(socket) };
		}
		if (!isWebSocketReusable(cached.socket)) {
			closeWebSocketSilently(cached.socket);
			entries?.delete(cacheKey);
			if (entries?.size === 0) websocketSessionCache.delete(sessionId);
		}
	}

	const socket = createWebSocket(client, headers);
	const entry: CachedWebSocketConnection = { socket, busy: true, createdAt: Date.now() };
	entries = websocketSessionCache.get(sessionId);
	if (!entries) {
		entries = new Map();
		websocketSessionCache.set(sessionId, entries);
	}
	entries.set(cacheKey, entry);
	return {
		socket,
		entry,
		release: (keep) => {
			if (!keep || !isWebSocketReusable(entry.socket)) {
				closeWebSocketSilently(entry.socket);
				if (entry.idleTimer) clearTimeout(entry.idleTimer);
				const current = websocketSessionCache.get(sessionId);
				if (current?.get(cacheKey) === entry) current.delete(cacheKey);
				if (current?.size === 0) websocketSessionCache.delete(sessionId);
				return;
			}
			entry.busy = false;
			scheduleWebSocketExpiry(sessionId, cacheKey, entry);
		},
	};
}

function responseInputItems(input: ResponseCreateParamsStreaming["input"]): ResponseInput | undefined {
	return Array.isArray(input) ? (input as ResponseInput) : undefined;
}

function requestBodiesMatchExceptInput(a: ResponseCreateParamsStreaming, b: ResponseCreateParamsStreaming): boolean {
	const { input: _aInput, previous_response_id: _aPreviousResponseId, ...aRest } = a;
	const { input: _bInput, previous_response_id: _bPreviousResponseId, ...bRest } = b;
	return JSON.stringify(aRest) === JSON.stringify(bRest);
}

function getCachedWebSocketInputDelta(
	body: ResponseCreateParamsStreaming,
	continuation: CachedWebSocketContinuationState,
): ResponseInput | undefined {
	if (!requestBodiesMatchExceptInput(body, continuation.lastRequestBody)) return undefined;
	const currentInput = responseInputItems(body.input);
	const lastInput = responseInputItems(continuation.lastRequestBody.input);
	if (!currentInput || !lastInput) return undefined;
	const baseline = [...lastInput, ...continuation.lastResponseItems];
	if (currentInput.length < baseline.length) return undefined;
	if (JSON.stringify(currentInput.slice(0, baseline.length)) !== JSON.stringify(baseline)) return undefined;
	return currentInput.slice(baseline.length);
}

function buildCachedWebSocketRequestBody(
	entry: CachedWebSocketConnection,
	body: ResponseCreateParamsStreaming,
): ResponseCreateParamsStreaming {
	if (!entry.continuation) return body;
	const delta = getCachedWebSocketInputDelta(body, entry.continuation);
	if (delta === undefined || !entry.continuation.lastResponseId) {
		entry.continuation = undefined;
		return body;
	}
	return { ...body, previous_response_id: entry.continuation.lastResponseId, input: delta };
}

function extractWebSocketApiError(error: unknown): OpenAIResponsesWebSocketApiError | undefined {
	const event = (error as { error?: unknown } | undefined)?.error;
	if (!event || typeof event !== "object") return undefined;
	const details = event as { code?: unknown; message?: unknown; error?: { code?: unknown; message?: unknown } };
	const code =
		typeof details.code === "string"
			? details.code
			: typeof details.error?.code === "string"
				? details.error.code
				: undefined;
	const message =
		typeof details.message === "string"
			? details.message
			: typeof details.error?.message === "string"
				? details.error.message
				: undefined;
	return new OpenAIResponsesWebSocketApiError(
		`OpenAI Responses WebSocket error: ${message || code || JSON.stringify(event)}`,
		code,
	);
}

function isTerminalWebSocketEvent(event: ResponseStreamEvent): boolean {
	const type = (event as { type?: unknown }).type;
	return (
		type === "response.completed" ||
		type === "response.done" ||
		type === "response.incomplete" ||
		type === "response.failed"
	);
}

function normalizeWebSocketEvent(event: unknown): ResponseStreamEvent {
	const record = event as { type?: unknown };
	if (record.type === "response.done") {
		return { ...(event as object), type: "response.completed" } as ResponseStreamEvent;
	}
	return event as ResponseStreamEvent;
}

function waitForWebSocketUpdate<T>(
	next: Promise<IteratorResult<T>>,
	timeoutMs: number | undefined,
	phase: string,
	signal?: AbortSignal,
): Promise<IteratorResult<T>> {
	return new Promise((resolve, reject) => {
		let settled = false;
		let timeout: ReturnType<typeof setTimeout> | undefined;
		const cleanup = () => {
			if (timeout) clearTimeout(timeout);
			signal?.removeEventListener("abort", onAbort);
		};
		const succeed = (value: IteratorResult<T>) => {
			if (settled) return;
			settled = true;
			cleanup();
			resolve(value);
		};
		const fail = (error: Error) => {
			if (settled) return;
			settled = true;
			cleanup();
			reject(error);
		};
		const onAbort = () => fail(new Error("Request was aborted"));
		next.then(succeed, (error) => fail(error instanceof Error ? error : new Error(String(error))));
		if (timeoutMs !== undefined && timeoutMs > 0) {
			timeout = setTimeout(() => fail(new Error(`WebSocket ${phase} timeout after ${timeoutMs}ms`)), timeoutMs);
		}
		signal?.addEventListener("abort", onAbort, { once: true });
		if (signal?.aborted) onAbort();
	});
}
async function* parseWebSocket(
	socket: ResponsesWS,
	body: ResponseCreateParamsStreaming,
	signal: AbortSignal | undefined,
	idleTimeoutMs: number | undefined,
	connectTimeoutMs: number | undefined,
): AsyncGenerator<ResponseStreamEvent> {
	const iterator = socket.stream();
	let opened = false;
	let completed = false;
	try {
		while (true) {
			const timeoutMs = opened ? idleTimeoutMs : (connectTimeoutMs ?? DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS);
			const next = await waitForWebSocketUpdate(iterator.next(), timeoutMs, opened ? "idle" : "connect", signal);
			if (next.done) break;
			const update = next.value;
			if (update.type === "connecting" || update.type === "reconnecting" || update.type === "reconnected") continue;
			if (update.type === "open") {
				opened = true;
				socket.send({ ...body, type: "response.create" } as never);
				continue;
			}
			if (update.type === "error") {
				throw extractWebSocketApiError(update.error) ?? update.error;
			}
			if (update.type === "close") {
				throw new Error(`WebSocket closed ${update.code}${update.reason ? ` ${update.reason}` : ""}`);
			}
			if (update.type !== "message") continue;
			const event = normalizeWebSocketEvent(update.message);
			if ((event as { type?: unknown }).type === "error") {
				throw new OpenAIResponsesWebSocketApiError(`OpenAI Responses WebSocket error: ${JSON.stringify(event)}`);
			}
			yield event;
			if (isTerminalWebSocketEvent(event)) {
				completed = true;
				return;
			}
		}
		if (!completed) throw new Error("WebSocket stream closed before a terminal response event");
	} catch (error) {
		closeWebSocketSilently(socket, "stream_error");
		throw error;
	} finally {
		await iterator.return?.();
	}
}

async function* startWebSocketOutputOnFirstEvent(
	events: AsyncIterable<ResponseStreamEvent>,
	onStart: () => void,
): AsyncGenerator<ResponseStreamEvent> {
	let started = false;
	for await (const event of events) {
		if (!started) {
			started = true;
			onStart();
		}
		yield event;
	}
}

async function processWebSocketStream({
	client,
	body,
	headers,
	output,
	stream,
	model,
	grammarToolInputProperties,
	cacheSessionId,
	cacheKey,
	useCachedContext,
	idleTimeoutMs,
	connectTimeoutMs,
	signal,
	onStart,
}: ProcessWebSocketStreamOptions): Promise<void> {
	const { socket, entry, release } = acquireWebSocket(client, headers, cacheSessionId, cacheKey, useCachedContext);
	let keepConnection = useCachedContext;
	const fullBody = body;
	const requestBody = useCachedContext && entry ? buildCachedWebSocketRequestBody(entry, fullBody) : fullBody;
	try {
		await processResponsesStream(
			startWebSocketOutputOnFirstEvent(
				parseWebSocket(socket, requestBody, signal, idleTimeoutMs, connectTimeoutMs),
				onStart,
			),
			output,
			stream,
			model,
			{
				serviceTier: requestBody.service_tier,
				grammarToolInputProperties,
				applyServiceTierPricing: (usage, serviceTier) => applyServiceTierPricing(usage, serviceTier, model),
			},
		);
		if (signal?.aborted) throw new Error("Request was aborted");
		if (useCachedContext && entry && output.responseId) {
			const responseItems = convertResponsesMessages(model, { messages: [output] }, OPENAI_TOOL_CALL_PROVIDERS, {
				grammarToolInputProperties,
			}).filter((item) => item.type !== "function_call_output" && item.type !== "custom_tool_call_output");
			entry.continuation = {
				lastRequestBody: fullBody,
				lastResponseId: output.responseId,
				lastResponseItems: responseItems,
			};
		}
	} catch (error) {
		if (entry) entry.continuation = undefined;
		keepConnection = false;
		throw error;
	} finally {
		release(keepConnection);
	}
}
