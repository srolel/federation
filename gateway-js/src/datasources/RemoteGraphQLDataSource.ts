import { GraphQLDataSourceProcessOptions } from '@apollo/gateway';
import { GraphQLDataSourceRequestKind } from '@apollo/gateway/dist/datasources/types';
import { ResponsePath } from '@apollo/query-planner';
import {
  GatewayCacheHint,
  GatewayCachePolicy,
  GatewayGraphQLRequest,
  GatewayGraphQLRequestContext,
  GatewayGraphQLResponse,
} from '@apollo/server-gateway-interface';
import { Fetcher, FetcherRequestInit, FetcherResponse } from '@apollo/utils.fetcher';
import { createHash } from 'crypto';
import { GraphQLError, GraphQLErrorExtensions } from 'graphql';
import { isObject } from 'lodash';
import { Headers as FetchHeaders, Request as NodeFetchRequest } from 'node-fetch';
import { RequestContext } from '../../requestNamespace';

export class RemoteGraphQLDataSource {
  constructor(
    protected config: {
      // whether to honor the cache-control header sent from the client or specified in the query
      honorSubgraphCacheControlHeader?: boolean;
      url: string;
      apq?: boolean;
      fetcher: Fetcher;
    }
  ) {}

  async sendRequest(
    options: GraphQLDataSourceProcessOptions<RequestContext>
  ): Promise<{ request: GatewayGraphQLRequest; response: GatewayGraphQLResponse }> {
    const { request, context } = options;

    if (!request.query) {
      throw new Error('Missing query');
    }

    const { query, ...requestWithoutQuery } = request;

    // If APQ was enabled, we'll run the same request again, but add in the
    // previously omitted `query`.  If APQ was NOT enabled, this is the first
    // request (non-APQ, all the way).
    if (this.config.apq) {
      const apqHash = createHash('sha256').update(request.query).digest('hex');

      // Take the original extensions and extend them with
      // the necessary "extensions" for APQ handshaking.
      requestWithoutQuery.extensions = {
        ...request.extensions,
        persistedQuery: {
          version: 1,
          sha256Hash: apqHash,
        },
      };

      const apqOptimisticResponse = await this._sendRequest(requestWithoutQuery, context as RequestContext);

      // If we didn't receive notice to retry with APQ, then let's
      // assume this is the best result we'll get and return it!
      if (
        !apqOptimisticResponse.errors ||
        !apqOptimisticResponse.errors.find((error) => error.message === 'PersistedQueryNotFound')
      ) {
        return { request: requestWithoutQuery, response: apqOptimisticResponse };
      }
    }

    // If APQ was enabled, we'll run the same request again, but add in the
    // previously omitted `query`.  If APQ was NOT enabled, this is the first
    // request (non-APQ, all the way).
    const requestWithQuery: GatewayGraphQLRequest = {
      query,
      ...requestWithoutQuery,
    };

    const response = await this._sendRequest(requestWithQuery, context as RequestContext);

    return { request: requestWithQuery, response };
  }

  public willSendRequest?(options: GraphQLDataSourceProcessOptions<RequestContext>): void | Promise<void>;

  toFetchHeaders(headers?: { [Symbol.iterator](): Iterator<[string, string]> } | [string, string][]): FetchHeaders {
    const fetchHeaders = new FetchHeaders();
    if (headers) {
      for (const [key, value] of headers) {
        fetchHeaders.append(key, value);
      }
    }
    return fetchHeaders;
  }

  async process(options: GraphQLDataSourceProcessOptions<RequestContext>): Promise<GatewayGraphQLResponse> {
    // Respect incoming http headers (eg, apollo-federation-include-trace).
    const headers = this.toFetchHeaders(options.request.http?.headers);
    headers.set('Content-Type', 'application/json');

    options.request.http = {
      method: 'POST',
      url: this.config.url,
      headers,
    };

    this.willSendRequest?.(options);

    if (!options.request.query) {
      throw new Error('Missing query');
    }

    // Special handling of cache-control headers in response. Requires
    // Apollo Server 3, so we check to make sure the method we want is
    // there.
    const overallCachePolicy =
      this.config.honorSubgraphCacheControlHeader &&
      options.kind === GraphQLDataSourceRequestKind.INCOMING_OPERATION &&
      options.incomingRequestContext.overallCachePolicy &&
      'restrict' in options.incomingRequestContext.overallCachePolicy
        ? options.incomingRequestContext.overallCachePolicy
        : null;

    const { request, response } = await this.sendRequest(options);
    return this.respond({
      response,
      request,
      context: options.context as RequestContext,
      overallCachePolicy,
      pathInIncomingRequest:
        options.kind === GraphQLDataSourceRequestKind.INCOMING_OPERATION ? options.pathInIncomingRequest : undefined,
    });
  }

  private async _sendRequest(request: GatewayGraphQLRequest, context: RequestContext): Promise<GatewayGraphQLResponse> {
    // This would represent an internal programming error since this shouldn't
    // be possible in the way that this method is invoked right now.
    if (!request.http) {
      throw new Error("Internal error: Only 'http' requests are supported.");
    }

    // We don't want to serialize the `http` properties into the body that is
    // being transmitted.  Instead, we want those to be used to indicate what
    // we're accessing (e.g. url) and what we access it with (e.g. headers).
    const { http, ...requestWithoutHttp } = request;
    const stringifiedRequestWithoutHttp = JSON.stringify(requestWithoutHttp);
    const requestInit: FetcherRequestInit = {
      method: http.method,
      headers: Object.fromEntries(http.headers),
      body: stringifiedRequestWithoutHttp,
    };
    // Note that we don't actually send this Request object to the fetcher; it
    // is merely sent to methods on this object that might be overridden by users.
    // We are careful to only send data to the overridable fetcher function that uses
    // plain JS objects --- some fetch implementations don't know how to handle
    // Request or Headers objects created by other fetch implementations.
    const fetchRequest = new NodeFetchRequest(http.url, requestInit);

    let fetchResponse: FetcherResponse | undefined;

    try {
      // Use our local `fetcher` to allow for fetch injection
      fetchResponse = await this.config.fetcher(http.url, requestInit);

      if (!fetchResponse?.ok) {
        throw await this.errorFromResponse(fetchResponse);
      }

      const body = await this.parseBody(fetchResponse, fetchRequest, context);

      if (!isObject(body)) {
        throw new Error(`Expected JSON response body, but received: ${body}`);
      }

      return {
        ...body,
        http: fetchResponse,
      };
    } catch (error) {
      this.didEncounterError(error as Error, fetchRequest, fetchResponse, context);
      throw error;
    }
  }

  parseCacheControlHeader(header: string | null | undefined): Record<string, string | true> {
    const cc: Record<string, string | true> = {};
    if (!header) return cc;

    const parts = header.trim().split(/\s*,\s*/);
    for (const part of parts) {
      const [k, v] = part.split(/\s*=\s*/, 2);
      cc[k] = v === undefined ? true : v.replace(/^"|"$/g, '');
    }

    return cc;
  }

  private async respond({
    response,
    request,
    context,
    overallCachePolicy,
    pathInIncomingRequest,
  }: {
    response: GatewayGraphQLResponse;
    request: GatewayGraphQLRequest;
    context: RequestContext;
    overallCachePolicy: GatewayCachePolicy | null;
    pathInIncomingRequest?: ResponsePath;
  }): Promise<GatewayGraphQLResponse> {
    const processedResponse =
      typeof this.didReceiveResponse === 'function'
        ? await this.didReceiveResponse({ response, request, context, pathInIncomingRequest })
        : response;

    if (overallCachePolicy) {
      const parsed = this.parseCacheControlHeader(response.http?.headers.get('cache-control'));

      const hint: GatewayCacheHint = {};
      const maxAge = parsed['max-age'];
      if (typeof maxAge === 'string' && maxAge.match(/^[0-9]+$/)) {
        hint.maxAge = +maxAge;
      }
      if (parsed['public'] === true) {
        hint.scope = 'PUBLIC';
      }
      if (parsed['private'] === true) {
        hint.scope = 'PRIVATE';
      }
      overallCachePolicy.restrict(hint);
    }

    return processedResponse;
  }

  public didReceiveResponse?(
    requestContext: Required<Pick<GatewayGraphQLRequestContext<RequestContext>, 'request' | 'response' | 'context'>> & {
      pathInIncomingRequest?: ResponsePath;
    }
  ): GatewayGraphQLResponse | Promise<GatewayGraphQLResponse>;

  public didEncounterError(
    error: Error,
    _fetchRequest: NodeFetchRequest,
    _fetchResponse?: FetcherResponse,
    _context?: RequestContext
  ) {
    throw error;
  }

  public parseBody(
    fetchResponse: FetcherResponse,
    _fetchRequest?: NodeFetchRequest,
    _context?: RequestContext
  ): Promise<object | string> {
    const contentType = fetchResponse.headers.get('Content-Type');
    if (contentType && contentType.startsWith('application/json')) {
      return fetchResponse.json();
    } else {
      return fetchResponse.text();
    }
  }

  public async errorFromResponse(response: FetcherResponse) {
    const body = await this.parseBody(response);

    const extensions: GraphQLErrorExtensions = {
      response: {
        url: response.url,
        status: response.status,
        statusText: response.statusText,
        body,
      },
    };

    if (response.status === 401) {
      extensions.code = 'UNAUTHENTICATED';
    } else if (response.status === 403) {
      extensions.code = 'FORBIDDEN';
    }

    return new GraphQLError(`${response.status}: ${response.statusText}`, {
      extensions,
    });
  }
}
