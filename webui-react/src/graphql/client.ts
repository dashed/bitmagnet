import { GraphQLClient } from "graphql-request";
import type { Variables } from "graphql-request";

import type { TypedDocumentString } from "./generated/graphql";

function getGraphqlEndpoint() {
  if (import.meta.env.DEV) {
    return "http://localhost:3333/graphql";
  }

  return `${window.location.protocol}//${window.location.host}/graphql`;
}

export const graphqlEndpoint = getGraphqlEndpoint();

export const graphqlClient = new GraphQLClient(graphqlEndpoint);

export function execute<TResult, TVariables extends Variables>(
  document: TypedDocumentString<TResult, TVariables>,
  variables: TVariables,
  signal?: AbortSignal,
) {
  // graphql-request v7's VariablesAndRequestHeadersArgs conditional type rejects a
  // plain generic TVariables argument; the string-document overload is typed for
  // us by TypedDocumentString, so the cast is sound.
  return graphqlClient.request<TResult>({
    document: document.toString(),
    signal,
    variables: variables as Variables,
  });
}
