package graphqlshadow

import (
	"errors"
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// OperationType is the kind of GraphQL operation a request would execute.
type OperationType string

const (
	// OperationQuery is a read-only query operation — the ONLY kind eligible for
	// a dark Rust shadow execution.
	OperationQuery OperationType = "query"
	// OperationMutation is a write operation. It must never reach the Rust shadow
	// endpoint, so it is hard-dropped.
	OperationMutation OperationType = "mutation"
	// OperationSubscription is a subscription operation. Not served over POST
	// /graphql, but hard-dropped defensively regardless.
	OperationSubscription OperationType = "subscription"
)

// ErrNoOperation is returned when a document contains no executable operation.
var ErrNoOperation = errors.New("graphqlshadow: document contains no executable operation")

// ClassifyOperation parses a GraphQL request document and returns the type of
// the single operation that would execute for the given operationName, applying
// the GraphQL spec's operation-selection rules:
//
//   - operationName != "": the operation with that name is selected; if no
//     operation has that name it is an error.
//   - operationName == "": if the document defines exactly one operation it is
//     selected; if it defines none, or more than one, it is an error (the spec
//     requires operationName to disambiguate multiple operations).
//
// Any parse failure, or any of the above error conditions, returns a non-nil
// error. Callers enforcing the mutation-double-execute safety gate MUST fail
// closed on a non-nil error (treat it as ineligible / hard-drop) — see
// IsShadowEligible.
//
// The classification is deliberately conservative: it reflects the operation
// the primary Go server selected for the (query, operationName) pair, so a
// document that pairs a query with a mutation is only eligible when
// operationName selects the query.
func ClassifyOperation(query, operationName string) (OperationType, error) {
	_, selected, err := selectOperation(query, operationName)
	if err != nil {
		return "", err
	}

	switch selected.Operation {
	case ast.Query:
		return OperationQuery, nil
	case ast.Mutation:
		return OperationMutation, nil
	case ast.Subscription:
		return OperationSubscription, nil
	default:
		// gqlparser only produces the three operations above; an empty/unknown
		// operation is treated as ineligible rather than assumed read-only.
		return "", fmt.Errorf("graphqlshadow: unknown operation type %q", selected.Operation)
	}
}

// IsComparableSearchOperation reports whether the selected operation is a
// read-only query containing only an unaliased torrentContent.search selection
// (plus optional __typename fields), with either the canonical id or all four
// fields needed to reconstruct each item's stable InferID. Any out-of-scope
// sibling makes the whole operation ineligible: Rust's first dark-soak contract
// is search-only, so sending a mixed operation would contaminate its evidence
// with errors from intentionally unserved fields.
//
// The unaliased requirement matches SearchResultFromResponseData's fixed
// response path. It is conservative by design: aliases and incomplete item
// projections are skipped rather than risking a misleading comparison.
func IsComparableSearchOperation(
	query string,
	operationName string,
	variables map[string]any,
) bool {
	doc, selected, err := selectOperation(query, operationName)
	if err != nil || selected.Operation != ast.Query {
		return false
	}

	foundComparableSearch := false

	for _, field := range selectionFields(doc, selected.SelectionSet, nil) {
		if field.Name == "__typename" {
			continue
		}

		if field.Name != "torrentContent" || field.Alias != field.Name {
			return false
		}

		for _, child := range selectionFields(doc, field.SelectionSet, nil) {
			if child.Name == "__typename" {
				continue
			}

			if child.Name != "search" || child.Alias != child.Name {
				return false
			}

			if !searchSelectionComparable(doc, child, variables) {
				return false
			}

			foundComparableSearch = true
		}
	}

	return foundComparableSearch
}

func selectOperation(query, operationName string) (*ast.QueryDocument, *ast.OperationDefinition, error) {
	doc, err := parser.ParseQuery(&ast.Source{Name: "shadow-request", Input: query})
	if err != nil {
		return nil, nil, fmt.Errorf("graphqlshadow: parse operation document: %w", err)
	}

	if len(doc.Operations) == 0 {
		return nil, nil, ErrNoOperation
	}

	var selected *ast.OperationDefinition

	if operationName == "" {
		if len(doc.Operations) > 1 {
			return nil, nil, fmt.Errorf(
				"graphqlshadow: document defines %d operations but no operationName was provided",
				len(doc.Operations))
		}

		selected = doc.Operations[0]
	} else {
		for _, op := range doc.Operations {
			if op.Name == operationName {
				selected = op

				break
			}
		}

		if selected == nil {
			return nil, nil, fmt.Errorf("graphqlshadow: no operation named %q in document", operationName)
		}
	}

	return doc, selected, nil
}

// IsShadowEligible reports whether a request is a selected read-only query. It
// is the broad operation-type decision point; IsComparableSearchOperation adds
// the narrower response-projection gate used before any Rust call.
func IsShadowEligible(query, operationName string) bool {
	op, err := ClassifyOperation(query, operationName)

	return err == nil && op == OperationQuery
}

// selectionFields expands inline fragments and named fragment spreads into the
// fields visible at one selection depth. visited prevents a malformed cyclic
// fragment graph from recursing forever; valid GraphQL documents cannot contain
// such a cycle, but this gate must fail safely even before schema validation.
func selectionFields(
	doc *ast.QueryDocument,
	set ast.SelectionSet,
	visited map[string]bool,
) []*ast.Field {
	if visited == nil {
		visited = make(map[string]bool)
	}

	var fields []*ast.Field

	for _, selection := range set {
		switch selection := selection.(type) {
		case *ast.Field:
			fields = append(fields, selection)
		case *ast.InlineFragment:
			fields = append(fields, selectionFields(doc, selection.SelectionSet, visited)...)
		case *ast.FragmentSpread:
			if visited[selection.Name] {
				continue
			}

			fragment := doc.Fragments.ForName(selection.Name)
			if fragment == nil {
				continue
			}

			visited[selection.Name] = true
			fields = append(fields, selectionFields(doc, fragment.SelectionSet, visited)...)
		}
	}

	return fields
}

func searchSelectionComparable(
	doc *ast.QueryDocument,
	search *ast.Field,
	variables map[string]any,
) bool {
	input := search.Arguments.ForName("input")
	if input == nil {
		return false
	}

	resolved, err := input.Value.Value(variables)
	if err != nil {
		return false
	}

	inputMap, ok := resolved.(map[string]any)
	if !ok {
		return false
	}

	totalCount, ok := inputMap["totalCount"].(bool)
	if !ok || !totalCount {
		return false
	}

	fields := selectionFields(doc, search.SelectionSet, nil)
	if !containsUnaliasedFields(fields, "totalCount", "totalCountIsEstimate", "items", "aggregations") {
		return false
	}

	var (
		items        []*ast.Field
		aggregations []*ast.Field
	)

	for _, field := range fields {
		if field.Alias != field.Name {
			continue
		}

		switch field.Name {
		case "items":
			items = append(items, field)
		case "aggregations":
			aggregations = append(aggregations, field)
		}
	}

	if !itemSelectionsHaveIdentity(doc, items) {
		return false
	}

	return aggregationSelectionsComplete(doc, aggregations)
}

func itemSelectionsHaveIdentity(doc *ast.QueryDocument, selections []*ast.Field) bool {
	var fields []*ast.Field
	for _, selection := range selections {
		fields = append(fields, selectionFields(doc, selection.SelectionSet, nil)...)
	}

	identity := map[string]bool{
		"id":            false,
		"infoHash":      false,
		"contentType":   false,
		"contentSource": false,
		"contentId":     false,
	}

	for _, field := range fields {
		if _, needed := identity[field.Name]; needed && field.Alias == field.Name {
			identity[field.Name] = true
		}
	}

	return identity["id"] || identity["infoHash"] && identity["contentType"] &&
		identity["contentSource"] && identity["contentId"]
}

func aggregationSelectionsComplete(doc *ast.QueryDocument, selections []*ast.Field) bool {
	var fields []*ast.Field
	for _, selection := range selections {
		fields = append(fields, selectionFields(doc, selection.SelectionSet, nil)...)
	}

	byFacet := make(map[string][]*ast.Field)

	for _, field := range fields {
		if _, known := aggFieldToFacetKey[field.Name]; !known {
			continue
		}

		if field.Alias != field.Name {
			return false
		}

		byFacet[field.Name] = append(byFacet[field.Name], field)
	}

	for _, facetSelections := range byFacet {
		var subfields []*ast.Field
		for _, facet := range facetSelections {
			subfields = append(subfields, selectionFields(doc, facet.SelectionSet, nil)...)
		}

		if !containsUnaliasedFields(subfields, "value", "label", "count", "isEstimate") {
			return false
		}
	}

	return true
}

func containsUnaliasedFields(fields []*ast.Field, required ...string) bool {
	found := make(map[string]bool, len(required))

	for _, field := range fields {
		if field.Alias == field.Name {
			found[field.Name] = true
		}
	}

	for _, name := range required {
		if !found[name] {
			return false
		}
	}

	return true
}
