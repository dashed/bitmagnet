// Command dht_runtime_lifecycle freezes deterministic Go DHT runtime defaults
// and lifecycle structure for cross-language parity tests.
//
// The command deliberately does not start the production server: doing so would
// bind a UDP socket and make goroutine/timing observations nondeterministic. It
// calls the public production constructors that are safe without I/O, and uses
// the Go AST to validate otherwise-private construction and lifecycle facts.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
)

const (
	fixtureRelativePath = "bitmagnet-rs/fixtures/dht_runtime_lifecycle.json"
	nodeIDSamples       = 64
)

type fixture struct {
	SchemaVersion int         `json:"schemaVersion"`
	Subsystem     string      `json:"subsystem"`
	Evidence      evidence    `json:"evidence"`
	Defaults      defaults    `json:"defaults"`
	Identity      identity    `json:"identity"`
	Lifecycle     lifecycle   `json:"lifecycle"`
	Limitations   limitations `json:"limitations"`
}

type evidence struct {
	Generator       string   `json:"generator"`
	Mode            string   `json:"mode"`
	ProductionFiles []string `json:"productionFiles"`
}

type defaults struct {
	ConfigNamespace                 string `json:"configNamespace"`
	BindIP                          string `json:"bindIp"`
	BindPort                        uint16 `json:"bindPort"`
	BindAddrPort                    string `json:"bindAddrPort"`
	QueryTimeoutNanos               int64  `json:"queryTimeoutNanos"`
	ResponderTimeoutNanos           int64  `json:"responderTimeoutNanos"`
	SampleInfoHashesIntervalSeconds int64  `json:"sampleInfoHashesIntervalSeconds"`
}

type identity struct {
	Provider                   string `json:"provider"`
	TotalBytes                 int    `json:"totalBytes"`
	RandomPrefixBytes          int    `json:"randomPrefixBytes"`
	SuffixOffsetBytes          int    `json:"suffixOffsetBytes"`
	SuffixASCII                string `json:"suffixAscii"`
	SuffixHex                  string `json:"suffixHex"`
	SamplesChecked             int    `json:"samplesChecked"`
	AllSamplesMatchSuffixShape bool   `json:"allSamplesMatchSuffixShape"`
}

type lifecycle struct {
	ConstructionIsLazy             bool           `json:"constructionIsLazy"`
	StartTrigger                   string         `json:"startTrigger"`
	StopBeforeInitializationIsNoOp bool           `json:"stopBeforeInitializationIsNoOp"`
	SocketOpenBeforeGoroutines     bool           `json:"socketOpenBeforeGoroutines"`
	StopMechanism                  string         `json:"stopMechanism"`
	StopIsIdempotent               bool           `json:"stopIsIdempotent"`
	SecondStopPanics               bool           `json:"secondStopPanics"`
	ShutdownWorkerDetached         bool           `json:"shutdownWorkerDetached"`
	ReadLoopDetached               bool           `json:"readLoopDetached"`
	QueryHandlersDetached          bool           `json:"queryHandlersDetached"`
	ResponseHandlersDetached       bool           `json:"responseHandlersDetached"`
	StopWaitsForReadLoop           bool           `json:"stopWaitsForReadLoop"`
	StopWaitsForHandlers           bool           `json:"stopWaitsForHandlers"`
	SocketCloseErrorIgnored        bool           `json:"socketCloseErrorIgnored"`
	ActiveReceiveErrorPolicy       string         `json:"activeReceiveErrorPolicy"`
	PendingQueries                 pendingQueries `json:"pendingQueries"`
}

type pendingQueries struct {
	RegistryInitiallyEmpty      bool     `json:"registryInitiallyEmpty"`
	ResponseChannelCapacity     int      `json:"responseChannelCapacity"`
	CleanupOnlyWhenQueryReturns bool     `json:"cleanupOnlyWhenQueryReturns"`
	StopTouchesRegistry         bool     `json:"stopTouchesRegistry"`
	StopClosesResponseChannels  bool     `json:"stopClosesResponseChannels"`
	QuerySelectInputs           []string `json:"querySelectInputs"`
	StopSignalSelectedByQuery   bool     `json:"stopSignalSelectedByQuery"`
}

type limitations struct {
	SocketOpened            bool   `json:"socketOpened"`
	NetworkUsed             bool   `json:"networkUsed"`
	GoroutinesStarted       bool   `json:"goroutinesStarted"`
	TimingObserved          bool   `json:"timingObserved"`
	LifecycleEvidenceClass  string `json:"lifecycleEvidenceClass"`
	DetachedCompletionOrder string `json:"detachedCompletionOrder"`
	PendingAtStopCount      string `json:"pendingAtStopCount"`
}

type parsedSource struct {
	path string
	fset *token.FileSet
	file *ast.File
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "dht_runtime_lifecycle:", err)
		os.Exit(1)
	}
}

func run() error {
	rootFlag := flag.String("root", "", "repository root (defaults to walking up from the current directory)")
	outFlag := flag.String("out", fixtureRelativePath, "fixture path, relative to repository root")
	writeFlag := flag.Bool("write", false, "replace the fixture instead of checking it")
	printFlag := flag.Bool("print", false, "write the generated fixture to stdout")
	flag.Parse()

	root, err := resolveRoot(*rootFlag)
	if err != nil {
		return err
	}
	data, err := generate(root)
	if err != nil {
		return err
	}
	if *printFlag {
		if _, err := os.Stdout.Write(data); err != nil {
			return fmt.Errorf("write generated fixture to stdout: %w", err)
		}
	}

	fixturePath := *outFlag
	if !filepath.IsAbs(fixturePath) {
		fixturePath = filepath.Join(root, fixturePath)
	}
	if *writeFlag {
		if err := os.MkdirAll(filepath.Dir(fixturePath), 0o755); err != nil {
			return fmt.Errorf("create fixture directory: %w", err)
		}
		if err := os.WriteFile(fixturePath, data, 0o644); err != nil {
			return fmt.Errorf("write fixture: %w", err)
		}
		if _, err := fmt.Fprintf(os.Stdout, "wrote %s sha256=%x\n", fixturePath, sha256.Sum256(data)); err != nil {
			return fmt.Errorf("report written fixture: %w", err)
		}

		return nil
	}

	want, err := os.ReadFile(fixturePath)
	if err != nil {
		return fmt.Errorf("read fixture (run with -write to create it): %w", err)
	}
	if !bytes.Equal(want, data) {
		return errors.New("fixture is stale; run with -write and review the byte-for-byte diff")
	}
	if _, err := fmt.Fprintf(os.Stdout, "fresh %s sha256=%x\n", fixturePath, sha256.Sum256(data)); err != nil {
		return fmt.Errorf("report fresh fixture: %w", err)
	}

	return nil
}

func generate(root string) ([]byte, error) {
	serverFactory, err := parseSource(root, "internal/protocol/dht/server/factory.go")
	if err != nil {
		return nil, err
	}
	serverRuntime, err := parseSource(root, "internal/protocol/dht/server/server.go")
	if err != nil {
		return nil, err
	}
	responderFactory, err := parseSource(root, "internal/protocol/dht/responder/factory.go")
	if err != nil {
		return nil, err
	}
	protocolID, err := parseSource(root, "internal/protocol/id.go")
	if err != nil {
		return nil, err
	}
	dhtModule, err := parseSource(root, "internal/protocol/dht/dhtfx/module.go")
	if err != nil {
		return nil, err
	}
	lazySource, err := parseSource(root, "internal/lazy/lazy.go")
	if err != nil {
		return nil, err
	}

	config := server.NewDefaultConfig()
	responderTimeout, err := keyedDuration(serverFactory, "responderTimeout")
	if err != nil {
		return nil, err
	}
	sampleInterval, err := keyedInt(responderFactory, "sampleInfoHashesInterval")
	if err != nil {
		return nil, err
	}
	suffix, err := constString(protocolID, "idClientPart")
	if err != nil {
		return nil, err
	}
	configNamespace, err := configModuleNamespace(dhtModule)
	if err != nil {
		return nil, err
	}

	if err := validateServerFactory(serverFactory); err != nil {
		return nil, err
	}
	if err := validateLazy(lazySource); err != nil {
		return nil, err
	}
	runtimeFacts, err := inspectServerRuntime(serverRuntime)
	if err != nil {
		return nil, err
	}
	if err := validateIdentityWiring(dhtModule); err != nil {
		return nil, err
	}

	identityShape, err := inspectIdentity(suffix)
	if err != nil {
		return nil, err
	}
	bindIP := "0.0.0.0"
	result := fixture{
		SchemaVersion: 1,
		Subsystem:     "dht_runtime_lifecycle",
		Evidence: evidence{
			Generator: "tools/parity/dht_runtime_lifecycle",
			Mode:      "production public calls plus Go AST structural validation",
			ProductionFiles: []string{
				"internal/lazy/lazy.go",
				"internal/protocol/dht/dhtfx/module.go",
				"internal/protocol/dht/responder/factory.go",
				"internal/protocol/dht/server/config.go",
				"internal/protocol/dht/server/factory.go",
				"internal/protocol/dht/server/server.go",
				"internal/protocol/id.go",
			},
		},
		Defaults: defaults{
			ConfigNamespace:                 configNamespace,
			BindIP:                          bindIP,
			BindPort:                        config.Port,
			BindAddrPort:                    fmt.Sprintf("%s:%d", bindIP, config.Port),
			QueryTimeoutNanos:               config.QueryTimeout.Nanoseconds(),
			ResponderTimeoutNanos:           responderTimeout.Nanoseconds(),
			SampleInfoHashesIntervalSeconds: sampleInterval,
		},
		Identity:  identityShape,
		Lifecycle: runtimeFacts,
		Limitations: limitations{
			SocketOpened:            false,
			NetworkUsed:             false,
			GoroutinesStarted:       false,
			TimingObserved:          false,
			LifecycleEvidenceClass:  "source-derived AST invariants; not behaviorally executed",
			DetachedCompletionOrder: "not asserted",
			PendingAtStopCount:      "not observed; only absence of stop-side registry draining is asserted",
		},
	}

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal fixture: %w", err)
	}
	return append(data, '\n'), nil
}

func inspectIdentity(suffix string) (identity, error) {
	var totalBytes int
	allMatch := true
	for range nodeIDSamples {
		id := protocol.RandomNodeIDWithClientSuffix()
		bytes := id.Bytes()
		if totalBytes == 0 {
			totalBytes = len(bytes)
		}
		if len(bytes) != totalBytes || len(bytes) < len(suffix) || string(bytes[len(bytes)-len(suffix):]) != suffix {
			allMatch = false
		}
	}
	if !allMatch {
		return identity{}, errors.New("production node ID samples do not match the source-derived suffix shape")
	}
	prefixBytes := totalBytes - len(suffix)
	return identity{
		Provider:                   "protocol.RandomNodeIDWithClientSuffix",
		TotalBytes:                 totalBytes,
		RandomPrefixBytes:          prefixBytes,
		SuffixOffsetBytes:          prefixBytes,
		SuffixASCII:                suffix,
		SuffixHex:                  hex.EncodeToString([]byte(suffix)),
		SamplesChecked:             nodeIDSamples,
		AllSamplesMatchSuffixShape: true,
	}, nil
}

func validateServerFactory(source parsedSource) error {
	newFunction, err := function(source, "", "New")
	if err != nil {
		return err
	}
	text, err := formatted(source, newFunction)
	if err != nil {
		return err
	}
	return requireFragments(source.path, text,
		"lazy.New(func() (Server, error)",
		"make(chan struct{})",
		"localAddr: netip.AddrPortFrom(",
		"netip.IPv4Unspecified(),",
		"p.Config.Port,",
		"make(map[string]pendingQuery)",
		"p.Config.QueryTimeout",
		"if err := s.start(); err != nil",
		"return ls.IfInitialized(func(s Server) error",
		"s.stop()",
	)
}

func validateLazy(source parsedSource) error {
	get, err := function(source, "lazy", "Get")
	if err != nil {
		return err
	}
	getText, err := formatted(source, get)
	if err != nil {
		return err
	}
	ifInitialized, err := function(source, "lazy", "IfInitialized")
	if err != nil {
		return err
	}
	ifText, err := formatted(source, ifInitialized)
	if err != nil {
		return err
	}
	if err := requireFragments(source.path, getText, "if !l.done", "l.v, l.err = l.fn()", "l.done = true"); err != nil {
		return err
	}
	return requireFragments(source.path, ifText, "if l.done && l.err == nil", "return fn(l.v)", "return nil")
}

func validateIdentityWiring(source parsedSource) error {
	newFunction, err := function(source, "", "New")
	if err != nil {
		return err
	}
	text, err := formatted(source, newFunction)
	if err != nil {
		return err
	}
	return requireFragments(source.path, text,
		`"dht_node_id"`,
		"protocol.RandomNodeIDWithClientSuffix",
	)
}

func inspectServerRuntime(source parsedSource) (lifecycle, error) {
	start, err := function(source, "server", "start")
	if err != nil {
		return lifecycle{}, err
	}
	startText, err := formatted(source, start)
	if err != nil {
		return lifecycle{}, err
	}
	if err := requireFragments(source.path, startText,
		"s.socket.Open(s.localAddr)",
		"go func()",
		"context.WithCancel(context.Background())",
		"go s.read(ctx)",
		"<-s.stopped",
		"cancel()",
		"_ = s.socket.Close()",
	); err != nil {
		return lifecycle{}, err
	}
	openOffset := strings.Index(startText, "s.socket.Open(s.localAddr)")
	goOffset := strings.Index(startText, "go func()")
	if openOffset < 0 || goOffset < 0 || openOffset >= goOffset {
		return lifecycle{}, fmt.Errorf("%s: socket open is no longer structurally before goroutine launch", source.path)
	}

	stop, err := function(source, "server", "stop")
	if err != nil {
		return lifecycle{}, err
	}
	stopText, err := formatted(source, stop)
	if err != nil {
		return lifecycle{}, err
	}
	if len(stop.Body.List) != 1 || !strings.Contains(stopText, "close(s.stopped)") {
		return lifecycle{}, fmt.Errorf("%s: stop is no longer the unguarded close-only implementation", source.path)
	}

	read, err := function(source, "server", "read")
	if err != nil {
		return lifecycle{}, err
	}
	readText, err := formatted(source, read)
	if err != nil {
		return lifecycle{}, err
	}
	if err := requireFragments(source.path, readText,
		"if ctx.Err() == nil",
		`panic(fmt.Errorf("socket read error: %w", err))`,
		"go s.handleQuery(ctx, recvMsg)",
		"go s.handleResponse(recvMsg)",
	); err != nil {
		return lifecycle{}, err
	}

	query, err := function(source, "server", "Query")
	if err != nil {
		return lifecycle{}, err
	}
	queryText, err := formatted(source, query)
	if err != nil {
		return lifecycle{}, err
	}
	if err := requireFragments(source.path, queryText,
		"ch := make(chan dht.RecvMsg, 1)",
		"s.queries[transactionID] = pendingQuery{ch: ch, addr: addr}",
		"defer (func()",
		"delete(s.queries, transactionID)",
		"queryCtx, cancel := context.WithTimeout(ctx, s.queryTimeout)",
		"case <-queryCtx.Done()",
		"case res, ok := <-ch",
	); err != nil {
		return lifecycle{}, err
	}
	if strings.Contains(queryText, "<-s.stopped") {
		return lifecycle{}, fmt.Errorf("%s: Query now selects the stop signal; update the oracle classification", source.path)
	}
	if strings.Contains(stopText, ".queries") || strings.Contains(stopText, "range") {
		return lifecycle{}, fmt.Errorf("%s: stop now touches pending queries; update the oracle classification", source.path)
	}
	if strings.Contains(startText, ".queries") {
		return lifecycle{}, fmt.Errorf("%s: start now touches pending queries; update the oracle classification", source.path)
	}
	wholeFile, err := formatted(source, source.file)
	if err != nil {
		return lifecycle{}, err
	}
	if count := strings.Count(wholeFile, "delete(s.queries, transactionID)"); count != 1 {
		return lifecycle{}, fmt.Errorf("%s: expected one pending-registry delete, found %d", source.path, count)
	}

	return lifecycle{
		ConstructionIsLazy:             true,
		StartTrigger:                   "first lazy.Get attempt; initialization result is cached",
		StopBeforeInitializationIsNoOp: true,
		SocketOpenBeforeGoroutines:     true,
		StopMechanism:                  "unguarded close(stopped)",
		StopIsIdempotent:               false,
		SecondStopPanics:               true,
		ShutdownWorkerDetached:         true,
		ReadLoopDetached:               true,
		QueryHandlersDetached:          true,
		ResponseHandlersDetached:       true,
		StopWaitsForReadLoop:           false,
		StopWaitsForHandlers:           false,
		SocketCloseErrorIgnored:        true,
		ActiveReceiveErrorPolicy:       "panic in detached read goroutine while context is active",
		PendingQueries: pendingQueries{
			RegistryInitiallyEmpty:      true,
			ResponseChannelCapacity:     1,
			CleanupOnlyWhenQueryReturns: true,
			StopTouchesRegistry:         false,
			StopClosesResponseChannels:  false,
			QuerySelectInputs:           []string{"query_context", "response_channel"},
			StopSignalSelectedByQuery:   false,
		},
	}, nil
}

func parseSource(root, relative string) (parsedSource, error) {
	path := filepath.Join(root, relative)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return parsedSource{}, fmt.Errorf("parse %s: %w", relative, err)
	}
	return parsedSource{path: relative, fset: fset, file: file}, nil
}

func function(source parsedSource, receiver, name string) (*ast.FuncDecl, error) {
	for _, declaration := range source.file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || receiverName(fn) != receiver {
			continue
		}
		return fn, nil
	}
	return nil, fmt.Errorf("%s: function %s.%s not found", source.path, receiver, name)
}

func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	receiver := fn.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if indexed, ok := receiver.(*ast.IndexExpr); ok {
		receiver = indexed.X
	}
	if indexed, ok := receiver.(*ast.IndexListExpr); ok {
		receiver = indexed.X
	}
	if identifier, ok := receiver.(*ast.Ident); ok {
		return identifier.Name
	}
	return ""
}

func formatted(source parsedSource, node ast.Node) (string, error) {
	var output bytes.Buffer
	if err := format.Node(&output, source.fset, node); err != nil {
		return "", fmt.Errorf("format AST node from %s: %w", source.path, err)
	}
	return output.String(), nil
}

func requireFragments(path, text string, fragments ...string) error {
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			return fmt.Errorf("%s: required AST fragment missing: %q", path, fragment)
		}
	}
	return nil
}

func keyedDuration(source parsedSource, key string) (time.Duration, error) {
	expression, err := uniqueKeyedExpression(source, key)
	if err != nil {
		return 0, err
	}
	return durationExpression(source.path, expression)
}

func keyedInt(source parsedSource, key string) (int64, error) {
	expression, err := uniqueKeyedExpression(source, key)
	if err != nil {
		return 0, err
	}
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.INT {
		return 0, fmt.Errorf("%s: %s is no longer an integer literal", source.path, key)
	}
	value, err := strconv.ParseInt(literal.Value, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: parse %s: %w", source.path, key, err)
	}
	return value, nil
}

func uniqueKeyedExpression(source parsedSource, key string) (ast.Expr, error) {
	var matches []ast.Expr
	ast.Inspect(source.file, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		identifier, ok := entry.Key.(*ast.Ident)
		if ok && identifier.Name == key {
			matches = append(matches, entry.Value)
		}
		return true
	})
	if len(matches) != 1 {
		return nil, fmt.Errorf("%s: expected one %s keyed expression, found %d", source.path, key, len(matches))
	}
	return matches[0], nil
}

func durationExpression(path string, expression ast.Expr) (time.Duration, error) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		integer, err := strconv.ParseInt(value.Value, 0, 64)
		return time.Duration(integer), err
	case *ast.SelectorExpr:
		packageName, ok := value.X.(*ast.Ident)
		if !ok || packageName.Name != "time" {
			break
		}
		switch value.Sel.Name {
		case "Nanosecond":
			return time.Nanosecond, nil
		case "Microsecond":
			return time.Microsecond, nil
		case "Millisecond":
			return time.Millisecond, nil
		case "Second":
			return time.Second, nil
		case "Minute":
			return time.Minute, nil
		}
	case *ast.BinaryExpr:
		left, leftErr := durationExpression(path, value.X)
		right, rightErr := scalarExpression(value.Y)
		if leftErr == nil && rightErr == nil {
			switch value.Op {
			case token.MUL:
				return left * time.Duration(right), nil
			case token.QUO:
				return left / time.Duration(right), nil
			}
		}
		rightDuration, rightDurationErr := durationExpression(path, value.Y)
		leftScalar, leftScalarErr := scalarExpression(value.X)
		if rightDurationErr == nil && leftScalarErr == nil && value.Op == token.MUL {
			return time.Duration(leftScalar) * rightDuration, nil
		}
	}
	return 0, fmt.Errorf("%s: unsupported duration AST expression", path)
}

func scalarExpression(expression ast.Expr) (int64, error) {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.INT {
		return 0, errors.New("not an integer literal")
	}
	return strconv.ParseInt(literal.Value, 0, 64)
}

func constString(source parsedSource, name string) (string, error) {
	for _, declaration := range source.file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, specification := range general.Specs {
			valueSpec, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for index, identifier := range valueSpec.Names {
				if identifier.Name != name || index >= len(valueSpec.Values) {
					continue
				}
				literal, ok := valueSpec.Values[index].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return "", fmt.Errorf("%s: %s is no longer a string literal", source.path, name)
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					return "", fmt.Errorf("%s: unquote %s: %w", source.path, name, err)
				}
				return value, nil
			}
		}
	}
	return "", fmt.Errorf("%s: const %s not found", source.path, name)
}

func configModuleNamespace(source parsedSource) (string, error) {
	newFunction, err := function(source, "", "New")
	if err != nil {
		return "", err
	}
	var values []string
	ast.Inspect(newFunction, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.IndexExpr)
		if !ok {
			return true
		}
		selected, ok := selector.X.(*ast.SelectorExpr)
		if !ok || selected.Sel.Name != "NewConfigModule" || len(call.Args) == 0 {
			return true
		}
		literal, ok := call.Args[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, unquoteErr := strconv.Unquote(literal.Value)
		if unquoteErr == nil {
			values = append(values, value)
		}
		return true
	})
	if len(values) != 1 {
		return "", fmt.Errorf("%s: expected one config module namespace, found %d", source.path, len(values))
	}
	return values[0], nil
}

func resolveRoot(explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	directory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		goMod := filepath.Join(directory, "go.mod")
		data, readErr := os.ReadFile(goMod)
		if readErr == nil && strings.Contains(string(data), "module github.com/bitmagnet-io/bitmagnet") {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", errors.New("could not locate bitmagnet repository root")
		}
		directory = parent
	}
}
