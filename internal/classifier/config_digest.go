package classifier

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"go.uber.org/zap"
)

const effectiveConfigDigestVersion = 1

// EffectiveConfigDigest returns a versioned digest of the effective classifier
// behavior selected by the resolved source and configured default workflow.
//
// Source.Schema and execution-only settings such as Config.Concurrency are
// intentionally excluded: neither changes classification results.
func EffectiveConfigDigest(source Source, defaultWorkflow string) (string, error) {
	document := effectiveConfigDocument{
		Version:         effectiveConfigDigestVersion,
		DefaultWorkflow: defaultWorkflow,
		Source: effectiveConfigSource{
			Workflows:       source.Workflows,
			FlagDefinitions: source.FlagDefinitions,
			Flags:           source.Flags,
			Keywords:        source.Keywords,
			Extensions:      source.Extensions,
		},
	}
	if err := validateEffectiveConfigDigestValue(reflect.ValueOf(document)); err != nil {
		return "", err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	// Keep the byte contract language-neutral. encoding/json's default HTML
	// escaping would rewrite CEL operators such as && and >, unlike serde_json.
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(document); err != nil {
		return "", err
	}

	encodedBytes := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	sum := sha256.Sum256(encodedBytes)

	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func logEffectiveConfigDigest(
	logger *zap.SugaredLogger,
	source Source,
	defaultWorkflow string,
) (string, error) {
	digest, err := EffectiveConfigDigest(source, defaultWorkflow)
	if err != nil {
		return "", err
	}
	if logger != nil {
		logger.Infow(
			"classifier runner initialized",
			"effective_config_digest", digest,
			"default_workflow", defaultWorkflow,
		)
	}

	return digest, nil
}

func validateEffectiveConfigDigestValue(value reflect.Value) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}

		return validateEffectiveConfigDigestValue(value.Elem())
	}

	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		return fmt.Errorf(
			"effective classifier config digest v%d does not support floating-point values",
			effectiveConfigDigestVersion,
		)
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf(
				"effective classifier config digest v%d requires string mapping keys",
				effectiveConfigDigestVersion,
			)
		}
		iter := value.MapRange()
		for iter.Next() {
			if err := validateEffectiveConfigDigestValue(iter.Value()); err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		for i := range value.Len() {
			if err := validateEffectiveConfigDigestValue(value.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := range value.NumField() {
			if err := validateEffectiveConfigDigestValue(value.Field(i)); err != nil {
				return err
			}
		}
	}

	return nil
}

type effectiveConfigDocument struct {
	Version         int                   `json:"version"`
	DefaultWorkflow string                `json:"default_workflow"`
	Source          effectiveConfigSource `json:"source"`
}

type effectiveConfigSource struct {
	Workflows       workflowSources `json:"workflows"`
	FlagDefinitions flagDefinitions `json:"flag_definitions"`
	Flags           Flags           `json:"flags"`
	Keywords        keywordGroups   `json:"keywords"`
	Extensions      extensionGroups `json:"extensions"`
}
