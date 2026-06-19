package search

// FeatureFlagsConfig is the "search_features" config section binding the
// DROP-gate / search-migration toggles to env vars (SEARCH_FEATURES_*, e.g.
// SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB=true). Every flag is OFF by
// default, so a fresh install / an unset env behaves exactly as upstream.
//
// It is registered with configfx.NewConfigModule in databasefx and applied to
// the package-level snapshot by ApplyFeatureFlags.
type FeatureFlagsConfig struct {
	GateFileExtensionsJSONB bool
	PopularitySortDefault   bool
	FileBrowserFromBlob     bool
	FileSearchEnabled       bool
}

// NewDefaultFeatureFlagsConfig returns the safe, all-OFF default.
func NewDefaultFeatureFlagsConfig() FeatureFlagsConfig {
	return FeatureFlagsConfig{
		GateFileExtensionsJSONB: false,
		PopularitySortDefault:   false,
		FileBrowserFromBlob:     false,
		FileSearchEnabled:       false,
	}
}

func (c FeatureFlagsConfig) flags() FeatureFlags {
	return FeatureFlags(c)
}

// ApplyFeatureFlags publishes the resolved config to the package-level snapshot.
// It is an fx.Invoke target wired in databasefx; it runs once at startup.
func ApplyFeatureFlags(c FeatureFlagsConfig) {
	SetFeatureFlags(c.flags())
}
