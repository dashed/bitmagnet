package classifier

type Config struct {
	Workflow    string
	Keywords    map[string][]string
	Extensions  map[string][]string
	Flags       map[string]any
	DeleteXxx   bool
	Concurrency int
	// TapeDir enables observation recording and names the directory the tape is
	// written to. Empty -- the default -- disables recording entirely: no
	// session is ever placed on a classification's context, so every seam stays
	// on its normal path.
	//
	// Recording is an offline evidence-gathering mode used to build a replay
	// oracle for a port of the classifier. It is not something a serving
	// deployment should have set.
	TapeDir string
	// TapeMaxRecords bounds how many classifications a recording run captures
	// before it stops. Reaching the cap writes the tape and marks it truncated.
	TapeMaxRecords int
	// TapePlanPath and TapePlanSHA256 opt into a reviewed, digest-pinned
	// acquisition plan that seeds synthetic evidence records before normal
	// classifier traffic starts. Both must be set together, and TapeDir must be
	// enabled. With both empty the plan facility is completely inactive.
	TapePlanPath   string
	TapePlanSHA256 string
}

func NewDefaultConfig() Config {
	return Config{
		Workflow:    "default",
		Concurrency: 10,
	}
}
