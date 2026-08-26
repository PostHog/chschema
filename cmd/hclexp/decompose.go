package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

const decomposeStateFile = ".hclexp-decompose.json"
const decomposeNamedCollectionKind = "named_collection"

// decomposeAssignment is deliberately small and durable. The automatic
// policy is suitable for the mechanical majority; overrides record only the
// decisions a human made, so refreshing dumps does not freeze a generated
// inventory into configuration.
type decomposeAssignment struct {
	Version     int                                  `json:"version"`
	BaselineEnv string                               `json:"baseline_env,omitempty"`
	Objects     map[string]decomposeObjectAssignment `json:"objects,omitempty"`
}

type decomposeObjectAssignment struct {
	Mode string `json:"mode"` // auto | shared | environment | exclude
}

type decomposeInventory struct {
	Environments []string                   `json:"environments"`
	Roles        []string                   `json:"roles"`
	ReplicaDrift []decomposeReplicaDrift    `json:"replica_drift"`
	Objects      []decomposeInventoryObject `json:"objects"`
}

type decomposeReplicaDrift struct {
	Environment string `json:"environment"`
	Role        string `json:"role"`
	Reference   string `json:"reference"`
	Node        string `json:"node"`
	Summary     string `json:"summary"`
}

type decomposeInventoryObject struct {
	Key      string   `json:"key"`
	Role     string   `json:"role"`
	Database string   `json:"database"`
	Kind     string   `json:"kind"`
	Name     string   `json:"name"`
	Present  []string `json:"present_in"`
	Uniform  bool     `json:"uniform"`
}

type decomposeSnapshot struct {
	Env    string
	Role   string
	Schema *hclload.Schema
}

type decomposeObject struct {
	Role, Database, Kind, Name string
}

func (o decomposeObject) key() string {
	return strings.Join([]string{o.Role, o.Database, o.Kind, o.Name}, "/")
}

type generatedDecomposition struct {
	Files     map[string][]byte
	Inventory decomposeInventory
}

func runDecompose(args []string) {
	fs := flag.NewFlagSet("hclexp decompose", flag.ExitOnError)
	dumpRoot := fs.String("dump-root", "", "root containing one directory per environment, each with per-node HCL dumps")
	envs := fs.String("env", "", "comma-separated environment directory names (default: every direct child containing HCL files)")
	glob := fs.String("glob", "*", "comma-separated dump filename globs within each environment")
	exclude := fs.String("exclude", "", "HCL exclude config applied to every dump before inventory and emission")
	assignmentPath := fs.String("assignment", "", "optional JSON assignment file; object modes are auto, shared, environment, or exclude")
	out := fs.String("out", "", "output directory for layers, manifest, and composed goldens")
	list := fs.Bool("list", false, "print the cross-environment inventory as JSON without writing layers")
	zkPaths := fs.String("zk-paths", "mask-uuid", "ReplicatedMergeTree zoo_path handling: keep | mask-uuid | ignore")
	_ = fs.Parse(args)

	if *dumpRoot == "" {
		fmt.Fprintln(os.Stderr, "decompose: -dump-root is required")
		os.Exit(2)
	}
	if !*list && *out == "" {
		fmt.Fprintln(os.Stderr, "decompose: -out is required unless -list is used")
		os.Exit(2)
	}
	if *zkPaths != "keep" && *zkPaths != "mask-uuid" && *zkPaths != "ignore" {
		fmt.Fprintf(os.Stderr, "decompose: invalid -zk-paths %q (want keep|mask-uuid|ignore)\n", *zkPaths)
		os.Exit(2)
	}

	assignment, err := readDecomposeAssignment(*assignmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decompose: %v\n", err)
		os.Exit(1)
	}
	snapshots, selectedEnvs, replicaDrift, err := loadDecomposeSnapshots(*dumpRoot, splitList(*envs), *glob, *zkPaths, loadExcludeFlag(*exclude))
	if err != nil {
		fmt.Fprintf(os.Stderr, "decompose: %v\n", err)
		os.Exit(1)
	}
	if !*list && len(replicaDrift) > 0 {
		first := replicaDrift[0]
		fmt.Fprintf(os.Stderr, "decompose: environment %q role %q has intra-env drift between %s and %s: %s (run with -list to inspect all disagreements)\n", first.Environment, first.Role, first.Reference, first.Node, first.Summary)
		os.Exit(1)
	}
	generated, err := buildDecomposition(snapshots, selectedEnvs, assignment)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decompose: %v\n", err)
		os.Exit(1)
	}
	generated.Inventory.ReplicaDrift = replicaDrift
	if *list {
		body, err := json.MarshalIndent(generated.Inventory, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "decompose: render inventory: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
	if err := writeDecomposition(*out, generated.Files); err != nil {
		fmt.Fprintf(os.Stderr, "decompose: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("decomposed %d environments into %s (%d generated files; round-trip verified)\n", len(selectedEnvs), *out, len(generated.Files))
}

func readDecomposeAssignment(path string) (decomposeAssignment, error) {
	assignment := decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}}
	if path == "" {
		return assignment, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return assignment, fmt.Errorf("read assignment %q: %w", path, err)
	}
	if err := json.Unmarshal(body, &assignment); err != nil {
		return assignment, fmt.Errorf("parse assignment %q: %w", path, err)
	}
	if assignment.Version != 1 {
		return assignment, fmt.Errorf("assignment %q: unsupported version %d (want 1)", path, assignment.Version)
	}
	for key, object := range assignment.Objects {
		switch object.Mode {
		case "", "auto", "shared", "environment", "exclude":
		default:
			return assignment, fmt.Errorf("assignment %q: object %q has invalid mode %q", path, key, object.Mode)
		}
	}
	return assignment, nil
}

func loadDecomposeSnapshots(root string, requested []string, glob, zkMode string, exclude *hclload.ExcludeMatcher) ([]decomposeSnapshot, []string, []decomposeReplicaDrift, error) {
	envs := append([]string(nil), requested...)
	if len(envs) == 0 {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read dump root %q: %w", root, err)
		}
		for _, entry := range entries {
			if entry.IsDir() && entry.Name() != ".git" {
				envs = append(envs, entry.Name())
			}
		}
	}
	sort.Strings(envs)
	seenEnv := map[string]bool{}
	var snapshots []decomposeSnapshot
	var replicaDrift []decomposeReplicaDrift
	for _, env := range envs {
		if seenEnv[env] {
			return nil, nil, nil, fmt.Errorf("environment %q selected more than once", env)
		}
		seenEnv[env] = true
		nodes, err := loadDriftNodes(filepath.Join(root, env), glob)
		if err != nil {
			if len(requested) == 0 && errors.Is(rootCause(err), os.ErrNotExist) {
				continue
			}
			return nil, nil, nil, fmt.Errorf("environment %q: %w", env, err)
		}
		if len(nodes) == 0 {
			if len(requested) > 0 {
				return nil, nil, nil, fmt.Errorf("environment %q: no dumps match %q", env, glob)
			}
			continue
		}
		byRole := map[string][]driftNode{}
		for i := range nodes {
			normalizeZKPaths(nodes[i].Schema, zkMode)
			hclload.FilterSchema(nodes[i].Schema, exclude)
			nodes[i].Schema.Nodes = nil
			role := nodes[i].Role
			if role == "" {
				role = nodes[i].Macros["hostClusterRole"]
			}
			if role == "" {
				return nil, nil, nil, fmt.Errorf("environment %q: cannot determine role for dump %s (missing hostClusterRole and filename suffix)", env, nodes[i].File)
			}
			byRole[role] = append(byRole[role], nodes[i])
		}
		roles := sortedKeysLocal(byRole)
		for _, role := range roles {
			nodes := byRole[role]
			sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
			for _, peer := range nodes[1:] {
				if diff := hclload.Diff(nodes[0].Schema, peer.Schema); !diff.IsEmpty() {
					objects := hclload.BuildObjectComparisons(diff, hclload.GenerateSQL(diff), nodes[0].Schema, peer.Schema)
					replicaDrift = append(replicaDrift, decomposeReplicaDrift{
						Environment: env, Role: role, Reference: nodes[0].Name, Node: peer.Name,
						Summary: hclload.SummarizeComparisons(objects).OneLiner(),
					})
				}
			}
			snapshots = append(snapshots, decomposeSnapshot{Env: env, Role: role, Schema: nodes[0].Schema})
		}
	}
	actual := map[string]bool{}
	for _, snapshot := range snapshots {
		actual[snapshot.Env] = true
	}
	envs = sortedKeysLocal(actual)
	if len(envs) == 0 {
		return nil, nil, nil, fmt.Errorf("no environment dumps found under %q", root)
	}
	return snapshots, envs, replicaDrift, nil
}

func rootCause(err error) error {
	for errors.Unwrap(err) != nil {
		err = errors.Unwrap(err)
	}
	return err
}

func buildDecomposition(snapshots []decomposeSnapshot, envs []string, assignment decomposeAssignment) (generatedDecomposition, error) {
	byEnvRole := map[string]map[string]*hclload.Schema{}
	rolesSet := map[string]bool{}
	for _, snapshot := range snapshots {
		if byEnvRole[snapshot.Env] == nil {
			byEnvRole[snapshot.Env] = map[string]*hclload.Schema{}
		}
		byEnvRole[snapshot.Env][snapshot.Role] = snapshot.Schema
		rolesSet[snapshot.Role] = true
	}
	roles := sortedKeysLocal(rolesSet)
	objects := inventoryObjects(snapshots)
	knownObjects := map[string]bool{}
	for _, object := range objects {
		knownObjects[object.key()] = true
	}
	for key := range assignment.Objects {
		if !knownObjects[key] {
			return generatedDecomposition{}, fmt.Errorf("assignment references unknown object %q", key)
		}
	}
	if assignment.BaselineEnv != "" {
		knownEnv := false
		for _, env := range envs {
			knownEnv = knownEnv || env == assignment.BaselineEnv
		}
		if !knownEnv {
			return generatedDecomposition{}, fmt.Errorf("assignment baseline_env %q is not loaded (available: %s)", assignment.BaselineEnv, strings.Join(envs, ", "))
		}
	}
	inventory := decomposeInventory{Environments: envs, Roles: roles}
	layers := map[string]*hclload.Schema{}
	targets := map[string]map[string]*hclload.Schema{}
	for _, env := range envs {
		targets[env] = map[string]*hclload.Schema{}
		for role := range byEnvRole[env] {
			targets[env][role] = &hclload.Schema{}
		}
	}
	for _, object := range objects {
		present := objectPresence(object, envs, byEnvRole)
		uniform := objectUniform(object, present, byEnvRole)
		inventory.Objects = append(inventory.Objects, decomposeInventoryObject{
			Key: object.key(), Role: object.Role, Database: object.Database, Kind: object.Kind,
			Name: object.Name, Present: present, Uniform: uniform,
		})
		mode := assignment.Objects[object.key()].Mode
		if mode == "" {
			mode = "auto"
		}
		if mode == "exclude" {
			continue
		}
		for _, env := range present {
			addObjectToSchema(targets[env][object.Role], byEnvRole[env][object.Role], object)
		}
		roleEnvs := environmentsForRole(object.Role, envs, byEnvRole)
		allPresent := slicesEqual(present, roleEnvs)
		if mode == "shared" && !allPresent {
			return generatedDecomposition{}, fmt.Errorf("assignment %q requests shared placement, but object is absent from: %s", object.key(), strings.Join(sliceDifference(roleEnvs, present), ", "))
		}
		if mode == "environment" || !allPresent {
			for _, env := range present {
				addObjectToSchema(layer(layers, envLayerPath(env, object.Role)), byEnvRole[env][object.Role], object)
			}
			continue
		}

		baseline := chooseBaseline(assignment.BaselineEnv, present)
		baseObject := objectSchema(byEnvRole[baseline][object.Role], object)
		if uniform {
			addObjectToSchema(layer(layers, sharedLayerPath(object.Role)), baseObject, object)
			continue
		}
		if object.Kind != hclload.KindTable {
			if mode == "shared" {
				return generatedDecomposition{}, fmt.Errorf("assignment %q requests shared placement, but differing %s objects cannot yet be represented as an exact patch; use mode environment", object.key(), object.Kind)
			}
			for _, env := range present {
				addObjectToSchema(layer(layers, envLayerPath(env, object.Role)), byEnvRole[env][object.Role], object)
			}
			continue
		}

		patches := map[string]hclload.PatchTableSpec{}
		patchable := true
		var patchErr error
		for _, env := range present {
			if env == baseline {
				continue
			}
			patch, err := tablePatch(baseObject, objectSchema(byEnvRole[env][object.Role], object), object)
			if err != nil {
				patchable, patchErr = false, err
				break
			}
			patches[env] = patch
		}
		if !patchable {
			if mode == "shared" {
				return generatedDecomposition{}, patchErr
			}
			for _, env := range present {
				addObjectToSchema(layer(layers, envLayerPath(env, object.Role)), byEnvRole[env][object.Role], object)
			}
			continue
		}
		addObjectToSchema(layer(layers, sharedLayerPath(object.Role)), baseObject, object)
		for env, patch := range patches {
			if patchTableEmpty(patch) {
				continue
			}
			addTablePatch(layer(layers, envLayerPath(env, object.Role)), object.Database, patch)
		}
	}

	files := map[string][]byte{}
	for path, schema := range layers {
		var body bytes.Buffer
		if err := hclload.WriteLayer(&body, schema); err != nil {
			return generatedDecomposition{}, fmt.Errorf("render %s: %w", path, err)
		}
		files[path] = body.Bytes()
	}
	files["manifest.hcl"] = renderDecomposeManifest(envs, roles, byEnvRole, layers)
	for _, env := range envs {
		for _, role := range roles {
			target := targets[env][role]
			if target == nil {
				continue
			}
			var body bytes.Buffer
			if err := hclload.Write(&body, target); err != nil {
				return generatedDecomposition{}, err
			}
			files[filepath.ToSlash(filepath.Join("goldens", env, role+".hcl"))] = body.Bytes()
		}
	}
	if err := verifyGeneratedDecomposition(files, envs, roles, targets); err != nil {
		return generatedDecomposition{}, err
	}
	return generatedDecomposition{Files: files, Inventory: inventory}, nil
}

func tablePatch(from, to *hclload.Schema, object decomposeObject) (hclload.PatchTableSpec, error) {
	diff := hclload.Diff(from, to)
	if len(diff.Databases) != 1 || len(diff.Databases[0].AlterTables) != 1 {
		return hclload.PatchTableSpec{}, fmt.Errorf("%s: expected one table delta", object.key())
	}
	td := diff.Databases[0].AlterTables[0]
	unsupported := []string{}
	if len(td.RenameColumns) > 0 {
		unsupported = append(unsupported, "column rename")
	}
	if td.ColumnOrderChange != nil {
		unsupported = append(unsupported, fmt.Sprintf("existing-column reorder %v -> %v", td.ColumnOrderChange.Old, td.ColumnOrderChange.New))
	}
	if td.PrimaryKeyChange != nil {
		unsupported = append(unsupported, "primary_key")
	}
	if td.CommentChange != nil {
		unsupported = append(unsupported, "comment")
	}
	if len(td.AddConstraints)+len(td.DropConstraints)+len(td.ModifyConstraints) > 0 {
		unsupported = append(unsupported, "constraints")
	}
	if len(td.DropProjections) > 0 {
		unsupported = append(unsupported, "projection removal/replacement")
	}
	if len(td.SettingsRemoved) > 0 {
		unsupported = append(unsupported, "settings removal")
	}
	if td.OrderByChange != nil && len(td.OrderByChange.New) == 0 {
		unsupported = append(unsupported, "clearing order_by")
	}
	for name, change := range map[string]*hclload.StringChange{"partition_by": td.PartitionByChange, "sample_by": td.SampleByChange, "ttl": td.TTLChange} {
		if change != nil && change.New == nil {
			unsupported = append(unsupported, "clearing "+name)
		}
	}
	for _, change := range td.ModifyColumns {
		if change.IsUnsafe() {
			unsupported = append(unsupported, "column storage-class change "+change.Name)
		}
	}
	if len(unsupported) > 0 {
		return hclload.PatchTableSpec{}, fmt.Errorf("%s cannot be split into shared base plus patch without repositioning or loss: %s", object.key(), strings.Join(unsupported, ", "))
	}
	fromTable := onlyTable(from)
	toTable := onlyTable(to)
	positionedIndexes, err := positionPatchIndexes(fromTable.Indexes, toTable.Indexes, td.AddIndexes, td.DropIndexes)
	if err != nil {
		return hclload.PatchTableSpec{}, fmt.Errorf("%s cannot be split into shared base plus patch without repositioning or loss: %w", object.key(), err)
	}
	if err := projectionAppendOnly(fromTable.Projections, toTable.Projections, td.AddProjections, td.DropProjections); err != nil {
		return hclload.PatchTableSpec{}, fmt.Errorf("%s cannot be split into shared base plus patch without repositioning or loss: %w", object.key(), err)
	}
	patch := hclload.PatchTableSpec{Name: object.Name, Columns: td.AddColumns, DropColumns: td.DropColumns,
		Indexes: positionedIndexes, DropIndexes: td.DropIndexes, Projections: td.AddProjections,
		Settings: map[string]string{}}
	for _, change := range td.ModifyColumns {
		patch.ModifyColumns = append(patch.ModifyColumns, change.New)
	}
	if td.EngineChange != nil {
		patch.Engine = &hclload.EngineSpec{Kind: td.EngineChange.New.Kind(), Decoded: td.EngineChange.New}
	}
	if td.OrderByChange != nil {
		patch.OrderBy = append([]string(nil), td.OrderByChange.New...)
	}
	if td.PartitionByChange != nil {
		patch.PartitionBy = td.PartitionByChange.New
	}
	if td.SampleByChange != nil {
		patch.SampleBy = td.SampleByChange.New
	}
	if td.TTLChange != nil {
		patch.TTL = td.TTLChange.New
	}
	for key, value := range td.SettingsAdded {
		patch.Settings[key] = value
	}
	for _, change := range td.SettingsChanged {
		patch.Settings[change.Key] = change.NewValue
	}
	if len(patch.Settings) == 0 {
		patch.Settings = nil
	}
	return patch, nil
}

func onlyTable(schema *hclload.Schema) *hclload.TableSpec {
	return &schema.Databases[0].Tables[0]
}

func positionPatchIndexes(from, to, additions []hclload.IndexSpec, drops []string) ([]hclload.IndexSpec, error) {
	dropped := map[string]bool{}
	for _, name := range drops {
		dropped[name] = true
	}
	current := []string{}
	for _, index := range from {
		if !dropped[index.Name] {
			current = append(current, index.Name)
		}
	}
	addByName := map[string]hclload.IndexSpec{}
	for _, index := range additions {
		addByName[index.Name] = index
	}
	commonOld := []string{}
	for _, name := range current {
		if _, replaced := addByName[name]; !replaced {
			commonOld = append(commonOld, name)
		}
	}
	commonNew := []string{}
	for _, index := range to {
		if containsString(commonOld, index.Name) {
			commonNew = append(commonNew, index.Name)
		}
	}
	if !slicesEqual(commonOld, commonNew) {
		return nil, fmt.Errorf("existing-index reorder %v -> %v", commonOld, commonNew)
	}

	var positioned []hclload.IndexSpec
	for targetPos, target := range to {
		index, added := addByName[target.Name]
		if !added {
			continue
		}
		needsPosition := false
		for _, later := range to[targetPos+1:] {
			if containsString(current, later.Name) || addByName[later.Name].Name != "" {
				needsPosition = true
				break
			}
		}
		if needsPosition {
			predecessor := ""
			for i := targetPos - 1; i >= 0; i-- {
				if containsString(current, to[i].Name) {
					predecessor = to[i].Name
					break
				}
			}
			if predecessor == "" {
				index.First = true
			} else {
				index.After = &predecessor
			}
		}
		positioned = append(positioned, index)
		insertAt := len(current)
		if index.First {
			insertAt = 0
		} else if index.After != nil {
			for i, name := range current {
				if name == *index.After {
					insertAt = i + 1
					break
				}
			}
		}
		current = append(current, "")
		copy(current[insertAt+1:], current[insertAt:])
		current[insertAt] = index.Name
	}
	return positioned, nil
}

func projectionAppendOnly(from, to, additions []hclload.ProjectionSpec, drops []string) error {
	if len(drops) > 0 {
		return fmt.Errorf("projection removal/replacement")
	}
	if len(additions) == 0 {
		return nil
	}
	if len(to) < len(from) {
		return fmt.Errorf("projection order changed")
	}
	for i := range from {
		if from[i].Name != to[i].Name {
			return fmt.Errorf("projection addition is interleaved before %q", from[i].Name)
		}
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func patchTableEmpty(p hclload.PatchTableSpec) bool {
	return len(p.Columns)+len(p.ModifyColumns)+len(p.DropColumns)+len(p.Indexes)+len(p.DropIndexes)+len(p.Projections) == 0 &&
		p.OrderBy == nil && p.PartitionBy == nil && p.SampleBy == nil && p.TTL == nil && len(p.Settings) == 0 && p.Engine == nil
}

func verifyGeneratedDecomposition(files map[string][]byte, envs, roles []string, targets map[string]map[string]*hclload.Schema) error {
	root, err := os.MkdirTemp("", "hclexp-decompose-verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	for path, body := range files {
		if strings.HasPrefix(path, "goldens/") || path == "manifest.hcl" {
			continue
		}
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, body, 0o644); err != nil {
			return err
		}
	}
	for _, env := range envs {
		for _, role := range roles {
			target := targets[env][role]
			if target == nil {
				continue
			}
			paths := []string{filepath.Join(root, sharedLayerPath(role)), filepath.Join(root, envLayerPath(env, role))}
			var existing []string
			for _, path := range paths {
				if _, err := os.Stat(path); err == nil {
					existing = append(existing, path)
				}
			}
			loaded, err := hclload.LoadLayers(existing)
			if err != nil {
				return fmt.Errorf("round-trip %s/%s load: %w", env, role, err)
			}
			if err := hclload.Resolve(loaded); err != nil {
				return fmt.Errorf("round-trip %s/%s resolve: %w", env, role, err)
			}
			if diff := hclload.Diff(loaded, target); !diff.IsEmpty() {
				objects := hclload.BuildObjectComparisons(diff, hclload.GenerateSQL(diff), loaded, target)
				return fmt.Errorf("round-trip %s/%s differs from dump: %s", env, role, hclload.SummarizeComparisons(objects).OneLiner())
			}
			var composedHCL, targetHCL bytes.Buffer
			if err := hclload.Write(&composedHCL, loaded); err != nil {
				return fmt.Errorf("round-trip %s/%s render composition: %w", env, role, err)
			}
			if err := hclload.Write(&targetHCL, target); err != nil {
				return fmt.Errorf("round-trip %s/%s render target: %w", env, role, err)
			}
			if !bytes.Equal(composedHCL.Bytes(), targetHCL.Bytes()) {
				return fmt.Errorf("round-trip %s/%s canonical HCL differs from dump (collection or physical ordering mismatch)", env, role)
			}
		}
	}
	return nil
}

func writeDecomposition(out string, files map[string][]byte) error {
	previous := []string{}
	statePath := filepath.Join(out, decomposeStateFile)
	if body, err := os.ReadFile(statePath); err == nil {
		_ = json.Unmarshal(body, &previous)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read generated-file state: %w", err)
	}
	current := sortedKeysLocal(files)
	currentSet := map[string]bool{}
	for _, path := range current {
		currentSet[path] = true
	}
	for _, path := range previous {
		if currentSet[path] {
			continue
		}
		full, err := safeOutputPath(out, path)
		if err != nil {
			return err
		}
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale generated file %s: %w", path, err)
		}
	}
	for _, path := range current {
		full, err := safeOutputPath(out, path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		tmp, err := os.CreateTemp(filepath.Dir(full), ".decompose-*")
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		ok := false
		defer func() {
			if !ok {
				_ = os.Remove(tmpName)
			}
		}()
		if _, err = tmp.Write(files[path]); err == nil {
			err = tmp.Chmod(0o644)
		}
		if closeErr := tmp.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(tmpName, full)
		}
		if err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		ok = true
	}
	state, _ := json.MarshalIndent(current, "", "  ")
	state = append(state, '\n')
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(statePath, state, 0o644); err != nil {
		return fmt.Errorf("write generated-file state: %w", err)
	}
	return nil
}

func safeOutputPath(root, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("generated path %q is absolute", rel)
	}
	path := filepath.Join(root, filepath.FromSlash(rel))
	check, err := filepath.Rel(root, path)
	if err != nil || check == ".." || strings.HasPrefix(check, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("generated path %q escapes output root", rel)
	}
	return path, nil
}

func inventoryObjects(snapshots []decomposeSnapshot) []decomposeObject {
	seen := map[string]decomposeObject{}
	for _, snapshot := range snapshots {
		for _, collection := range snapshot.Schema.NamedCollections {
			addInventoryObject(seen, decomposeObject{snapshot.Role, "_cluster", decomposeNamedCollectionKind, collection.Name})
		}
		for _, db := range snapshot.Schema.Databases {
			for _, table := range db.Tables {
				addInventoryObject(seen, decomposeObject{snapshot.Role, db.Name, hclload.KindTable, table.Name})
			}
			for _, mv := range db.MaterializedViews {
				addInventoryObject(seen, decomposeObject{snapshot.Role, db.Name, hclload.KindMaterializedView, mv.Name})
			}
			for _, view := range db.Views {
				addInventoryObject(seen, decomposeObject{snapshot.Role, db.Name, hclload.KindView, view.Name})
			}
			for _, dictionary := range db.Dictionaries {
				addInventoryObject(seen, decomposeObject{snapshot.Role, db.Name, hclload.KindDictionary, dictionary.Name})
			}
			for _, raw := range db.Raws {
				addInventoryObject(seen, decomposeObject{snapshot.Role, db.Name, hclload.KindRaw + ":" + raw.Kind, raw.Name})
			}
		}
	}
	keys := sortedKeysLocal(seen)
	out := make([]decomposeObject, 0, len(keys))
	for _, key := range keys {
		out = append(out, seen[key])
	}
	return out
}

func addInventoryObject(seen map[string]decomposeObject, object decomposeObject) {
	seen[object.key()] = object
}

func objectPresence(object decomposeObject, envs []string, schemas map[string]map[string]*hclload.Schema) []string {
	var present []string
	for _, env := range envs {
		if objectExists(schemas[env][object.Role], object) {
			present = append(present, env)
		}
	}
	return present
}

func objectUniform(object decomposeObject, envs []string, schemas map[string]map[string]*hclload.Schema) bool {
	if len(envs) < 2 {
		return true
	}
	first := objectSchema(schemas[envs[0]][object.Role], object)
	for _, env := range envs[1:] {
		if !hclload.Diff(first, objectSchema(schemas[env][object.Role], object)).IsEmpty() {
			return false
		}
	}
	return true
}

func objectExists(schema *hclload.Schema, object decomposeObject) bool {
	if schema == nil {
		return false
	}
	return objectSchema(schema, object) != nil
}

func objectSchema(schema *hclload.Schema, object decomposeObject) *hclload.Schema {
	if schema == nil {
		return nil
	}
	out := &hclload.Schema{}
	addObjectToSchema(out, schema, object)
	if len(out.Databases) == 0 && len(out.NamedCollections) == 0 {
		return nil
	}
	return out
}

func addObjectToSchema(out, source *hclload.Schema, object decomposeObject) {
	if out == nil || source == nil {
		return
	}
	if object.Kind == decomposeNamedCollectionKind {
		for _, collection := range source.NamedCollections {
			if collection.Name == object.Name {
				out.NamedCollections = append(out.NamedCollections, collection)
				return
			}
		}
		return
	}
	for _, db := range source.Databases {
		if db.Name != object.Database {
			continue
		}
		switch {
		case object.Kind == hclload.KindTable:
			for _, value := range db.Tables {
				if value.Name == object.Name {
					target := ensureDatabase(out, db.Name)
					target.Tables = append(target.Tables, value)
					return
				}
			}
		case object.Kind == hclload.KindMaterializedView:
			for _, value := range db.MaterializedViews {
				if value.Name == object.Name {
					target := ensureDatabase(out, db.Name)
					target.MaterializedViews = append(target.MaterializedViews, value)
					return
				}
			}
		case object.Kind == hclload.KindView:
			for _, value := range db.Views {
				if value.Name == object.Name {
					target := ensureDatabase(out, db.Name)
					target.Views = append(target.Views, value)
					return
				}
			}
		case object.Kind == hclload.KindDictionary:
			for _, value := range db.Dictionaries {
				if value.Name == object.Name {
					target := ensureDatabase(out, db.Name)
					target.Dictionaries = append(target.Dictionaries, value)
					return
				}
			}
		case strings.HasPrefix(object.Kind, hclload.KindRaw+":"):
			rawKind := strings.TrimPrefix(object.Kind, hclload.KindRaw+":")
			for _, value := range db.Raws {
				if value.Name == object.Name && value.Kind == rawKind {
					target := ensureDatabase(out, db.Name)
					target.Raws = append(target.Raws, value)
					return
				}
			}
		}
	}
}

func ensureDatabase(schema *hclload.Schema, name string) *hclload.DatabaseSpec {
	for i := range schema.Databases {
		if schema.Databases[i].Name == name {
			return &schema.Databases[i]
		}
	}
	schema.Databases = append(schema.Databases, hclload.DatabaseSpec{Name: name})
	return &schema.Databases[len(schema.Databases)-1]
}

func addTablePatch(schema *hclload.Schema, database string, patch hclload.PatchTableSpec) {
	db := ensureDatabase(schema, database)
	db.Patches = append(db.Patches, patch)
}

func layer(layers map[string]*hclload.Schema, path string) *hclload.Schema {
	if layers[path] == nil {
		layers[path] = &hclload.Schema{}
	}
	return layers[path]
}

func sharedLayerPath(role string) string {
	return filepath.ToSlash(filepath.Join("layers", "shared", role, "tables.hcl"))
}
func envLayerPath(env, role string) string {
	return filepath.ToSlash(filepath.Join("layers", "env", env, role, "patches.hcl"))
}

func environmentsForRole(role string, envs []string, schemas map[string]map[string]*hclload.Schema) []string {
	var out []string
	for _, env := range envs {
		if schemas[env][role] != nil {
			out = append(out, env)
		}
	}
	return out
}

func chooseBaseline(preferred string, present []string) string {
	for _, env := range present {
		if env == preferred {
			return env
		}
	}
	return present[0]
}

func renderDecomposeManifest(envs, roles []string, schemas map[string]map[string]*hclload.Schema, layers map[string]*hclload.Schema) []byte {
	var body strings.Builder
	for _, role := range roles {
		fmt.Fprintf(&body, "role %q {\n", role)
		for _, env := range envs {
			if schemas[env][role] == nil {
				continue
			}
			paths := []string{}
			if layers[sharedLayerPath(role)] != nil {
				paths = append(paths, filepath.ToSlash(filepath.Dir(sharedLayerPath(role))))
			}
			if layers[envLayerPath(env, role)] != nil {
				paths = append(paths, filepath.ToSlash(filepath.Dir(envLayerPath(env, role))))
			}
			fmt.Fprintf(&body, "  env %q { layers = [", env)
			for i, path := range paths {
				if i > 0 {
					body.WriteString(", ")
				}
				fmt.Fprintf(&body, "%q", path)
			}
			body.WriteString("] }\n")
		}
		body.WriteString("}\n\n")
	}
	return []byte(body.String())
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sliceDifference(a, b []string) []string {
	seen := map[string]bool{}
	for _, value := range b {
		seen[value] = true
	}
	var out []string
	for _, value := range a {
		if !seen[value] {
			out = append(out, value)
		}
	}
	return out
}

func sortedKeysLocal[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
