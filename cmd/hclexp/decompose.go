package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	Mode string   `json:"mode"` // auto | shared | group | environment | exclude
	Envs []string `json:"envs,omitempty"`
	Name string   `json:"name,omitempty"`
}

type decomposeGroup struct {
	Name string
	Envs []string
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
	assignmentPath := fs.String("assignment", "", "optional JSON assignment file; object modes are auto, shared, group, environment, or exclude")
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
		if err := validateDecomposeObjectAssignment(key, object); err != nil {
			return assignment, fmt.Errorf("assignment %q: %w", path, err)
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
				diff := hclload.Diff(nodes[0].Schema, peer.Schema)
				equal, err := canonicalSchemasEqual(nodes[0].Schema, peer.Schema)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("environment %q role %q: compare canonical dumps: %w", env, role, err)
				}
				if !equal {
					objects := hclload.BuildObjectComparisons(diff, hclload.GenerateSQL(diff), nodes[0].Schema, peer.Schema)
					summary := "canonical HCL differs"
					if !diff.IsEmpty() {
						summary = hclload.SummarizeComparisons(objects).OneLiner()
					}
					replicaDrift = append(replicaDrift, decomposeReplicaDrift{
						Environment: env, Role: role, Reference: nodes[0].Name, Node: peer.Name,
						Summary: summary,
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
	groupsByObject, groups, err := resolveDecomposeGroups(assignment.Objects, envs)
	if err != nil {
		return generatedDecomposition{}, err
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
		objectAssignment := assignment.Objects[object.key()]
		mode := objectAssignment.Mode
		if mode == "" {
			mode = "auto"
		}
		if mode == "exclude" {
			continue
		}
		for _, env := range present {
			addObjectToSchema(targets[env][object.Role], byEnvRole[env][object.Role], object)
		}
		if mode == "group" {
			group := groupsByObject[object.key()]
			if !sameStringSet(present, group.Envs) {
				return generatedDecomposition{}, fmt.Errorf("assignment %q requests group %q in environments [%s], but object is present in [%s]",
					object.key(), group.Name, strings.Join(group.Envs, ", "), strings.Join(present, ", "))
			}
			if !uniform && (object.Kind != hclload.KindTable || !tableObjectUniformIgnoringSettings(object, group.Envs, byEnvRole)) {
				return generatedDecomposition{}, fmt.Errorf("assignment %q requests group %q, but object differs between member environments: %s",
					object.key(), group.Name, strings.Join(group.Envs, ", "))
			}
			baseObject := objectSchema(byEnvRole[group.Envs[0]][object.Role], object)
			patches := map[string]func(*hclload.Schema){}
			if object.Kind == hclload.KindTable {
				setTableSettingsIntersection(baseObject, object, group.Envs, byEnvRole)
				for _, env := range group.Envs {
					apply, err := objectPatch(baseObject, objectSchema(byEnvRole[env][object.Role], object), object)
					if err != nil {
						return generatedDecomposition{}, fmt.Errorf("assignment %q cannot synthesize group %q table settings base: %w", object.key(), group.Name, err)
					}
					patches[env] = apply
				}
			}
			addObjectToSchema(layer(layers, groupLayerPath(group.Name, object.Role)), baseObject, object)
			for env, apply := range patches {
				if apply != nil {
					apply(layer(layers, envLayerPath(env, object.Role)))
				}
			}
			continue
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
		if object.Kind == hclload.KindTable {
			setTableSettingsIntersection(baseObject, object, present, byEnvRole)
		}
		if uniform {
			addObjectToSchema(layer(layers, sharedLayerPath(object.Role)), baseObject, object)
			continue
		}
		patches := map[string]func(*hclload.Schema){}
		patchable := true
		var patchErr error
		for _, env := range present {
			if env == baseline && object.Kind != hclload.KindTable {
				continue
			}
			apply, err := objectPatch(baseObject, objectSchema(byEnvRole[env][object.Role], object), object)
			if err != nil {
				patchable, patchErr = false, err
				break
			}
			patches[env] = apply
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
		for env, apply := range patches {
			if apply != nil {
				apply(layer(layers, envLayerPath(env, object.Role)))
			}
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
	files["manifest.hcl"] = renderDecomposeManifest(envs, roles, byEnvRole, layers, groups)
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
	if err := verifyGeneratedDecomposition(files, envs, roles, targets, groups); err != nil {
		return generatedDecomposition{}, err
	}
	return generatedDecomposition{Files: files, Inventory: inventory}, nil
}

// objectPatch returns a mutation which adds the exact authored patch for one
// target environment. A nil mutation means the isolated objects are equal.
// Unsupported transitions return an error so auto mode can fall back to an
// environment declaration and explicit shared mode can fail closed.
func objectPatch(from, to *hclload.Schema, object decomposeObject) (func(*hclload.Schema), error) {
	switch object.Kind {
	case hclload.KindTable:
		patch, err := tablePatch(from, to, object)
		if err != nil || patchTableEmpty(patch) {
			return nil, err
		}
		return func(schema *hclload.Schema) { addTablePatch(schema, object.Database, patch) }, nil
	case hclload.KindMaterializedView:
		patch, err := materializedViewPatch(from, to, object)
		if err != nil || patchMaterializedViewEmpty(patch) {
			return nil, err
		}
		return func(schema *hclload.Schema) {
			db := ensureDatabase(schema, object.Database)
			db.MaterializedViewPatches = append(db.MaterializedViewPatches, patch)
		}, nil
	case hclload.KindView:
		patch, err := viewPatch(from, to, object)
		if err != nil || (patch.Query == nil && patch.Comment == nil) {
			return nil, err
		}
		return func(schema *hclload.Schema) {
			db := ensureDatabase(schema, object.Database)
			db.ViewPatches = append(db.ViewPatches, patch)
		}, nil
	case hclload.KindDictionary:
		patch, err := dictionaryPatch(from, to, object)
		if err != nil || patchDictionaryEmpty(patch) {
			return nil, err
		}
		return func(schema *hclload.Schema) {
			db := ensureDatabase(schema, object.Database)
			db.DictionaryPatches = append(db.DictionaryPatches, patch)
		}, nil
	case decomposeNamedCollectionKind:
		collection := to.NamedCollections[0]
		collection.Override = true
		return func(schema *hclload.Schema) { schema.NamedCollections = append(schema.NamedCollections, collection) }, nil
	default:
		return nil, fmt.Errorf("%s cannot use a shared base: %s changes are recreate-only; use mode environment", object.key(), object.Kind)
	}
}

func tablePatch(from, to *hclload.Schema, object decomposeObject) (hclload.PatchTableSpec, error) {
	diff := hclload.Diff(from, to)
	if diff.IsEmpty() {
		return hclload.PatchTableSpec{}, nil
	}
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

func materializedViewPatch(from, to *hclload.Schema, object decomposeObject) (hclload.PatchMaterializedViewSpec, error) {
	f, t := onlyMaterializedView(from), onlyMaterializedView(to)
	var unsupported []string
	if f.ToTable != t.ToTable {
		unsupported = append(unsupported, "to_table")
	}
	if !reflect.DeepEqual(f.Cluster, t.Cluster) {
		unsupported = append(unsupported, "cluster")
	}
	if !reflect.DeepEqual(f.Comment, t.Comment) {
		unsupported = append(unsupported, "comment")
	}
	adds, modifies, drops, err := columnPatch(f.Columns, t.Columns)
	if err != nil {
		unsupported = append(unsupported, err.Error())
	}
	if len(unsupported) > 0 {
		return hclload.PatchMaterializedViewSpec{}, fmt.Errorf("%s cannot use patch_materialized_view without loss: %s", object.key(), strings.Join(unsupported, ", "))
	}
	patch := hclload.PatchMaterializedViewSpec{Name: object.Name, Columns: adds, ModifyColumns: modifies, DropColumns: drops}
	if f.Query != t.Query {
		query := t.Query
		patch.Query = &query
	}
	return patch, nil
}

func columnPatch(from, to []hclload.ColumnSpec) (adds, modifies []hclload.ColumnSpec, drops []string, err error) {
	fromByName := map[string]hclload.ColumnSpec{}
	toByName := map[string]hclload.ColumnSpec{}
	for _, column := range from {
		fromByName[column.Name] = column
	}
	for _, column := range to {
		toByName[column.Name] = column
	}
	commonOld := []string{}
	for _, column := range from {
		if _, ok := toByName[column.Name]; ok {
			commonOld = append(commonOld, column.Name)
		} else {
			drops = append(drops, column.Name)
		}
	}
	commonNew := []string{}
	for _, column := range to {
		if old, ok := fromByName[column.Name]; ok {
			commonNew = append(commonNew, column.Name)
			if !reflect.DeepEqual(old, column) {
				modifies = append(modifies, column)
			}
		}
	}
	if !slicesEqual(commonOld, commonNew) {
		return nil, nil, nil, fmt.Errorf("existing-column reorder %v -> %v", commonOld, commonNew)
	}

	current := append([]string(nil), commonOld...)
	newNames := map[string]bool{}
	for _, column := range to {
		if _, existed := fromByName[column.Name]; !existed {
			newNames[column.Name] = true
		}
	}
	for targetPos, target := range to {
		if !newNames[target.Name] {
			continue
		}
		column := target
		needsPosition := false
		for _, later := range to[targetPos+1:] {
			if containsString(current, later.Name) || newNames[later.Name] {
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
				column.First = true
			} else {
				column.After = &predecessor
			}
		}
		adds = append(adds, column)
		current = insertName(current, column.Name, column.After, column.First)
	}
	return adds, modifies, drops, nil
}

func viewPatch(from, to *hclload.Schema, object decomposeObject) (hclload.PatchViewSpec, error) {
	diff := hclload.Diff(from, to)
	if len(diff.Databases) != 1 || len(diff.Databases[0].AlterViews) != 1 {
		return hclload.PatchViewSpec{}, fmt.Errorf("%s: expected one view delta", object.key())
	}
	change := diff.Databases[0].AlterViews[0]
	if change.Recreate {
		return hclload.PatchViewSpec{}, fmt.Errorf("%s cannot use patch_view without recreation: %s", object.key(), strings.Join(change.RecreateChanged, ", "))
	}
	patch := hclload.PatchViewSpec{Name: object.Name}
	if change.QueryChange != nil {
		patch.Query = change.QueryChange.New
	}
	if change.Comment != nil {
		if change.Comment.New == nil {
			return hclload.PatchViewSpec{}, fmt.Errorf("%s cannot use patch_view to clear comment", object.key())
		}
		patch.Comment = change.Comment.New
	}
	return patch, nil
}

func dictionaryPatch(from, to *hclload.Schema, object decomposeObject) (hclload.PatchDictionarySpec, error) {
	diff := hclload.Diff(from, to)
	if len(diff.Databases) != 1 || len(diff.Databases[0].AlterDictionaries) != 1 {
		return hclload.PatchDictionarySpec{}, fmt.Errorf("%s: expected one dictionary delta", object.key())
	}
	change := diff.Databases[0].AlterDictionaries[0]
	if len(change.SkippedRedactedSecrets) > 0 {
		return hclload.PatchDictionarySpec{}, fmt.Errorf("%s has unverifiable redacted fields: %s", object.key(), strings.Join(change.SkippedRedactedSecrets, ", "))
	}
	supported := map[string]bool{"source": true, "layout": true, "lifetime": true, "settings": true}
	var unsupported []string
	for _, field := range change.Changed {
		if !supported[field] {
			unsupported = append(unsupported, field)
		}
	}
	oldDictionary, newDictionary := change.Old, change.New
	patch := hclload.PatchDictionarySpec{Name: object.Name}
	if containsString(change.Changed, "source") {
		if newDictionary.Source == nil {
			unsupported = append(unsupported, "clearing source")
		} else {
			patch.Source = newDictionary.Source
		}
	}
	if containsString(change.Changed, "layout") {
		if newDictionary.Layout == nil {
			unsupported = append(unsupported, "clearing layout")
		} else {
			patch.Layout = newDictionary.Layout
		}
	}
	if containsString(change.Changed, "lifetime") {
		if newDictionary.Lifetime == nil {
			unsupported = append(unsupported, "clearing lifetime")
		} else {
			patch.Lifetime = newDictionary.Lifetime
		}
	}
	if containsString(change.Changed, "settings") {
		patch.Settings = map[string]string{}
		for key, value := range newDictionary.Settings {
			patch.Settings[key] = value
		}
		for key := range oldDictionary.Settings {
			if _, exists := newDictionary.Settings[key]; !exists {
				unsupported = append(unsupported, "removing setting "+key)
			}
		}
	}
	if len(unsupported) > 0 {
		return hclload.PatchDictionarySpec{}, fmt.Errorf("%s cannot use patch_dictionary without loss: %s", object.key(), strings.Join(unsupported, ", "))
	}
	return patch, nil
}

func onlyMaterializedView(schema *hclload.Schema) *hclload.MaterializedViewSpec {
	return &schema.Databases[0].MaterializedViews[0]
}

func patchMaterializedViewEmpty(p hclload.PatchMaterializedViewSpec) bool {
	return len(p.Columns)+len(p.ModifyColumns)+len(p.DropColumns) == 0 && p.Query == nil
}

func patchDictionaryEmpty(p hclload.PatchDictionarySpec) bool {
	return p.Source == nil && p.Layout == nil && p.Lifetime == nil && len(p.Settings) == 0
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

func insertName(current []string, name string, after *string, first bool) []string {
	position := len(current)
	if first {
		position = 0
	} else if after != nil {
		for i, existing := range current {
			if existing == *after {
				position = i + 1
				break
			}
		}
	}
	current = append(current, "")
	copy(current[position+1:], current[position:])
	current[position] = name
	return current
}

func patchTableEmpty(p hclload.PatchTableSpec) bool {
	return len(p.Columns)+len(p.ModifyColumns)+len(p.DropColumns)+len(p.Indexes)+len(p.DropIndexes)+len(p.Projections) == 0 &&
		p.OrderBy == nil && p.PartitionBy == nil && p.SampleBy == nil && p.TTL == nil && len(p.Settings) == 0 && p.Engine == nil
}

func verifyGeneratedDecomposition(files map[string][]byte, envs, roles []string, targets map[string]map[string]*hclload.Schema, groups []decomposeGroup) error {
	root, err := os.MkdirTemp("", "hclexp-decompose-verify-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(root) }()
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
			paths := decomposeLayerPaths(env, role, groups)
			var existing []string
			for _, path := range paths {
				full := filepath.Join(root, filepath.FromSlash(path))
				if _, err := os.Stat(full); err == nil {
					existing = append(existing, full)
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
		equal, err := canonicalSchemasEqual(first, objectSchema(schemas[env][object.Role], object))
		if err != nil || !equal {
			return false
		}
	}
	return true
}

func tableObjectUniformIgnoringSettings(object decomposeObject, envs []string, schemas map[string]map[string]*hclload.Schema) bool {
	if len(envs) < 2 {
		return true
	}
	withoutSettings := func(env string) *hclload.Schema {
		isolated := objectSchema(schemas[env][object.Role], object)
		onlyTable(isolated).Settings = nil
		return isolated
	}
	first := withoutSettings(envs[0])
	for _, env := range envs[1:] {
		equal, err := canonicalSchemasEqual(first, withoutSettings(env))
		if err != nil || !equal {
			return false
		}
	}
	return true
}

func setTableSettingsIntersection(base *hclload.Schema, object decomposeObject, envs []string, schemas map[string]map[string]*hclload.Schema) {
	intersection := map[string]string{}
	for key, value := range onlyTable(objectSchema(schemas[envs[0]][object.Role], object)).Settings {
		intersection[key] = value
	}
	for _, env := range envs[1:] {
		settings := onlyTable(objectSchema(schemas[env][object.Role], object)).Settings
		for key, value := range intersection {
			if candidate, ok := settings[key]; !ok || candidate != value {
				delete(intersection, key)
			}
		}
	}
	if len(intersection) == 0 {
		intersection = nil
	}
	onlyTable(base).Settings = intersection
}

func canonicalSchemasEqual(a, b *hclload.Schema) (bool, error) {
	var left, right bytes.Buffer
	if err := hclload.Write(&left, a); err != nil {
		return false, err
	}
	if err := hclload.Write(&right, b); err != nil {
		return false, err
	}
	return bytes.Equal(left.Bytes(), right.Bytes()), nil
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
func groupLayerPath(group, role string) string {
	return filepath.ToSlash(filepath.Join("layers", "group", group, role, "tables.hcl"))
}
func envLayerPath(env, role string) string {
	return filepath.ToSlash(filepath.Join("layers", "env", env, role, "patches.hcl"))
}

func decomposeLayerPaths(env, role string, groups []decomposeGroup) []string {
	paths := []string{sharedLayerPath(role)}
	for _, group := range groups {
		if containsString(group.Envs, env) {
			paths = append(paths, groupLayerPath(group.Name, role))
		}
	}
	return append(paths, envLayerPath(env, role))
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

func validateDecomposeObjectAssignment(key string, object decomposeObjectAssignment) error {
	switch object.Mode {
	case "", "auto", "shared", "group", "environment", "exclude":
	default:
		return fmt.Errorf("object %q has invalid mode %q", key, object.Mode)
	}
	if object.Mode != "group" {
		if len(object.Envs) > 0 || object.Name != "" {
			return fmt.Errorf("object %q sets group envs/name with mode %q", key, object.Mode)
		}
		return nil
	}
	if len(object.Envs) < 2 {
		return fmt.Errorf("object %q group mode requires at least two environments", key)
	}
	seen := map[string]bool{}
	for _, env := range object.Envs {
		if env == "" {
			return fmt.Errorf("object %q group contains an empty environment", key)
		}
		if seen[env] {
			return fmt.Errorf("object %q group contains environment %q more than once", key, env)
		}
		seen[env] = true
	}
	if object.Name != "" && !validDecomposeGroupName(object.Name) {
		return fmt.Errorf("object %q group name %q is invalid (use letters, digits, dot, dash, or underscore)", key, object.Name)
	}
	return nil
}

func resolveDecomposeGroups(objects map[string]decomposeObjectAssignment, envs []string) (map[string]decomposeGroup, []decomposeGroup, error) {
	available := map[string]bool{}
	for _, env := range envs {
		available[env] = true
	}
	byObject := map[string]decomposeGroup{}
	byName := map[string]decomposeGroup{}
	for _, key := range sortedKeysLocal(objects) {
		object := objects[key]
		if err := validateDecomposeObjectAssignment(key, object); err != nil {
			return nil, nil, err
		}
		if object.Mode != "group" {
			continue
		}
		members := append([]string(nil), object.Envs...)
		sort.Strings(members)
		for _, env := range members {
			if !available[env] {
				return nil, nil, fmt.Errorf("object %q group references unknown environment %q (available: %s)", key, env, strings.Join(envs, ", "))
			}
		}
		name := object.Name
		if name == "" {
			name = deriveDecomposeGroupName(members)
		}
		if !validDecomposeGroupName(name) {
			return nil, nil, fmt.Errorf("object %q cannot derive a safe group name from environments [%s]; set name explicitly", key, strings.Join(members, ", "))
		}
		group := decomposeGroup{Name: name, Envs: members}
		if existing, ok := byName[name]; ok && !sameStringSet(existing.Envs, members) {
			return nil, nil, fmt.Errorf("group name %q maps to conflicting environment sets [%s] and [%s]; set distinct names",
				name, strings.Join(existing.Envs, ", "), strings.Join(members, ", "))
		}
		byName[name] = group
		byObject[key] = group
	}
	names := sortedKeysLocal(byName)
	groups := make([]decomposeGroup, 0, len(names))
	for _, name := range names {
		groups = append(groups, byName[name])
	}
	return byObject, groups, nil
}

func deriveDecomposeGroupName(envs []string) string {
	common := strings.Split(envs[0], "-")
	for _, env := range envs[1:] {
		parts := strings.Split(env, "-")
		limit := len(common)
		if len(parts) < limit {
			limit = len(parts)
		}
		i := 0
		for i < limit && common[i] == parts[i] {
			i++
		}
		common = common[:i]
	}
	if len(common) > 0 {
		return strings.Join(common, "-")
	}
	return strings.Join(envs, "--")
}

func validDecomposeGroupName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '.' || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left, right := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(left)
	sort.Strings(right)
	return slicesEqual(left, right)
}

func renderDecomposeManifest(envs, roles []string, schemas map[string]map[string]*hclload.Schema, layers map[string]*hclload.Schema, groups []decomposeGroup) []byte {
	var body strings.Builder
	for _, role := range roles {
		fmt.Fprintf(&body, "role %q {\n", role)
		for _, env := range envs {
			if schemas[env][role] == nil {
				continue
			}
			paths := []string{}
			for _, path := range decomposeLayerPaths(env, role, groups) {
				if layers[path] != nil {
					paths = append(paths, filepath.ToSlash(filepath.Dir(path)))
				}
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
