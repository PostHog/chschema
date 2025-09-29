package loader

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/posthog/chschema/gen/chschema_v1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DesiredState holds the complete desired schema configuration
type DesiredState struct {
	Clusters          map[string]*chschema_v1.Cluster
	Tables            map[string]*chschema_v1.Table
	Views             map[string]*chschema_v1.View
	MaterializedViews map[string]*chschema_v1.MaterializedView
}

func NewDesiredState() *DesiredState {
	return &DesiredState{
		Clusters:          make(map[string]*chschema_v1.Cluster),
		Tables:            make(map[string]*chschema_v1.Table),
		Views:             make(map[string]*chschema_v1.View),
		MaterializedViews: make(map[string]*chschema_v1.MaterializedView),
	}
}

// SchemaLoader is responsible for loading schema files from a directory
type SchemaLoader struct {
	path string
}

func NewSchemaLoader(path string) *SchemaLoader {
	return &SchemaLoader{path: path}
}

// Load reads all YAML files from the schema directory and returns a DesiredState
func (l *SchemaLoader) Load() (*DesiredState, error) {
	state := NewDesiredState()

	loaders := map[string]func(data []byte, path string) error{
		"clusters": func(data []byte, path string) error {
			var cluster chschema_v1.Cluster
			if err := unmarshalYAMLToProto(data, &cluster); err != nil {
				return err
			}
			state.Clusters[cluster.Name] = &cluster
			return nil
		},
		"tables": func(data []byte, path string) error {
			var table chschema_v1.Table
			if err := unmarshalYAMLToProto(data, &table); err != nil {
				return err
			}
			state.Tables[table.Name] = &table
			return nil
		},
	}

	for subdir, loaderFunc := range loaders {
		dirPath := filepath.Join(l.path, subdir)
		files, err := os.ReadDir(dirPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("failed to read subdirectory %s: %w", dirPath, err)
		}

		for _, file := range files {
			if file.IsDir() || (filepath.Ext(file.Name()) != ".yaml" && filepath.Ext(file.Name()) != ".yml") {
				continue
			}

			filePath := filepath.Join(dirPath, file.Name())
			data, err := ioutil.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
			}

			if err := loaderFunc(data, filePath); err != nil {
				return nil, fmt.Errorf("failed to process file %s: %w", filePath, err)
			}
			fmt.Printf("Successfully loaded and parsed %s\n", filePath)
		}
	}

	return state, nil
}

func unmarshalYAMLToProto(data []byte, m proto.Message) error {
	var body interface{}
	if err := yaml.Unmarshal(data, &body); err != nil {
		return fmt.Errorf("failed to unmarshal yaml: %w", err)
	}

	jsonBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal to json: %w", err)
	}

	return protojson.Unmarshal(jsonBytes, m)
}
