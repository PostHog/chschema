package loader

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// NewDesiredState creates a new NodeSchemaState (alias for backward compatibility)
func NewDesiredState() *chschema_v1.NodeSchemaState {
	return &chschema_v1.NodeSchemaState{
		Clusters:          []*chschema_v1.Cluster{},
		Tables:            []*chschema_v1.Table{},
		Views:             []*chschema_v1.View{},
		MaterializedViews: []*chschema_v1.MaterializedView{},
	}
}

// SchemaLoader is responsible for loading schema files from a directory
type SchemaLoader struct {
	path string
}

func NewSchemaLoader(path string) *SchemaLoader {
	return &SchemaLoader{path: path}
}

// Load reads all YAML files from the schema directory and returns a NodeSchemaState
func (l *SchemaLoader) Load() (*chschema_v1.NodeSchemaState, error) {
	state := NewDesiredState()

	loaders := map[string]func(data []byte, path string) error{
		"clusters": func(data []byte, path string) error {
			var cluster chschema_v1.Cluster
			if err := unmarshalYAMLToProto(data, &cluster); err != nil {
				return err
			}
			state.Clusters = append(state.Clusters, &cluster)
			return nil
		},
		"tables": func(data []byte, path string) error {
			var table chschema_v1.Table
			if err := unmarshalYAMLToProto(data, &table); err != nil {
				return err
			}
			state.Tables = append(state.Tables, &table)
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
			log.Debug().Str("file", filePath).Msg("Successfully loaded and parsed YAML file")
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
