package policy

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const maxPolicyBytes = 4 << 20

func decodeStrict(source []byte) (Policy, map[string]sourcePosition, error) {
	var policy Policy
	if len(source) == 0 {
		return policy, nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: "document is empty"}}}
	}
	if len(source) > maxPolicyBytes {
		return policy, nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: fmt.Sprintf("document exceeds %d bytes", maxPolicyBytes)}}}
	}
	if !utf8.Valid(source) {
		return policy, nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: "document is not valid UTF-8"}}}
	}

	root, err := decodeNode(source)
	if err != nil {
		return policy, nil, err
	}
	positions := make(map[string]sourcePosition)
	collector := newValidationCollector(positions)
	validateNodeShape(root, reflect.TypeOf(policy), "", positions, collector)
	if err := collector.err(); err != nil {
		return policy, nil, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(source))
	decoder.KnownFields(true)
	if err := decoder.Decode(&policy); err != nil {
		return policy, nil, decodeFailure(err, positions)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("more than one YAML document is not allowed")
		}
		return policy, nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: err.Error()}}}
	}
	return policy, positions, nil
}

var yamlLinePattern = regexp.MustCompile(`line ([0-9]+):`)

func decodeFailure(err error, positions map[string]sourcePosition) error {
	path := "$"
	line := 0
	if match := yamlLinePattern.FindStringSubmatch(err.Error()); len(match) == 2 {
		line, _ = strconv.Atoi(match[1])
		paths := make([]string, 0)
		for candidate, position := range positions {
			if position.line == line {
				paths = append(paths, candidate)
			}
		}
		sort.Strings(paths)
		if len(paths) > 0 {
			path = paths[len(paths)-1]
		}
	}
	position := positions[path]
	if position.line == 0 {
		position.line = line
	}
	return &ValidationError{Problems: []FieldError{{
		Path: path, Message: "YAML decode failed: " + err.Error(),
		Line: position.line, Column: position.column,
	}}}
}

func decodeNode(source []byte) (*yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: "YAML decode failed: " + err.Error()}}}
	}
	if len(document.Content) != 1 {
		return nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: "document must contain exactly one root value"}}}
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("more than one YAML document is not allowed")
		}
		return nil, &ValidationError{Problems: []FieldError{{Path: "$", Message: err.Error()}}}
	}
	return document.Content[0], nil
}

func validateNodeShape(node *yaml.Node, typ reflect.Type, path string, positions map[string]sourcePosition, collector *validationCollector) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if node.Tag == "!!null" {
		collector.add(displayPath(path), "null values are not allowed")
		return
	}
	if node.Kind == yaml.AliasNode || node.Anchor != "" {
		collector.add(displayPath(path), "YAML anchors and aliases are not allowed")
		return
	}
	if implementsYAMLUnmarshaller(typ) {
		resolvedPath := displayPath(path)
		positions[resolvedPath] = sourcePosition{line: node.Line, column: node.Column}
		value := reflect.New(typ).Interface()
		if unmarshaler, ok := value.(yaml.Unmarshaler); ok {
			if err := unmarshaler.UnmarshalYAML(node); err != nil {
				collector.add(resolvedPath, "%v", err)
			}
		}
		return
	}

	switch typ.Kind() {
	case reflect.Struct:
		if node.Kind != yaml.MappingNode {
			collector.add(displayPath(path), "expected a mapping")
			return
		}
		fields := yamlFields(typ)
		for index := 0; index < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				collector.add(displayPath(path), "mapping keys must be strings")
				continue
			}
			childPath := joinPath(path, key.Value)
			positions[childPath] = sourcePosition{line: value.Line, column: value.Column}
			fieldType, ok := fields[key.Value]
			if !ok {
				collector.add(childPath, "unknown field")
				continue
			}
			if key.Value == "<<" {
				collector.add(childPath, "YAML merge keys are not allowed")
				continue
			}
			validateNodeShape(value, fieldType, childPath, positions, collector)
		}
	case reflect.Slice, reflect.Array:
		if node.Kind != yaml.SequenceNode {
			collector.add(displayPath(path), "expected a sequence")
			return
		}
		for index, child := range node.Content {
			childPath := fmt.Sprintf("%s[%d]", path, index)
			positions[childPath] = sourcePosition{line: child.Line, column: child.Column}
			validateNodeShape(child, typ.Elem(), childPath, positions, collector)
		}
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			collector.add(displayPath(path), "expected a mapping")
			return
		}
		if typ.Key().Kind() != reflect.String {
			collector.add(displayPath(path), "only string-keyed mappings are supported")
			return
		}
		for index := 0; index < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				collector.add(displayPath(path), "mapping keys must be strings")
				continue
			}
			childPath := joinPath(path, key.Value)
			positions[childPath] = sourcePosition{line: value.Line, column: value.Column}
			validateNodeShape(value, typ.Elem(), childPath, positions, collector)
		}
	default:
		positions[displayPath(path)] = sourcePosition{line: node.Line, column: node.Column}
	}
}

func implementsYAMLUnmarshaller(typ reflect.Type) bool {
	unmarshaler := reflect.TypeOf((*yaml.Unmarshaler)(nil)).Elem()
	return typ.Implements(unmarshaler) || reflect.PointerTo(typ).Implements(unmarshaler)
}

func yamlFields(typ reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for index := 0; index < typ.NumField(); index++ {
		field := typ.Field(index)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "" {
			name = strings.ToLower(field.Name[:1]) + field.Name[1:]
		}
		if name != "-" {
			fields[name] = field.Type
		}
	}
	return fields
}

func joinPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

func displayPath(path string) string {
	if path == "" {
		return "$"
	}
	return path
}
