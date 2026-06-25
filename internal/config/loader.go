package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func LoadFile(path string) (*PipelineConfig, error) {
	format, err := DetectFormat(path, "")
	if err != nil {
		return nil, err
	}
	switch format {
	case FormatYAML:
		return LoadYAML(path)
	case FormatHOCON:
		return LoadHOCON(path)
	default:
		return nil, fmt.Errorf("unsupported config format for %q", path)
	}
}

type Format string

const (
	FormatYAML  Format = "yaml"
	FormatHOCON Format = "hocon"
)

func DetectFormat(path, explicit string) (Format, error) {
	if explicit != "" {
		switch strings.ToLower(explicit) {
		case "yaml", "yml":
			return FormatYAML, nil
		case "hocon", "conf":
			return FormatHOCON, nil
		default:
			return "", fmt.Errorf("unknown format %q", explicit)
		}
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		return FormatYAML, nil
	case ".conf", ".hocon":
		return FormatHOCON, nil
	default:
		return "", fmt.Errorf("cannot detect format from extension %q; use --format", ext)
	}
}

func LoadYAML(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg PipelineConfig
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(false)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("yaml decode: %w", err)
	}
	return &cfg, nil
}

func LoadDir(dir string) ([]*PipelineConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var configs []*PipelineConfig
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" && ext != ".conf" && ext != ".hocon" {
			continue
		}
		cfg, err := LoadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		configs = append(configs, cfg)
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("no pipeline configs found in %s", dir)
	}
	return configs, nil
}
