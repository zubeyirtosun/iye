package config

import (
	"os"
	"path/filepath"

	"github.com/iye/iye/pkg/models"
	"gopkg.in/yaml.v3"
)

func Load(path string) (*models.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return models.DefaultConfig(), nil
		}
		return nil, err
	}

	config := models.DefaultConfig()
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, err
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

func LoadOrDefault(paths ...string) (*models.Config, error) {
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			return Load(path)
		}
	}

	defaultPaths := []string{
		"/etc/iye/config.yaml",
		"/etc/iye/config.yml",
		"./config.yaml",
		"./config.yml",
		filepath.Join(os.Getenv("HOME"), ".iye", "config.yaml"),
	}

	for _, path := range defaultPaths {
		if _, err := os.Stat(path); err == nil {
			return Load(path)
		}
	}

	cfg := models.DefaultConfig()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func Save(config *models.Config, path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}