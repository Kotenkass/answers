//go:build tools

// Package tools documents tooling dependencies that are intentionally not imported by application code.
//
// The security workflow runs govulncheck with an explicit module version, so this package has no
// runtime imports. It exists to keep the tools build tag harmless and self-documenting.
package tools
