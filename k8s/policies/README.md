# Image security controls

This directory contains Kyverno policies for container image security.

## Policy order

1. `restrict-container-images.yaml`
   - blocks `:latest` and untagged images
   - requires images from the approved registry
   - starts in `audit` mode

2. `require-safe-pod-security-context.yaml`
   - blocks `privileged: true`
   - blocks `runAsNonRoot: false`
   - starts in `audit` mode

3. `require-cosign-signature-prod.yaml`
   - requires production images to be signed with the configured cosign public key
   - uses `COSIGN_PUB` repository variable

## Enforcement

Policies are committed in `enforce` mode.
