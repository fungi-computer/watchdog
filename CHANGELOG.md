# @fungi.computer/watchdog

## 0.1.5

### Patch Changes

- Changes since 0.1.4:

  - fix(watchdog): let concurrent Postgres claimers skip a job another is taking

## 0.1.4

### Patch Changes

- Changes since 0.1.3:

  - fix(watchdog): read the transaction flag through a call so lint sees it
    change
  - fix(watchdog): surface the original error when SQLite already rolled back
  - fix(watchdog): roll back a commit SQLite refuses as busy

## 0.1.3

### Patch Changes

- Changes since 0.1.2:

  - fix(docs): build each package before its own docs check

## 0.1.2

### Patch Changes

- Changes since 0.1.1:

  - fix(docs): build mycelium and watchdog before their docs check and drop
    in-page README anchors

## 0.1.1

### Patch Changes

- Changes since 0.1.0:

  - fix(durability): give Watchdog delivery and timer readiness
  - fix(durability): preserve retry decisions and drain ownership across restart
  - fix(watchdog): retain execution decisions across settlement failures

## 0.1.0

### Minor Changes

- Changes since 0.0.1:

  - feat(stranger): typecheck each public entry in its own environment

### Patch Changes

- Updated dependencies
  - @fungi.computer/cairn@0.1.0

## 0.0.1

### Patch Changes

- Inaugural public release of @fungi.computer/watchdog.
- Updated dependencies
  - @fungi.computer/cairn@0.0.1
