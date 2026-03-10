# Changelog

All notable changes to the Azure Kusto Spark Connector will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Version numbers follow the `{major}.{minor}.{patch}` scheme (e.g., `7.0.5`).
Tags are created as `v4.0_{version}` (Spark 4 / master) and `v3.0_{version}` (Spark 3 / release/spark3).

## [Unreleased]

### Changed
- None

### Added
- None

### Fixed
- None

## [7.0.5]

### Changed
- None

### Added
- Add documentation for troubleshooting (#461)

### Fixed
- Fix issue with CloudInfo cache (#466)
- Fix Hadoop and Spark config setup (#462, #463)
- Remove minimizeJar directive (#470)
- Fix Github release pipeline (#472)

## [7.0.3]

### Changed
- None

### Added
- None

### Fixed
- Fix version of Kusto SDK (#459)

## [7.0.2]

### Changed
- None

### Added
- New read option for storage protocol in distributed reads (#450)

### Fixed
- Whitelist storage to avoid Fabric error (#455)
- Fix for release pipeline for Spark 4 (#454)
- Correct the resilience and vavr lib versions (#452)
- Reformatted code to have SAS key checks (#458)

## [7.0.1]

### Changed
- None

### Added
- Syntax support for V11 engine for extents drop (#445)

### Fixed
- Remove V3 engine check (#439)
- Fix filter keys (#446)
