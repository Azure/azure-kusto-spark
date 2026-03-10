# Changelog

All notable changes to the Azure Kusto Spark Connector will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Version numbers follow the `{major}.{minor}.{patch}` scheme (e.g., `7.0.5`).
Tags are created as `v4.0_{version}` (Spark 4 / master) and `v3.0_{version}` (Spark 3 / release/spark3).

## [Unreleased]

### Breaking Changes
- None

### Features
- None

### Fixes
- None

## [7.0.5]

### Breaking Changes
- None

### Features
- Add documentation for troubleshooting (#461)

### Fixes
- Fix issue with CloudInfo cache (#466)
- Fix Hadoop and Spark config setup (#462, #463)
- Remove minimizeJar directive (#470)
- Fix Github release pipeline (#472)

## [7.0.3]

### Breaking Changes
- None

### Features
- None

### Fixes
- Fix version of Kusto SDK (#459)

## [7.0.2]

### Breaking Changes
- None

### Features
- New read option for storage protocol in distributed reads (#450)

### Fixes
- Whitelist storage to avoid Fabric error (#455)
- Fix for release pipeline for Spark 4 (#454)
- Correct the resilience and vavr lib versions (#452)
- Reformatted code to have SAS key checks (#458)

## [7.0.1]

### Breaking Changes
- None

### Features
- Syntax support for V11 engine for extents drop (#445)

### Fixes
- Remove V3 engine check (#439)
- Fix filter keys (#446)
