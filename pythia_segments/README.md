# Pythia Segments

Pythia segments is a simple Go API exposing list of users with their conversion predictions as segments, that can be used in REMP Mailer, Campaign, or other related tools.

## Build

### Pre-built binaries

You can find pre-built binaries on our [Github releases](https://github.com/remp2020/pythia-tools/releases) page.

### Docker

If you have Docker installed, you can build the API executable by using our docker image for building. You can run it with:

```
make docker-build
```

Build process creates `pythia_segments.tar` containing executable binary, `.env.example` file and `swagger` folder with OpenAPI specs - full package that's required to run the API on any machine.

### Go

If you have Go installed (1.13+), you can build the API directly by calling:

```
make build
```

Build process creates a `pythia_segments` binary that can be executed and a `swagger` folder containing OpenAPI specs that are necessary to API for work correctly.


## Usage

Create `.env` file based on `.env.example` file and fill the configuration options. The file should be within the working directory from which the executable is being run. When you have `.env` file prepared, start the API:

```
./pythia_segments
```