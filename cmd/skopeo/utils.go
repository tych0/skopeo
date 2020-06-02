package main

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/containers/image/v5/pkg/compression"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// errorShouldDisplayUsage is a subtype of error used by command handlers to indicate that cli.ShowSubcommandHelp should be called.
type errorShouldDisplayUsage struct {
	error
}

// commandAction intermediates between the RunE interface and the real handler,
// primarily to ensure that cobra.Command is not available to the handler, which in turn
// makes sure that the cmd.Flags() etc. flag access functions are not used,
// and everything is done using the *Options structures and the *Var() methods of cmd.Flag().
// handler may return errorShouldDisplayUsage to cause c.Help to be called.
func commandAction(handler func(args []string, stdout io.Writer) error) func(cmd *cobra.Command, args []string) error {
	return func(c *cobra.Command, args []string) error {
		err := handler(args, c.OutOrStdout())
		if _, ok := err.(errorShouldDisplayUsage); ok {
			c.Help()
		}
		return err
	}
}

// sharedImageOptions collects CLI flags which are image-related, but do not change across images.
// This really should be a part of globalOptions, but that would break existing users of (skopeo copy --authfile=).
type sharedImageOptions struct {
	authFilePath string // Path to a */containers/auth.json
}

// sharedImageFlags prepares a collection of CLI flags writing into sharedImageOptions, and the managed sharedImageOptions structure.
func sharedImageFlags() (pflag.FlagSet, *sharedImageOptions) {
	opts := sharedImageOptions{}
	fs := pflag.FlagSet{}
	fs.StringVar(&opts.authFilePath, "authfile", os.Getenv("REGISTRY_AUTH_FILE"), "path of the authentication file. Default is ${XDG_RUNTIME_DIR}/containers/auth.json")
	return fs, &opts
}

// dockerImageOptions collects CLI flags specific to the "docker" transport, which are
// the same across subcommands, but may be different for each image
// (e.g. may differ between the source and destination of a copy)
type dockerImageOptions struct {
	global         *globalOptions      // May be shared across several imageOptions instances.
	shared         *sharedImageOptions // May be shared across several imageOptions instances.
	authFilePath   optionalString      // Path to a */containers/auth.json (prefixed version to override shared image option).
	credsOption    optionalString      // username[:password] for accessing a registry
	dockerCertPath string              // A directory using Docker-like *.{crt,cert,key} files for connecting to a registry or a daemon
	tlsVerify      optionalBool        // Require HTTPS and verify certificates (for docker: and docker-daemon:)
	noCreds        bool                // Access the registry anonymously
}

// imageOptions collects CLI flags which are the same across subcommands, but may be different for each image
// (e.g. may differ between the source and destination of a copy)
type imageOptions struct {
	dockerImageOptions
	sharedBlobDir    string // A directory to use for OCI blobs, shared across repositories
	dockerDaemonHost string // docker-daemon: host to connect to
}

// dockerImageFlags prepares a collection of docker-transport specific CLI flags
// writing into imageOptions, and the managed imageOptions structure.
func dockerImageFlags(global *globalOptions, shared *sharedImageOptions, flagPrefix, credsOptionAlias string) (pflag.FlagSet, *imageOptions) {
	flags := imageOptions{
		dockerImageOptions: dockerImageOptions{
			global: global,
			shared: shared,
		},
	}

	fs := pflag.FlagSet{}
	if flagPrefix != "" {
		// the non-prefixed flag is handled by a shared flag.
		fs.Var(newOptionalStringValue(&flags.authFilePath), flagPrefix+"authfile", "path of the authentication file. Default is ${XDG_RUNTIME_DIR}/containers/auth.json")
	}
	fs.Var(newOptionalStringValue(&flags.credsOption), flagPrefix+"creds", "Use `USERNAME[:PASSWORD]` for accessing the registry")
	if credsOptionAlias != "" {
		// This is horribly ugly, but we need to support the old option forms of (skopeo copy) for compatibility.
		// Don't add any more cases like this.
		f := fs.VarPF(newOptionalStringValue(&flags.credsOption), credsOptionAlias, "", "Use `USERNAME[:PASSWORD]` for accessing the registry")
		f.Hidden = true
	}
	fs.StringVar(&flags.dockerCertPath, flagPrefix+"cert-dir", "", "use certificates at `PATH` (*.crt, *.cert, *.key) to connect to the registry or daemon")
	optionalBoolFlag(&fs, &flags.tlsVerify, flagPrefix+"tls-verify", "require HTTPS and verify certificates when talking to the container registry or daemon (defaults to true)")
	fs.BoolVar(&flags.noCreds, flagPrefix+"no-creds", false, "Access the registry anonymously")
	return fs, &flags
}

// imageFlags prepares a collection of CLI flags writing into imageOptions, and the managed imageOptions structure.
func imageFlags(global *globalOptions, shared *sharedImageOptions, flagPrefix, credsOptionAlias string) (pflag.FlagSet, *imageOptions) {
	dockerFlags, opts := dockerImageFlags(global, shared, flagPrefix, credsOptionAlias)

	fs := pflag.FlagSet{}
	fs.StringVar(&opts.sharedBlobDir, flagPrefix+"shared-blob-dir", "", "`DIRECTORY` to use to share blobs across OCI repositories")
	fs.StringVar(&opts.dockerDaemonHost, flagPrefix+"daemon-host", "", "use docker daemon host at `HOST` (docker-daemon: only)")
	fs.AddFlagSet(&dockerFlags)
	return fs, opts
}

// newSystemContext returns a *types.SystemContext corresponding to opts.
// It is guaranteed to return a fresh instance, so it is safe to make additional updates to it.
func (opts *imageOptions) newSystemContext() (*types.SystemContext, error) {
	// *types.SystemContext instance from globalOptions
	//  imageOptions option overrides the instance if both are present.
	ctx := opts.global.newSystemContext()
	ctx.DockerCertPath = opts.dockerCertPath
	ctx.OCISharedBlobDirPath = opts.sharedBlobDir
	ctx.AuthFilePath = opts.shared.authFilePath
	ctx.DockerDaemonHost = opts.dockerDaemonHost
	ctx.DockerDaemonCertPath = opts.dockerCertPath
	if opts.dockerImageOptions.authFilePath.present {
		ctx.AuthFilePath = opts.dockerImageOptions.authFilePath.value
	}
	if opts.tlsVerify.present {
		ctx.DockerDaemonInsecureSkipTLSVerify = !opts.tlsVerify.value
	}
	if opts.tlsVerify.present {
		ctx.DockerInsecureSkipTLSVerify = types.NewOptionalBool(!opts.tlsVerify.value)
	}
	if opts.credsOption.present && opts.noCreds {
		return nil, errors.New("creds and no-creds cannot be specified at the same time")
	}
	if opts.credsOption.present {
		var err error
		ctx.DockerAuthConfig, err = getDockerAuth(opts.credsOption.value)
		if err != nil {
			return nil, err
		}
	}
	if opts.noCreds {
		ctx.DockerAuthConfig = &types.DockerAuthConfig{}
	}

	return ctx, nil
}

// imageDestOptions is a superset of imageOptions specialized for iamge destinations.
type imageDestOptions struct {
	*imageOptions
	dirForceCompression            bool        // Compress layers when saving to the dir: transport
	ociAcceptUncompressedLayers    bool        // Whether to accept uncompressed layers in the oci: transport
	dockerAcceptUncompressedLayers bool   // Whether to accept uncompressed layers in the docker: transport
	compressionFormat              string      // Format to use for the compression
	compressionLevel               optionalInt // Level to use for the compression
}

// imageDestFlags prepares a collection of CLI flags writing into imageDestOptions, and the managed imageDestOptions structure.
func imageDestFlags(global *globalOptions, shared *sharedImageOptions, flagPrefix, credsOptionAlias string) (pflag.FlagSet, *imageDestOptions) {
	genericFlags, genericOptions := imageFlags(global, shared, flagPrefix, credsOptionAlias)
	opts := imageDestOptions{imageOptions: genericOptions}
	fs := pflag.FlagSet{}
	fs.AddFlagSet(&genericFlags)
	fs.BoolVar(&opts.dirForceCompression, flagPrefix+"compress", false, "Compress tarball image layers when saving to directory using the 'dir' transport. (default is same compression type as source)")
	fs.BoolVar(&opts.ociAcceptUncompressedLayers, flagPrefix+"oci-accept-uncompressed-layers", false, "Allow uncompressed image layers when saving to an OCI image using the 'oci' transport. (default is to compress things that aren't compressed)")
	fs.BoolVar(&opts.dockerAcceptUncompressedLayers, flagPrefix+"docker-accept-uncompressed-layers", false, "Allow uncompressed image layers when saving to a Docker repository using the 'docker' transport. (default is to compress things that aren't compressed)")
	fs.StringVar(&opts.compressionFormat, flagPrefix+"compress-format", "", "`FORMAT` to use for the compression")
	fs.Var(newOptionalIntValue(&opts.compressionLevel), flagPrefix+"compress-level", "`LEVEL` to use for the compression")
	return fs, &opts
}

// newSystemContext returns a *types.SystemContext corresponding to opts.
// It is guaranteed to return a fresh instance, so it is safe to make additional updates to it.
func (opts *imageDestOptions) newSystemContext() (*types.SystemContext, error) {
	ctx, err := opts.imageOptions.newSystemContext()
	if err != nil {
		return nil, err
	}

	ctx.DirForceCompress = opts.dirForceCompression
	ctx.OCIAcceptUncompressedLayers = opts.ociAcceptUncompressedLayers
	ctx.DockerAcceptUncompressedLayers = opts.dockerAcceptUncompressedLayers
	if opts.compressionFormat != "" {
		cf, err := compression.AlgorithmByName(opts.compressionFormat)
		if err != nil {
			return nil, err
		}
		ctx.CompressionFormat = &cf
	}
	if opts.compressionLevel.present {
		ctx.CompressionLevel = &opts.compressionLevel.value
	}
	return ctx, err
}

func parseCreds(creds string) (string, string, error) {
	if creds == "" {
		return "", "", errors.New("credentials can't be empty")
	}
	up := strings.SplitN(creds, ":", 2)
	if len(up) == 1 {
		return up[0], "", nil
	}
	if up[0] == "" {
		return "", "", errors.New("username can't be empty")
	}
	return up[0], up[1], nil
}

func getDockerAuth(creds string) (*types.DockerAuthConfig, error) {
	username, password, err := parseCreds(creds)
	if err != nil {
		return nil, err
	}
	return &types.DockerAuthConfig{
		Username: username,
		Password: password,
	}, nil
}

// parseImageSource converts image URL-like string to an ImageSource.
// The caller must call .Close() on the returned ImageSource.
func parseImageSource(ctx context.Context, opts *imageOptions, name string) (types.ImageSource, error) {
	ref, err := alltransports.ParseImageName(name)
	if err != nil {
		return nil, err
	}
	sys, err := opts.newSystemContext()
	if err != nil {
		return nil, err
	}
	return ref.NewImageSource(ctx, sys)
}

// usageTemplate returns the usage template for skopeo commands
// This blocks the displaying of the global options. The main skopeo
// command should not use this.
const usageTemplate = `Usage:{{if .Runnable}}
{{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}

{{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
{{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
{{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}
{{end}}
`

// adjustUsage uses usageTemplate template to get rid the GlobalOption from usage
// and disable [flag] at the end of command usage
func adjustUsage(c *cobra.Command) {
	c.SetUsageTemplate(usageTemplate)
	c.DisableFlagsInUseLine = true
}
