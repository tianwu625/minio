/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package opfs

import (
	"context"

	"github.com/minio/cli"
	"github.com/minio/madmin-go"
    minio "github.com/minio/minio/cmd"
)

func init() {
	const opfsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  path to Minio directory

EXAMPLES:
  1. Start minio gateway server for OPFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} /opfs/vols/dir
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.OPFSBackendGateway,
		Usage:              "Network-attached storage (OPFS)",
		Action:             opfsGatewayMain,
		CustomHelpTemplate: opfsGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway nas' command line.
func opfsGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.OPFSBackendGateway, 1)
	}

	minio.StartGateway(ctx, &OPFS{ctx.Args().First()})
}

// NAS implements Gateway.
type OPFS struct {
	path string
}

// Name implements Gateway interface.
func (o *OPFS) Name() string {
	return minio.OPFSBackendGateway
}

// NewGatewayLayer returns nas gatewaylayer.
func (o *OPFS) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	var err error
	newObject, err := minio.NewOPFSObjectLayer(o.path)
	if err != nil {
		return nil, err
	}
	return &opfsObjects{newObject}, nil
}

// IsListenSupported returns whether listen bucket notification is applicable for this gateway.
func (o *opfsObjects) IsListenSupported() bool {
	return false
}

func (o *opfsObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si, errs := o.ObjectLayer.StorageInfo(ctx)
	si.Backend.GatewayOnline = si.Backend.Type == madmin.FS
	si.Backend.Type = madmin.Gateway
	return si, errs
}

// opfsObjects implements gateway for MinIO and S3 compatible object storage servers.
type opfsObjects struct {
	minio.ObjectLayer
}

func (o *opfsObjects) IsTaggingSupported() bool {
	return true
}
