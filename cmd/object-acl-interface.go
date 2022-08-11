package cmd

import (
	"context"
	"time"

	"github.com/minio/minio/internal/auth"
)

type ObjectAcler interface {
    SetAcl(ctx context.Context, bucket, object string, grants []grant) error
    GetAcl(ctx context.Context, bucket, object string) ([]grant, error)
}

type GetUserCanionialIDer interface {
    GetUserCanionialID (cred *auth.Credentials) (string, error)
}

type GetExpiryDurationer interface {
    GetExpiryDuration (dsecs string) (time.Duration, error)
}
