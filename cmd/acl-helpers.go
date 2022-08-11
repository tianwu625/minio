package cmd

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

const (
	httpSchemWithSlash = "http://"
	AWSCom             = "acs.amazonaws.com"
	AWSGroup           = "groups"
	AWSS3              = "s3"
	AWSGlobal          = "global"
	AWSAllUser         = "AllUsers"
	AWSNS              = "http://www.w3.org/2001/XMLSchema-instance"
)

func parseURIGroup(uri string) (string, error) {
	allstr := strings.Split(strings.TrimPrefix(strings.TrimSpace(uri), httpSchemWithSlash), "/")
	if len(allstr) != 4 {
		return "", errInvalidArgument
	}

	if allstr[0] != AWSCom ||
		allstr[1] != AWSGroup ||
		(allstr[2] != AWSS3 && allstr[2] != AWSGlobal) {
		return "", errInvalidArgument
	}

	return allstr[3], nil
}

func isAllUser(uri string) bool {
	allstr := strings.Split(strings.TrimPrefix(strings.TrimSpace(uri), httpSchemWithSlash), "/")

	return allstr[2] == AWSGlobal && allstr[3] == AWSAllUser
}

func haveAclHeader(r *http.Request) bool {

	return r.Header.Get(xhttp.AmzGrantRead) != "" ||
		r.Header.Get(xhttp.AmzGrantWrite) != "" ||
		r.Header.Get(xhttp.AmzGrantReadAcp) != "" ||
		r.Header.Get(xhttp.AmzGrantWriteAcp) != "" ||
		r.Header.Get(xhttp.AmzGrantFullControl) != ""
}

func setACLWithCanned(ctx context.Context, aclAPI ObjectAcler, bucket, object, cannedACL string) error {

	if cannedACL == ACLCannedAwsExecRead ||
		cannedACL == ACLCannedBucketOwnerRead ||
		cannedACL == ACLCannedBucketOwnerFullControl ||
		cannedACL == ACLCannedLogDeliveryWrite {
		return NotImplemented{}
	}

	grants := make([]grant, 0, 3)
	if cannedACL == ACLCannedPrivate {
		g := grant{
			Grantee: grantee{
				XMLXSI: OwnerType,
			},
			Permission: GrantPermFullControl,
		}
		grants = append(grants, g)
	}

	if cannedACL == ACLCannedPublicRead {
		gowner := grant{
			Grantee: grantee{
				XMLXSI: OwnerType,
			},
			Permission: GrantPermFullControl,
		}
		gevery := grant{
			Grantee: grantee{
				XMLXSI: EveryType,
			},
			Permission: GrantPermRead,
		}
		grants = append(grants, gowner)
		grants = append(grants, gevery)
	}

	if cannedACL == ACLCannedPublicReadWrite {
		gowner := grant{
			Grantee: grantee{
				XMLXSI: OwnerType,
			},
			Permission: GrantPermFullControl,
		}
		geveryRead := grant{
			Grantee: grantee{
				XMLXSI: EveryType,
			},
			Permission: GrantPermRead,
		}
		geveryWrite := grant{
			Grantee: grantee{
				XMLXSI: EveryType,
			},
			Permission: GrantPermWrite,
		}
		grants = append(grants, gowner)
		grants = append(grants, geveryRead)
		grants = append(grants, geveryWrite)
	}
	if cannedACL == ACLCannedAuthRead {
		gowner := grant{
			Grantee: grantee{
				XMLXSI: OwnerType,
			},
			Permission: GrantPermFullControl,
		}
		gauthRead := grant{
			Grantee: grantee{
				XMLXSI: AuthType,
			},
			Permission: GrantPermRead,
		}

		grants = append(grants, gowner)
		grants = append(grants, gauthRead)
	}
	if err := aclAPI.SetAcl(ctx, bucket, object, grants); err != nil {
		return err
	}
	return nil
}

//http head acl key type
const (
	ACLKeyURI   = "uri"
	ACLKeyID    = "id"
	ACLKeyEmail = "email"
)

func strConvGrant(gt, gv, p string) (grant, error) {
	var g grant
	g.Permission = p
	switch gt {
	case ACLKeyURI:
		g.Grantee.XMLXSI = GroupType
		g.Grantee.URI = gv
	case ACLKeyID:
		g.Grantee.XMLXSI = UserType
		g.Grantee.ID = gv
	case ACLKeyEmail:
		g.Grantee.XMLXSI = EmailType
		g.Grantee.Email = gv
	default:
		return g, errInvalidArgument
	}

	return g, nil
}

func strConvGrants(ctx context.Context, strs, p string) ([]grant, error) {
	statements := strings.Split(strs, ",")
	grants := make([]grant, 0, len(statements))

	for _, s := range statements {
		vs := strings.Split(s, "=")
		if len(vs) != 2 {
			logger.LogIf(ctx, fmt.Errorf("%v: %v", p, strs))
			return grants, errInvalidArgument
		}
		g, err := strConvGrant(vs[0], vs[1], p)
		if err != nil {
			return grants, err
		}
		grants = append(grants, g)
	}

	return grants, nil

}

func setACLWithHeader(ctx context.Context, aclAPI ObjectAcler, bucket, object, readstr, writestr, readacpstr, writeacpstr, fullstr string) error {
	grantsSum := make([]grant, 0)

	if readstr != "" {
		grants, err := strConvGrants(ctx, readstr, GrantPermRead)
		if err != nil {
			return err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if writestr != "" {
		grants, err := strConvGrants(ctx, writestr, GrantPermWrite)
		if err != nil {
			return err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if readacpstr != "" {
		grants, err := strConvGrants(ctx, readacpstr, GrantPermReadAcp)
		if err != nil {
			return err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if writeacpstr != "" {
		grants, err := strConvGrants(ctx, writeacpstr, GrantPermWriteAcp)
		if err != nil {
			return err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if fullstr != "" {
		grants, err := strConvGrants(ctx, fullstr, GrantPermFullControl)
		if err != nil {
			return err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if err := aclAPI.SetAcl(ctx, bucket, object, grantsSum); err != nil {
		logger.LogIf(ctx, fmt.Errorf("objAPI set fail, err=%v", err))
		return err
	}

	return nil
}
