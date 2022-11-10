package cmd

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	bucketpolicy "github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/bucket/policy/condition"
	iampolicy "github.com/minio/pkg/iam/policy"
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

func cannedStrTogrants(cannedACL string, isObject bool) ([]grant, error) {
	grants := make([]grant, 0, 3)
	if cannedACL == ACLCannedAwsExecRead ||
		cannedACL == ACLCannedBucketOwnerRead ||
		cannedACL == ACLCannedBucketOwnerFullControl ||
		cannedACL == ACLCannedLogDeliveryWrite {
		return grants, NotImplemented{}
	}
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
		if !isObject {
			grants = append(grants, geveryWrite)
		}
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
	return grants, nil
}

func setACLWithCanned(ctx context.Context, aclAPI ObjectAcler, bucket, object, cannedACL string) error {
	grants, err := cannedStrTogrants(cannedACL, object != "")
	if err != nil {
		return err
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

func strTogrants(ctx context.Context, isObject bool, readstr, writestr, readacpstr, writeacpstr, fullstr string) ([]grant, error) {
	grantsSum := make([]grant, 0)

	if readstr != "" {
		grants, err := strConvGrants(ctx, readstr, GrantPermRead)
		if err != nil {
			return grantsSum, err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if writestr != "" && isObject {
		return grantsSum, errInvalidArgument
	}

	if writestr != "" {
		grants, err := strConvGrants(ctx, writestr, GrantPermWrite)
		if err != nil {
			return grantsSum, err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if readacpstr != "" {
		grants, err := strConvGrants(ctx, readacpstr, GrantPermReadAcp)
		if err != nil {
			return grantsSum, err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if writeacpstr != "" {
		grants, err := strConvGrants(ctx, writeacpstr, GrantPermWriteAcp)
		if err != nil {
			return grantsSum, err
		}
		grantsSum = append(grantsSum, grants...)
	}

	if fullstr != "" {
		grants, err := strConvGrants(ctx, fullstr, GrantPermFullControl)
		if err != nil {
			return grantsSum, err
		}
		grantsSum = append(grantsSum, grants...)
	}

	return grantsSum, nil
}

func setACLWithHeader(ctx context.Context, aclAPI ObjectAcler, bucket, object, readstr, writestr, readacpstr, writeacpstr, fullstr string) error {
	grantsSum, err := strTogrants(ctx, object != "", readstr, writestr, readacpstr, writeacpstr, fullstr)
	if err != nil {
		return err
	}
	if err := aclAPI.SetAcl(ctx, bucket, object, grantsSum); err != nil {
		logger.LogIf(ctx, fmt.Errorf("objAPI set fail, err=%v", err))
		return err
	}

	return nil
}

func getACLFromRequest(ctx context.Context, r *http.Request, isObject bool) ([]grant, error) {
	acl := make([]grant, 0)
	if haveAclHeader(r) {
		grants, err := strTogrants(ctx, isObject, r.Header.Get(xhttp.AmzGrantRead),
			r.Header.Get(xhttp.AmzGrantWrite),
			r.Header.Get(xhttp.AmzGrantReadAcp),
			r.Header.Get(xhttp.AmzGrantWriteAcp),
			r.Header.Get(xhttp.AmzGrantFullControl))
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("read %v, write %v, read-acp %v, write-acp %v, full %v",
				r.Header.Get(xhttp.AmzGrantRead),
				r.Header.Get(xhttp.AmzGrantWrite),
				r.Header.Get(xhttp.AmzGrantReadAcp),
				r.Header.Get(xhttp.AmzGrantWriteAcp),
				r.Header.Get(xhttp.AmzGrantFullControl)))
			return grants, err
		}
		if len(acl) != 0 {
			logger.LogIf(ctx, fmt.Errorf("acl %v", acl))
			return grants, err
		}
		acl = grants
	}

	if aclHeader := r.Header.Get(xhttp.AmzACL); aclHeader != "" {
		grants, err := cannedStrTogrants(aclHeader, isObject)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("canned acl %v", aclHeader))
			return grants, err
		}
		if len(acl) != 0 {
			logger.LogIf(ctx, fmt.Errorf("acl %v", acl))
			return grants, err
		}
		acl = grants
	}

	return acl, nil
}
func getDefaultAcl() []grant {
	return []grant{
		grant{
			Grantee: grantee{
				XMLXSI: OwnerType,
			},
			Permission: GrantPermFullControl,
		},
	}
}

func checkObjectAclGrant(grants []grant) error {
	for _, g := range grants {
		if g.Permission == GrantPermWrite {
			return errInvalidArgument
		}
	}

	return nil
}

func actionToAcl(action iampolicy.Action) string {
	if action == iampolicy.GetObjectAction ||
		action == iampolicy.ListBucketAction ||
		action == iampolicy.ListBucketMultipartUploadsAction {
		return GrantPermRead
	} else if action == iampolicy.PutObjectAction {
		return GrantPermWrite
	} else if action == iampolicy.GetObjectAclAction ||
		action == iampolicy.GetBucketAclAction {
		return GrantPermReadAcp
	} else if action == iampolicy.PutObjectAclAction ||
		action == iampolicy.PutBucketAclAction {
		return GrantPermWriteAcp
	}
	return ""
}

func aclToAction(acl string) []iampolicy.Action {
	//full control is max, and len is 6
	actions := make([]iampolicy.Action, 0, 6)
	switch acl {
	case GrantPermRead:
		actions = append(actions, iampolicy.GetObjectAction,
			iampolicy.ListBucketAction, iampolicy.ListBucketMultipartUploadsAction)
	case GrantPermWrite:
		actions = append(actions, iampolicy.PutObjectAction)
	case GrantPermReadAcp:
		actions = append(actions, iampolicy.GetBucketAclAction)
	case GrantPermWriteAcp:
		actions = append(actions, iampolicy.PutBucketAclAction)
	case GrantPermFullControl:
		actions = append(actions, iampolicy.GetObjectAction,
			iampolicy.ListBucketAction, iampolicy.ListBucketMultipartUploadsAction,
			iampolicy.PutObjectAction, iampolicy.GetBucketAclAction, iampolicy.PutBucketAclAction)
	}
	return actions
}
func grantsToPolicy(grants []grant, cred auth.Credentials, bucket string) iampolicy.Policy {
	policy := iampolicy.Policy{}
	rset := iampolicy.NewResourceSet(iampolicy.NewResource(bucket, "*"))
	cond := condition.NewFunctions()
	uid, _ := cred.Claims[UserID].(int)
	cid, _ := globalIAMSys.GetCanionialIdByUid(uid)
	uname := cred.AccessKey
	var statements []iampolicy.Statement
	for _, g := range grants {
		switch g.Grantee.Type {
		case UserType:
			if cid == g.Grantee.ID {
				statements = append(statements, iampolicy.NewStatement("",
					bucketpolicy.Allow,
					iampolicy.NewActionSet(aclToAction(g.Permission)...),
					rset,
					cond))
			}
		case GroupType:
			if isAllUser(g.Grantee.URI) {
				statements = append(statements, iampolicy.NewStatement("",
					bucketpolicy.Allow,
					iampolicy.NewActionSet(aclToAction(g.Permission)...),
					rset,
					cond))
			}
			gnames := globalIAMSys.GetUserGroupMembership(uname)
			pg, _ := parseURIGroup(g.Grantee.URI)
			if gnames.Contains(pg) {
				statements = append(statements, iampolicy.NewStatement("",
					bucketpolicy.Allow,
					iampolicy.NewActionSet(aclToAction(g.Permission)...),
					rset,
					cond))
			}
		}
	}
	policy.Version = iampolicy.DefaultVersion
	policy.Statements = statements

	return policy
}

func aclToPolicy(ctx context.Context, cred auth.Credentials, objAPI ObjectLayer) iampolicy.Policy {
	policy := iampolicy.Policy{}
	aclAPI, aclSupport := objAPI.(*GatewayLocker).ObjectLayer.(ObjectAcler)
	if !aclSupport {
		logger.LogIf(ctx, fmt.Errorf("not support acl"))
		return policy
	}
	listBuckets := objAPI.ListBuckets
	bucketsInfo, err := listBuckets(ctx)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("fail to get buckets"))
		return policy
	}
	for _, bucketInfo := range bucketsInfo {
		grants, err := aclAPI.GetAcl(ctx, bucketInfo.Name, "")
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("get acl %v failed %v", bucketInfo.Name, err))
			return policy
		}
		policy = policy.Merge(grantsToPolicy(grants, cred, bucketInfo.Name))
	}

	return policy
}
