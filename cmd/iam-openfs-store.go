package cmd

import (
	"context"
	"sync"
	"os"
	"fmt"
	"io"
	"strconv"
	"path"
	"unicode/utf8"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
)

const (
	OpfsConfigDir = "/var/lib/openfs/cconf/cur"
	OpfsAuthFile = "/auth.conf"
)

type IdUser struct {
	Nt_passwd string    `json:"nt_passwd, omitempty"`
	Shell string        `json:"shell, omitempty"`
	AccessKey string    `json:"name"`
	Pgroup int	    `json:"prim_group"`
	SecretKey string    `json:"passwd"`
	Home string	    `json:"home"`
	Sgroups []int	    `json:"supl_groups, omitempty"`
	Comments string	    `json:"comments"`
	Uid int		    `json:"uid"`
}

type IdGroup struct {
	Gid int `json:"gid"`
	Name string `json:"name"`
}

type IdRecord struct {
	Version int     `json:"version"`
	IdUsers []IdUser   `json:"users, omitempty"`
	IdGroups []IdGroup `json:"groups, omitempty"`
}

type IAMOpfsStore struct {
	// Protect access to storage within the current server.
	sync.RWMutex

	*iamCache

	usersSysType UsersSysType

	objAPI ObjectLayer
}

func newIAMOpfsStore(objAPI ObjectLayer, usersSysType UsersSysType) *IAMOpfsStore {
	return &IAMOpfsStore{
		iamCache:     newIamCache(),
		objAPI:       objAPI,
		usersSysType: usersSysType,
	}
}

func (iamOpfs *IAMOpfsStore) rlock() *iamCache {
	iamOpfs.RLock()
	return iamOpfs.iamCache
}

func (iamOpfs *IAMOpfsStore) runlock() {
	iamOpfs.RUnlock()
}

func (iamOpfs *IAMOpfsStore) lock() *iamCache {
	iamOpfs.Lock()
	return iamOpfs.iamCache
}

func (iamOpfs *IAMOpfsStore) unlock() {
	iamOpfs.Unlock()
}

func (iamOpfs *IAMOpfsStore) getUsersSysType() UsersSysType {
	return iamOpfs.usersSysType
}

func (iamOpfs *IAMOpfsStore) loadIAMConfigBytesWithMetadata(ctx context.Context, objPath string) ([]byte, ObjectInfo, error) {
	data, meta, err := readConfigWithMetadata(ctx, nil, objPath)
	if err != nil {
		return nil, meta, err
	}

	if !utf8.Valid(data) && GlobalKMS != nil {
		data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err != nil {
			return nil, meta, err
		}
	}
	return data, meta, nil
}

func (iamOpfs *IAMOpfsStore) migrateBackendFormat(ctx context.Context) error {
	iamOpfs.Lock()
	defer iamOpfs.Unlock()
	return nil
}

func (iamOpfs *IAMOpfsStore) saveIAMConfig(ctx context.Context, item interface{}, objPath string, opts ...options) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err != nil {
			return err
		}
	}
	return saveConfig(ctx, iamOpfs.objAPI, objPath, data)
}

func (iamOpfs *IAMOpfsStore) loadIAMConfig(ctx context.Context, item interface{}, objPath string) error {
	data, _, err := iamOpfs.loadIAMConfigBytesWithMetadata(ctx, objPath)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(data, item)
}

func loadOPFSConfigBytes(ctx context.Context, objPath string) ([]byte, error) {
	r, err := os.Open(objPath)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("open opfs config path(%v) failed", objPath))
		return nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("read opfs config path(%v) failed", objPath))
		return nil, err
	}
	return data, nil
}

func loadOPFSConfig(ctx context.Context, objPath string) (*IdRecord, error) {
	data, err := loadOPFSConfigBytes(ctx, objPath)
	if err != nil {
		return nil, err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	var ids IdRecord
	json.Unmarshal(data, &ids)

	return &ids, nil
}

func createUserIdentityfromOPFS(uid IdUser) (*auth.Credentials, error) {
	cred, err := auth.CreateCredentials(uid.AccessKey, uid.SecretKey)
	if err != nil {
		return nil, err
	}
	claims := make(map[string]interface{})
	claims["userid"] = uid.Uid
	claims["groupid"] = uid.Pgroup
	cred.Claims = claims
	return &cred, nil
}

func createGroupInfoOPFS(gid IdGroup, ids *IdRecord) *GroupInfo {
	var members []string

	for _, uid := range ids.IdUsers {
		if uid.Pgroup == gid.Gid {
			members = append(members, uid.AccessKey)
		} else {
			for sid := range uid.Sgroups {
				if sid == gid.Gid {
					members = append(members, uid.AccessKey)
				}
			}
		}
	}
	g := newGroupInfo(members)
	return &g
}

func (iamOpfs *IAMOpfsStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteConfig(ctx, iamOpfs.objAPI, path)
}

func (iamOpfs *IAMOpfsStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	data, objInfo, err := iamOpfs.loadIAMConfigBytesWithMetadata(ctx, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	var p PolicyDoc
	err = p.parseJSON(data)
	if err != nil {
		return err
	}

	if p.Version == 0 {
		// This means that policy was in the old version (without any
		// timestamp info). We fetch the mod time of the file and save
		// that as create and update date.
		p.CreateDate = objInfo.ModTime
		p.UpdateDate = objInfo.ModTime
	}

	m[policy] = p
	return nil
}

func (iamOpfs *IAMOpfsStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	for item := range listIAMConfigItems(ctx, iamOpfs.objAPI, iamConfigPoliciesPrefix) {
		if item.Err != nil {
			return item.Err
		}

		policyName := path.Dir(item.Item)
		if err := iamOpfs.loadPolicyDoc(ctx, policyName, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (iamOpfs *IAMOpfsStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
	if userType == regUser {
		configPath := OpfsConfigDir + OpfsAuthFile
		ids, err := loadOPFSConfig(ctx, configPath)
		if err != nil {
			return err
		}
		found := false
		for _, uid := range ids.IdUsers {
			if uid.AccessKey == user {
				found = true
				cred, err := createUserIdentityfromOPFS(uid)
				if err != nil {
					return err
				}
				m[user] = *cred
				break
			}
		}
		if !found {
			return errNoSuchUser
		}
		return nil
	}
	var u UserIdentity
	err := iamOpfs.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamOpfs.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
		iamOpfs.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
		return nil
	}

	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}

	m[user] = u.Credentials
	return nil
}

func (iamOpfs *IAMOpfsStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var configPath string
	var basePrefix string
	switch userType {
	case svcUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		configPath = OpfsConfigDir + OpfsAuthFile
	}

	if userType == regUser {
		ids, err := loadOPFSConfig(ctx, configPath)
		if err != nil {
			return err
		}

		for _, uid := range ids.IdUsers {
			cred, err := createUserIdentityfromOPFS(uid)
			if err != nil {
				return err
			}
			m[cred.AccessKey] = *cred
		}
		return nil
	}

	for item := range listIAMConfigItems(ctx, iamOpfs.objAPI, basePrefix) {
		if item.Err != nil {
			return item.Err
		}

		userName := path.Dir(item.Item)
		if err := iamOpfs.loadUser(ctx, userName, userType, m); err != nil && err != errNoSuchUser {
			return err
		}
	}

	return nil
}

func (iamOpfs *IAMOpfsStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	return nil
}

func (iamOpfs *IAMOpfsStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	configPath := OpfsConfigDir + OpfsAuthFile
	ids, err := loadOPFSConfig(ctx, configPath)
	if err != nil {
		return err
	}
	for _, gid := range ids.IdGroups {
		ginfo := createGroupInfoOPFS(gid, ids)
		m[strconv.FormatInt(int64(gid.Gid), 10)] = *ginfo
	}
	return nil
}

func (iamOpfs *IAMOpfsStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool,
					      m map[string]MappedPolicy,) error {
	var p MappedPolicy
	err := iamOpfs.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}

func (iamOpfs *IAMOpfsStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	var basePath string
	if isGroup {
		basePath = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case svcUser:
			basePath = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePath = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePath = iamConfigPolicyDBUsersPrefix
		}
	}
	for item := range listIAMConfigItems(ctx, iamOpfs.objAPI, basePath) {
		if item.Err != nil {
			return item.Err
		}

		policyFile := item.Item
		userOrGroupName := strings.TrimSuffix(policyFile, ".json")
		if err := iamOpfs.loadMappedPolicy(ctx, userOrGroupName, userType, isGroup, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (iamOpfs *IAMOpfsStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
	return iamOpfs.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}

func (iamOpfs *IAMOpfsStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return iamOpfs.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}

func (iamOpfs *IAMOpfsStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	if userType == regUser {
		return NotImplemented{}
	}

	return iamOpfs.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}

func (iamOpfs *IAMOpfsStore) saveGroupInfo(ctx context.Context, name string, gi GroupInfo) error {
	return NotImplemented{}
}

func (iamOpfs *IAMOpfsStore) deletePolicyDoc(ctx context.Context, name string) error {
	err := iamOpfs.deleteIAMConfig(ctx, getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOpfs *IAMOpfsStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := iamOpfs.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOpfs *IAMOpfsStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	if userType == regUser {
		return NotImplemented{}
	}
	err := iamOpfs.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (iamOpfs *IAMOpfsStore) deleteGroupInfo(ctx context.Context, name string) error {
	return NotImplemented{}
}

func (iamOpfs *IAMOpfsStore) loadAllFromOPFS(ctx context.Context, cache *iamCache) error {
	// Loads things in the same order as `LoadIAMCache()`
	//1. load policies docs
	//2. set default canned policies
	//3. load users
	if err := iamOpfs.loadUsers(ctx, regUser, cache.iamUsersMap); err != nil {
		return err
	}
	//4. load groups
	if err := iamOpfs.loadGroups(ctx, cache.iamGroupsMap); err != nil {
		return err
	}
	//5. load polices maped to users
	//6. load polices mapped to groups
	//7. load service accounts
	if err := iamOpfs.loadUsers(ctx, svcUser, cache.iamUsersMap); err != nil {
		return err
	}
	//8. load sts user
	if err := iamOpfs.loadUsers(ctx, stsUser, cache.iamUsersMap); err != nil {
		return err
	}
	//9. load sts policy mapping
	if err := iamOpfs.loadMappedPolicies(ctx, stsUser, false, cache.iamUserPolicyMap); err != nil {
		return err
	}
	//10. build user and group memberships
	cache.buildUserGroupMemberships()
	return nil
}
