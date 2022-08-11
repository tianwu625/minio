// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"unsafe"

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
)

//grantee xmlxsi type
const (
	//grantee type for user
	UserType = "CanonicalUser"
	//grantee type for group
	GroupType = "Group"
	//grantee type for email
	EmailType = "AmazonCustomerByEmail"
	//grantee type for owner canned acl
	OwnerType = "Owner"
	//grantee type for every one canned acl
	EveryType = "EveryOne"
	//grantee type for auth user canned acl
	AuthType = "AuthuserType"
)

//grant permission type
const (
	//grant permission read type
	GrantPermRead = "READ"
	//grant permission write type
	GrantPermWrite = "WRITE"
	//grant permission read-acp type
	GrantPermReadAcp = "READ_ACP"
	//grant permission write-acp type
	GrantPermWriteAcp = "WRITE_ACP"
	//grant permission full-control type
	GrantPermFullControl = "FULL_CONTROL"
)

//acl head for canned acl type
const (
	// canned acl for private
	ACLCannedPrivate = "private"
	// canned acl for public-read
	ACLCannedPublicRead = "public-read"
	// canned acl for public-read-write
	ACLCannedPublicReadWrite = "public-read-write"
	// canned acl for authenticated-read
	ACLCannedAuthRead = "authenticated-read"
	// canned acl for aws-exec-read
	ACLCannedAwsExecRead = "aws-exec-read"
	// canned acl for bucket-owner-read
	ACLCannedBucketOwnerRead = "bucket-owner-read"
	// canned acl for bucket-owner-full-control
	ACLCannedBucketOwnerFullControl = "bucket-owner-full-control"
	// canned acl for log-delivery-write
	ACLCannedLogDeliveryWrite = "log-delivery-write"
)

// Data types used for returning dummy access control
// policy XML, these variables shouldn't be used elsewhere
// they are only defined to be used in this file alone.
type grantee struct {
	XMLNS       string
	XMLXSI      string
	Type        string
	ID          string
	DisplayName string
	URI         string
	Email       string
}

type grant struct {
	Grantee    grantee
	Permission string
}

type accessControlPolicy struct {
	XMLName           xml.Name
	Owner             Owner
	AccessControlList struct {
		Grants []grant
	}
}

//FIXME xmldecode fail for xmlxsi and xmlns
//So put encode and decode data struct seperatedly
//decode Struct
type granteeDecode struct {
	XMLNS       string `xml:"xsi,attr"`
	XMLXSI      string `xml:"type,attr"`
	Type        string `xml:"Type"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
	Email       string `xlm:"EmailAddress,omitempty"`
}

type grantDecode struct {
	Grantee    granteeDecode `xml:"Grantee"`
	Permission string        `xml:"Permission"`
}

type accessControlPolicyDecode struct {
	XMLName           xml.Name `xml:"AccessControlPolicy"`
	Owner             Owner    `xml:"Owner"`
	AccessControlList struct {
		Grants []grantDecode `xml:"Grant"`
	} `xml:"AccessControlList"`
}

type granteeEncode struct {
	XMLNS       string `xml:"xmlns:xsi,attr"`
	XMLXSI      string `xml:"xsi:type,attr"`
	Type        string `xml:"Type"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
	Email       string `xlm:"EmailAddress,omitempty"`
}

type grantEncode struct {
	Grantee    granteeEncode `xml:"Grantee"`
	Permission string        `xml:"Permission"`
}

type accessControlPolicyEncode struct {
	XMLName           xml.Name `xml:"AccessControlPolicy"`
	Owner             Owner    `xml:"Owner"`
	AccessControlList struct {
		Grants []grantEncode `xml:"Grant"`
	} `xml:"AccessControlList"`
}

// PutBucketACLHandler - PUT Bucket ACL
// -----------------
// This operation uses the ACL subresource
// to set ACL for a bucket, this is a dummy call
// only responds success if the ACL is private.
func (api objectAPIHandlers) PutBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketACL")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Allow putBucketACL if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	ctx, s3Error := newOpfsContext(ctx, r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	aclAPI, aclSupport := objAPI.(*GatewayLocker).ObjectLayer.(ObjectAcler)

	if haveAclHeader(r) {
		if aclSupport {
			if err := setACLWithHeader(ctx, aclAPI, bucket, "", r.Header.Get(xhttp.AmzGrantRead),
				r.Header.Get(xhttp.AmzGrantWrite),
				r.Header.Get(xhttp.AmzGrantReadAcp), r.Header.Get(xhttp.AmzGrantWriteAcp),
				r.Header.Get(xhttp.AmzGrantFullControl)); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			return
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
		return
	}

	aclHeader := r.Header.Get(xhttp.AmzACL)

	if aclHeader == "" {
		acld := &accessControlPolicyDecode{}
		if err = xmlDecoder(r.Body, acld, r.ContentLength); err != nil {
			if err == io.EOF {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingSecurityHeader),
					r.URL)
				return
			}
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		acl := (*accessControlPolicy)(unsafe.Pointer(acld))

		if len(acl.AccessControlList.Grants) == 0 {
			writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
			return
		}

		if acl.AccessControlList.Grants[0].Permission != GrantPermFullControl && !aclSupport {
			writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
			return
		}

		if acl.AccessControlList.Grants[0].Permission == GrantPermFullControl && !aclSupport {
			return
		}

		if err := aclAPI.SetAcl(ctx, bucket, "", acl.AccessControlList.Grants); err != nil {
			logger.LogIf(ctx, fmt.Errorf("objAPI set fail, err=%v", err))
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}

	if aclHeader != "" && aclHeader != ACLCannedPrivate && !aclSupport {
		writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
		return
	}

	if aclHeader != "" && aclHeader == ACLCannedPrivate && !aclSupport {
		return
	}

	if aclSupport && aclHeader != "" {
		if err := setACLWithCanned(ctx, aclAPI, bucket, "", aclHeader); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}
}

// GetBucketACLHandler - GET Bucket ACL
// -----------------
// This operation uses the ACL
// subresource to return the ACL of a specified bucket.
func (api objectAPIHandlers) GetBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketACL")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Allow getBucketACL if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	ctx, s3Error := newOpfsContext(ctx, r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	aclAPI, aclSupport := objAPI.(*GatewayLocker).ObjectLayer.(ObjectAcler)

	acle := &accessControlPolicyEncode{}
	if !aclSupport {
		acle.AccessControlList.Grants = append(acle.AccessControlList.Grants, grantEncode{
			Grantee: granteeEncode{
				XMLNS:  AWSNS,
				XMLXSI: UserType,
				Type:   UserType,
			},
			Permission: GrantPermFullControl,
		})
	} else {
		var grants []grant
		grants, err := aclAPI.GetAcl(ctx, bucket, "")
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		acle.AccessControlList.Grants = grantsToEncode(grants)
	}

	if err := xml.NewEncoder(w).Encode(acle); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
}

// PutObjectACLHandler - PUT Object ACL
// -----------------
// This operation uses the ACL subresource
// to set ACL for a bucket, this is a dummy call
// only responds success if the ACL is private.
func (api objectAPIHandlers) PutObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectACL")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Allow putObjectACL if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	ctx, s3Error := newOpfsContext(ctx, r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Before proceeding validate if object exists.
	_, err = objAPI.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	aclAPI, aclSupport := objAPI.(*GatewayLocker).ObjectLayer.(ObjectAcler)

	if haveAclHeader(r) {
		if aclSupport {
			if err := setACLWithHeader(ctx, aclAPI, bucket, object, r.Header.Get(xhttp.AmzGrantRead),
				r.Header.Get(xhttp.AmzGrantWrite),
				r.Header.Get(xhttp.AmzGrantReadAcp), r.Header.Get(xhttp.AmzGrantWriteAcp),
				r.Header.Get(xhttp.AmzGrantFullControl)); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			return
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
		return
	}

	aclHeader := r.Header.Get(xhttp.AmzACL)

	if aclHeader == "" {
		acld := &accessControlPolicyDecode{}
		if err = xmlDecoder(r.Body, acld, r.ContentLength); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		acl := (*accessControlPolicy)(unsafe.Pointer(acld))

		if len(acl.AccessControlList.Grants) == 0 {
			writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
			return
		}

		if acl.AccessControlList.Grants[0].Permission != GrantPermFullControl && !aclSupport {
			writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
			return
		}

		if acl.AccessControlList.Grants[0].Permission == GrantPermFullControl && !aclSupport {
			return
		}

		if err := aclAPI.SetAcl(ctx, bucket, object, acl.AccessControlList.Grants); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
	}

	if aclHeader != "" && aclHeader != ACLCannedPrivate && !aclSupport {
		writeErrorResponse(ctx, w, toAPIError(ctx, NotImplemented{}), r.URL)
		return
	}

	if aclHeader != "" && aclHeader != ACLCannedPrivate && !aclSupport {
		return
	}

	if aclSupport && aclHeader != "" {
		if err := setACLWithCanned(ctx, aclAPI, bucket, object, aclHeader); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}
}

// GetObjectACLHandler - GET Object ACL
// -----------------
// This operation uses the ACL
// subresource to return the ACL of a specified object.
func (api objectAPIHandlers) GetObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObjectACL")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Allow getObjectACL if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	ctx, s3Error := newOpfsContext(ctx, r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Before proceeding validate if object exists.
	_, err = objAPI.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	aclAPI, aclSupport := objAPI.(*GatewayLocker).ObjectLayer.(ObjectAcler)

	acle := &accessControlPolicyEncode{}
	if !aclSupport {
		acle.AccessControlList.Grants = append(acle.AccessControlList.Grants, grantEncode{
			Grantee: granteeEncode{
				XMLNS:  AWSNS,
				XMLXSI: UserType,
				Type:   UserType,
			},
			Permission: GrantPermFullControl,
		})
	} else {
		var grants []grant
		grants, err := aclAPI.GetAcl(ctx, bucket, object)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		acle.AccessControlList.Grants = grantsToEncode(grants)
	}

	if err := xml.NewEncoder(w).Encode(acle); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
}

func grantsToEncode(grants []grant) []grantEncode {
	var ge []grantEncode

	ehdr := (*reflect.SliceHeader)(unsafe.Pointer(&ge))
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&grants))
	ehdr.Data = hdr.Data
	ehdr.Len = hdr.Len
	ehdr.Cap = hdr.Cap

	return ge
}
