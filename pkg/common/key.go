package common

import (
	"fmt"
	"strings"

	guuid "github.com/google/uuid"
)

const (
	KEY_DELIMETER = "/"
	LOCKS         = "_locks"
	COMMENTS      = "_comments"
	INTERNAL_DB   = "_"
)

var prefix string

type Key struct {
	Prefix    string
	DBName    string
	TableName string
	// the id represents uuid for the rows and id for the comments and locks
	UUID string
}

func SetPrefix(prf string) {
	prefix = prf
}

func GetPrefix() string {
	return prefix
}

// Parses a key from a given string.
func ParseKey(keyStr string) (*Key, error) {
	keyParts := strings.Split(keyStr, KEY_DELIMETER)
	// We used well defined formatted key, when each part is separated by the KEY_DELIMETER:
	// <ovsdbPrefix><serviceName><dbname><tableName><uuid>
	if len(keyParts) != 5 {
		return nil, fmt.Errorf("wrong formatted key %q", keyStr)
	}
	prf := fmt.Sprintf("%s%s%s", keyParts[0], KEY_DELIMETER, keyParts[1])
	if prf != prefix {
		return nil, fmt.Errorf("wrong key, unmatched prefix %q, %q", prf, prefix)
	}
	retKey := Key{Prefix: prf, DBName: keyParts[2], TableName: keyParts[3], UUID: keyParts[4]}
	if retKey.DBName == "" || retKey.TableName == "" || retKey.UUID == "" {
		return nil, fmt.Errorf("wrong formatted key %q", keyStr)
	}
	return &retKey, nil
}

func (k *Key) String() string {
	if len(k.UUID) == 0 {
		return k.TableKeyString()
	}
	return fmt.Sprintf("%s%s%s%s%s%s%s", k.Prefix, KEY_DELIMETER, k.DBName, KEY_DELIMETER, k.TableName, KEY_DELIMETER, k.UUID)
}

// The helper function, that can be used for logging, when we don't need the prefix.
func (k *Key) ShortString() string {
	return fmt.Sprintf("%s%s%s%s%s", k.DBName, KEY_DELIMETER, k.TableName, KEY_DELIMETER, k.UUID)
}

func (k *Key) TableKeyString() string {
	if len(k.TableName) == 0 {
		return k.DBKeyString()
	}
	return fmt.Sprintf("%s%s%s%s%s", k.Prefix, KEY_DELIMETER, k.DBName, KEY_DELIMETER, k.TableName)
}

func (k *Key) DBKeyString() string {
	return fmt.Sprintf("%s%s%s", k.Prefix, KEY_DELIMETER, k.DBName)
}

func (k *Key) DeploymentKeyString() string {
	return k.Prefix
}

func (k *Key) ToTableKey() Key {
	return Key{Prefix: k.Prefix, DBName: k.DBName, TableName: k.TableName}
}

// Creates a new data key with a generated uuid
func GenerateDataKey(dbName, tableName string) Key {
	return NewDataKey(dbName, tableName, guuid.NewString()) /* generate RFC4122 UUID */
}

// Returns a new Data key. If the given uuid is an empty string, the return key will point to the entire table, and the
// this function call is equals to call `NewTableKey` with the same dbName and tableName parameters.
func NewDataKey(dbName, tableName, uuid string) Key {
	return Key{Prefix: prefix, DBName: dbName, TableName: tableName, UUID: uuid}
}

// Returns a new Comment key. If the given commentID is an empty string, the return key will point to the entire
// comments table, and the this function call is equals to call `NewCommentTableKey`.
func NewCommentKey(commentID string) Key {
	return NewDataKey(INTERNAL_DB, COMMENTS, commentID)
}

// Returns a new Lock key. If the given lockID is an empty string, the return key will point to the entire
// locks table, and the this function call is equals to call `NewLockTableKey`.
func NewLockKey(lockID string) Key {
	return NewDataKey(INTERNAL_DB, LOCKS, lockID)
}

// Helper function, which returns a key to entire table
func NewTableKey(dbName, tableName string) Key {
	return NewDataKey(dbName, tableName, "")
}

// Helper function, which returns a key to the Comments table
func NewCommentTableKey() Key {
	return NewCommentKey("")
}

// Helper function, which returns a key to the Locks table
func NewLockTableKey() Key {
	return NewLockKey("")
}

// Returns a key to entire database of this service
func NewDBPrefixKey(dbName string) Key {
	return Key{Prefix: prefix, DBName: dbName}
}
