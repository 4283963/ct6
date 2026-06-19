package lock

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewToken 生成锁持有者标识，用于安全释放（CAS 校验）。
// 结合实例 ID 与随机数，保证跨实例唯一且可追溯。
func NewToken(instanceID string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand read: %w", err)
	}
	return fmt.Sprintf("%s:%s", instanceID, hex.EncodeToString(b)), nil
}
