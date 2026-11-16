// Package domain defines how protocol state is encoded as metric values.
package domain

import raft "github.com/nickemma/meridian/internal/raft/domain"

// RoleCode encodes a Raft role for the meridian_raft_role gauge:
// 0=Follower 1=Candidate 2=Leader. Unknown roles report -1.
func RoleCode(role raft.Role) float64 {
	switch role {
	case raft.Follower:
		return 0
	case raft.Candidate:
		return 1
	case raft.Leader:
		return 2
	default:
		return -1
	}
}
