package common

import "testing"

func TestResetTransactionValue(t *testing.T) {
	if got := ResetTransactionValue(0xFF, 0); got != 0xF3 {
		t.Fatalf("got 0x%X", got)
	}
	if got := ResetTransactionValue(0, MessageSysFlagTransactionCommit); got != MessageSysFlagTransactionCommit {
		t.Fatalf("got 0x%X", got)
	}
	if GetTransactionValue(MessageSysFlagTransactionRollback|0x11) != MessageSysFlagTransactionRollback {
		t.Fatal("GetTransactionValue mask mismatch")
	}
}

func TestBuildSysFlagVectors(t *testing.T) {
	if got := BuildSysFlagBasic(true, false, false, false); got != 0x1 {
		t.Fatalf("basic = 0x%X", got)
	}
	if got := BuildSysFlag(true, true, true, true, true); got != 0x1F {
		t.Fatalf("all = 0x%X", got)
	}
	if got := BuildSysFlag(false, false, false, false, false); got != 0 {
		t.Fatalf("none = 0x%X", got)
	}
	if got := BuildSysFlagWithHashing(0, true); got != 0x1<<10 {
		t.Fatalf("hashing = 0x%X", got)
	}
	if got := BuildSysFlagWithHashing(0x1, false); got != 0x1 {
		t.Fatalf("hashing off = 0x%X", got)
	}
	if !HasSupportFilterAndComputeHashingFlag(0x1 << 10) {
		t.Fatal("hashing flag must be set")
	}
	if !HasLitePullFlag(BuildSysFlag(false, false, false, false, true)) {
		t.Fatal("lite pull flag must be set")
	}
	if HasCommitOffsetFlag(0) {
		t.Fatal("no commit offset expected")
	}
	if got := ClearCommitOffsetFlag(0x3); got != 0x2 {
		t.Fatalf("clear commit = 0x%X", got)
	}
	if !HasSuspendFlag(0x2) || HasSubscriptionFlag(0x2) || HasClassFilterFlag(0x2) {
		t.Fatal("bit probing mismatch")
	}
	if got := ClearSuspendFlag(0x2); got != 0 {
		t.Fatalf("clear suspend = 0x%X", got)
	}
	if got := BuildSysFlagWithSubscription(0); got != 0x4 {
		t.Fatalf("with subscription = 0x%X", got)
	}
}

func TestCompressionTypeBits(t *testing.T) {
	if got := GetCompressionType(MessageSysFlagCompressionZlib); got != ZlibType {
		t.Fatalf("zlib type = %d", got)
	}
	if got := GetCompressionType(MessageSysFlagCompressionLz4); got != Lz4Type {
		t.Fatalf("lz4 type = %d", got)
	}
	// SetCompressionType 只动类型位，其余位（含 COMPRESSED）保持
	set := SetCompressionType(MessageSysFlagCompressed|0xFF&^MessageSysFlagCompressionTypeMask, ZstdType)
	if GetCompressionType(set) != ZstdType {
		t.Fatalf("set type = %d", GetCompressionType(set))
	}
	if !IsCompressed(set) {
		t.Fatal("compressed bit must survive SetCompressionType")
	}
	// 解压后只清 COMPRESSED，类型位保留（Java 同语义）
	cleared := ClearCompressedFlag(MessageSysFlagCompressed | MessageSysFlagCompressionZlib)
	if IsCompressed(cleared) {
		t.Fatal("compressed bit must be cleared")
	}
	if GetCompressionType(cleared) != ZlibType {
		t.Fatal("type bits must be kept after decompression")
	}
	if NormalizeCompressionType(0) != ZlibType {
		t.Fatal("type 0 (pre-type-bit clients) must read as ZLIB")
	}
	if NormalizeCompressionType(2) != ZstdType {
		t.Fatal("type 2 must pass through")
	}
	if name, ok := CompressionTypeName(SnappyType); ok || name != "" {
		t.Fatal("snappy has no name here")
	}
	if name, _ := CompressionTypeName(MessageSysFlagCompressionZlib >> CompressionTypeShift); name != "ZLIB" {
		t.Fatalf("zlib name %q", name)
	}
}

func TestPermName(t *testing.T) {
	cases := []struct {
		perm int32
		want string
	}{
		{7, "RWX"},
		{6, "RW-"},
		{4, "R--"},
		{0, "---"},
	}
	for _, c := range cases {
		if got := Perm2String(c.perm); got != c.want {
			t.Fatalf("Perm2String(%d) = %q, want %q", c.perm, got, c.want)
		}
	}
	if !PermIsValid(0) || !PermIsValid(7) {
		t.Fatal("0..7 are valid")
	}
	if PermIsValid(8) || PermIsValid(-1) {
		t.Fatal("8 and negative are invalid")
	}
	if !PermIsValidStr("6") || PermIsValidStr("RW") || PermIsValidStr("") {
		t.Fatal("PermIsValidStr must only accept integers")
	}
	if !CheckPerm(6, PermRead|PermWrite) {
		t.Fatal("6 has R|W")
	}
	if CheckPerm(4, PermWrite) {
		t.Fatal("4 lacks W")
	}
}
