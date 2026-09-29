// Package-level error constructors shared by the remoting protocol files.
package remoting

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func decodeErrf(format string, args ...any) *common.Error {
	return common.DecodeError(fmt.Sprintf(format, args...))
}

func encodeErrf(format string, args ...any) *common.Error {
	return common.EncodeError(fmt.Sprintf(format, args...))
}
