package client_test

import (
	"fmt"

	ksql "github.com/vancelongwill/ksql/client"
)

func ExampleExecResult_As() {
	result := ksql.ExecResult{
		CommandResult: &ksql.CommandResult{
			CommandID: "69141AFF-1C6B-43F5-8905-7D6923588875",
			CommandStatus: ksql.CommandStatus{
				Status: "QUEUED",
			},
		},
	}
	var cmd ksql.CommandResult
	b := result.As(&cmd)
	fmt.Println(b)
	var describe ksql.DescribeResult
	b = result.As(&describe)
	fmt.Println(b)
	// output:
	// true
	// false
}
